package main

import (
	"bufio"
	"context"
	"github.com/go-redis/redis/v8"
	"go.guoyk.net/redmemd/memwire"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	optPort        = strings.TrimSpace(os.Getenv("PORT"))
	optRedisURL    = strings.TrimSpace(os.Getenv("REDIS_URL"))
	optRedisPrefix = strings.TrimSpace(os.Getenv("REDIS_PREFIX"))
)

func calculateRedisKey(key string) string {
	return optRedisPrefix + key
}

func calculateRedisFlagsKey(key string) string {
	return optRedisPrefix + "__FLAGS." + key
}

func main() {
	var err error
	defer func(err *error) {
		if *err != nil {
			log.Println("exited with error:", (*err).Error())
			os.Exit(1)
		} else {
			log.Println("exited")
		}
	}(&err)

	if optPort == "" {
		optPort = "11211"
	}

	if optRedisURL == "" {
		optRedisURL = "redis://127.0.0.1:6379/0"
	}

	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", "0.0.0.0:"+optPort); err != nil {
		return
	}

	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		return
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	chErr := make(chan error, 1)
	chSig := make(chan os.Signal, 1)

	signal.Notify(chSig, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		for {
			if conn, err1 := listener.AcceptTCP(); err1 != nil {
				chErr <- err1
				return
			} else {
				wg.Add(1)
				go handleConn(ctx, wg, conn)
			}
		}
	}()

	select {
	case err = <-chErr:
	case sig := <-chSig:
		log.Println("signal caught:", sig.String())
	}

	_ = listener.Close()

	ctxCancel()

	log.Println("waiting for existed connections")
	wg.Wait()
}

func handleConn(ctx context.Context, wg *sync.WaitGroup, conn *net.TCPConn) {
	defer wg.Done()
	defer conn.Close()

	log.Println("connected:", conn.RemoteAddr().String())
	defer log.Println("disconnected:", conn.RemoteAddr().String())

	var err error
	defer func(err *error) {
		if *err != nil {
			log.Println("failed:", conn.RemoteAddr().String(), (*err).Error())
		}
	}(&err)

	var opts *redis.Options

	if opts, err = redis.ParseURL(optRedisURL); err != nil {
		return
	}

	client := redis.NewClient(opts)
	defer client.Close()

	if err = client.Ping(ctx).Err(); err != nil {
		return
	}

	r := bufio.NewReaderSize(conn, 4096)
	w := bufio.NewWriterSize(conn, 4096)

	go func() {
		<-ctx.Done()
		time.Sleep(time.Second)
		_ = conn.Close()
	}()

	sendResp := func(res *memwire.Response) (err error) {
		if _, err = w.WriteString(res.String()); err != nil {
			return
		}
		if err = w.Flush(); err != nil {
			return
		}
		return
	}

	sendError := func(msg string) error {
		return sendResp(&memwire.Response{
			Response: msg,
		})
	}

rxLoop:
	for {
		var req *memwire.Request
		if req, err = memwire.ReadRequest(r); err != nil {
			if perr, ok := err.(memwire.Error); ok {
				if err = sendError(memwire.CodeClientErr + perr.Description); err != nil {
					return
				}
				continue
			} else {
				return
			}
		}
		if ctx.Err() != nil {
			_ = sendError(memwire.CodeServerErr + "shutting down")
			return
		}
		switch req.Command {
		case "get":
			res := &memwire.Response{}
			for _, key := range req.Keys {
				val, err1 := client.Get(ctx, calculateRedisKey(key)).Result()
				if err1 != nil {
					if err1 == redis.Nil {
						continue
					} else {
						if err = sendError(memwire.CodeServerErr + err1.Error()); err != nil {
							return
						}
						continue rxLoop
					}
				}
				flags, err2 := client.Get(ctx, calculateRedisFlagsKey(key)).Result()
				if err2 != nil {
					if err2 == redis.Nil {
						flags = "0"
					} else {
						if err = sendError(memwire.CodeServerErr + err1.Error()); err != nil {
							return
						}
						continue rxLoop
					}
				}
				res.Values = append(res.Values, memwire.Value{
					Key:   key,
					Flags: flags,
					Data:  []byte(val),
				})
			}
			res.Response = memwire.CodeEnd
			if err = sendResp(res); err != nil {
				return
			}
		default:
			if err = sendError(memwire.CodeClientErr + req.Command + " not implemented"); err != nil {
				return
			}
			continue
		}
	}

}
