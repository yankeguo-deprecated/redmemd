package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	optPort        = os.Getenv("PORT")
	optRedisURL    = os.Getenv("REDIS_URL")
	optRedisPrefix = os.Getenv("REDIS_PREFIX")
)

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

	go io.Copy(io.Discard, conn)

	tk := time.Tick(time.Second)

forLoop:
	for {
		select {
		case t := <-tk:
			conn.Write([]byte(t.String() + "\n"))
		case <-ctx.Done():
			conn.Write([]byte("QUIT\n"))
			break forLoop
		}
	}

}
