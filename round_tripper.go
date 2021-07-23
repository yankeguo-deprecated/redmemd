package main

import (
	"bufio"
	"context"
	"errors"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"go.guoyk.net/redmemd/memwire"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotStored       = errors.New("not stored")
	ErrExists          = errors.New("exists")
	ErrNotFound  error = redis.Nil
)

const (
	KeyValue = "value"
	KeyToken = "token"
	KeyFlags = "flags"
)

type RoundTripper struct {
	*memwire.Request
	Debug          bool
	Redis          *redis.Client
	RedisLock      *redislock.Client
	ResponseWriter *bufio.Writer
}

func (rt *RoundTripper) Reply(res *memwire.Response) (err error) {
	if rt.Noreply {
		if rt.Debug {
			log.Println("[debug] noreply")
		}
		return
	}
	if rt.Debug {
		log.Println("[debug] reply:", res.Response, res.Values)
	}
	if _, err = rt.ResponseWriter.WriteString(res.String()); err != nil {
		return
	}
	if err = rt.ResponseWriter.Flush(); err != nil {
		return
	}
	return
}

func (rt *RoundTripper) ReplyCode(code ...string) error {
	return rt.Reply(&memwire.Response{
		Response: strings.Join(code, " "),
	})
}

func (rt *RoundTripper) ReplyError(err error) error {
	if err == ErrExists {
		return rt.ReplyCode(memwire.CodeExists)
	}
	if err == ErrNotStored {
		return rt.ReplyCode(memwire.CodeNotStored)
	}
	if err == ErrNotFound {
		return rt.ReplyCode(memwire.CodeNotFound)
	}
	return rt.ReplyCode(memwire.CodeServerErr, err.Error())
}

func (rt *RoundTripper) WithLock(ctx context.Context, key string, fn func(ctx context.Context) error) error {
	obtain, err := rt.RedisLock.Obtain(ctx, "__LOCK."+key, time.Second, &redislock.Options{
		RetryStrategy: redislock.LinearBackoff(time.Millisecond * 100),
	})
	if err != nil {
		return err
	}
	defer obtain.Release(ctx)
	return fn(ctx)
}

func (rt *RoundTripper) Do(ctx context.Context) error {
	if rt.Debug {
		log.Println("[debug] request:", rt.Command, rt.Key, strings.Join(rt.Keys, ","), rt.Exptime)
	}
	switch rt.Command {
	case "set", "cas", "add", "replace":
		if err := rt.WithLock(ctx, rt.Key, func(ctx context.Context) error {
			var err error
			if rt.Command == "cas" || rt.Command == "add" || rt.Command == "replace" {
				var val map[string]string
				if val, err = rt.Redis.HGetAll(ctx, rt.Key).Result(); err != nil {
					if err == redis.Nil {
						switch rt.Command {
						case "cas":
							return err
						case "add":
							// no-op
						case "replace":
							return ErrNotStored
						}
					} else {
						return err
					}
				} else {
					switch rt.Command {
					case "cas":
						if val[KeyToken] != rt.Cas {
							return ErrExists
						}
					case "add":
						return ErrNotStored
					case "replace":
						// no-op
					}
				}
			}
			if err = rt.Redis.HSet(
				ctx,
				rt.Key,
				KeyValue, string(rt.Data),
				KeyFlags, rt.Flags,
				KeyToken, strconv.FormatInt(rand.Int63(), 10),
			).Err(); err != nil {
				return err
			}
			if rt.Exptime != 0 {
				if err = rt.Redis.Expire(
					ctx,
					rt.Key,
					time.Second*time.Duration(rt.Exptime),
				).Err(); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "get", "gets":
		res := &memwire.Response{}
		for _, key := range rt.Keys {
			var (
				err error
				val map[string]string
				flg string
				tkn string
			)
			if val, err = rt.Redis.HGetAll(ctx, key).Result(); err != nil {
				if err == redis.Nil {
					continue
				} else {
					return rt.ReplyError(err)
				}
			}
			flg = val[KeyFlags]
			if flg == "" {
				flg = "0"
			}
			if rt.Command == "gets" {
				tkn = val[KeyToken]
			}
			res.Values = append(res.Values, memwire.Value{
				Key:   key,
				Flags: flg,
				Data:  []byte(val[KeyValue]),
				Cas:   tkn,
			})
		}
		res.Response = memwire.CodeEnd
		return rt.Reply(res)
	case "delete":
		var count int
		for _, key := range rt.Keys {
			if err := rt.Redis.Del(ctx, key).Err(); err != nil {
				if err != redis.Nil {
					return rt.ReplyError(err)
				}
			} else {
				count++
			}
		}
		if count == 0 {
			return rt.ReplyCode(memwire.CodeNotFound)
		} else {
			return rt.ReplyCode(memwire.CodeDeleted)
		}
	case "incr", "decr", "append", "prepend":
		if err := rt.WithLock(ctx, rt.Key, func(ctx context.Context) (err error) {
			var val string
			if val, err = rt.Redis.HGet(ctx, rt.Key, KeyValue).Result(); err != nil {
				return
			}
			switch rt.Command {
			case "incr", "decr":
				var i int64
				if i, err = strconv.ParseInt(val, 10, 64); err != nil {
					return
				}
				switch rt.Command {
				case "incr":
					i = i + rt.Value
				case "decr":
					i = i - rt.Value
				}
				val = strconv.FormatInt(i, 10)
			case "append":
				val = val + string(rt.Data)
			case "prepend":
				val = string(rt.Data) + val
			}
			if err = rt.Redis.HSet(ctx, rt.Key, KeyValue, val).Err(); err != nil {
				return
			}
			return
		}); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "touch":
		if err := rt.Redis.Expire(ctx, rt.Key, time.Second*time.Duration(rt.Exptime)).Err(); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeTouched)
	case "version":
		return rt.ReplyCode("VERSION", "1")
	case "quit":
		return io.EOF
	default:
		// force send response
		rt.Noreply = false
		return rt.ReplyCode(memwire.CodeErr, rt.Command, "not implemented")
	}
}
