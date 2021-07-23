package main

import (
	"bufio"
	"context"
	"github.com/go-redis/redis/v8"
	"go.guoyk.net/redmemd/memwire"
	"strconv"
	"strings"
	"time"
)

type RoundTripper struct {
	*memwire.Request
	Prefix         string
	Client         *redis.Client
	ResponseWriter *bufio.Writer
}

func (rt *RoundTripper) CalculateKey(key string) string {
	return rt.Prefix + key
}

func (rt *RoundTripper) CalculateFlagsKey(key string) string {
	return rt.Prefix + "__FLAGS." + key
}

func (rt *RoundTripper) Reply(res *memwire.Response) (err error) {
	if rt.Noreply {
		return
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

func (rt *RoundTripper) ReplyServerError(err error) error {
	return rt.ReplyCode(memwire.CodeServerErr, err.Error())
}

func (rt *RoundTripper) Do(ctx context.Context) error {
	switch rt.Command {
	case "get", "gets":
		res := &memwire.Response{}
		for _, key := range rt.Keys {
			var (
				err   error
				val   string
				flags string
			)
			if val, err = rt.Client.Get(ctx, rt.CalculateKey(key)).Result(); err != nil {
				if err == redis.Nil {
					continue
				} else {
					return rt.ReplyServerError(err)
				}
			}
			if flags, err = rt.Client.Get(ctx, rt.CalculateFlagsKey(key)).Result(); err != nil {
				if err == redis.Nil {
					flags = "0"
				} else {
					return rt.ReplyServerError(err)
				}
			}
			res.Values = append(res.Values, memwire.Value{
				Key:   key,
				Flags: flags,
				Data:  []byte(val),
			})
		}
		res.Response = memwire.CodeEnd
		return rt.Reply(res)
	case "delete":
		var count int
		for _, key := range rt.Keys {
			err := rt.Client.Del(ctx, rt.CalculateKey(key), rt.CalculateFlagsKey(key)).Err()
			if err != nil && err != redis.Nil {
				return rt.ReplyServerError(err)
			} else {
				count++
			}
		}
		if count == 0 {
			return rt.ReplyCode(memwire.CodeNotFound)
		} else {
			return rt.ReplyCode(memwire.CodeDeleted)
		}
	case "set":
		if err := rt.Client.Set(ctx, rt.CalculateKey(rt.Key), string(rt.Data), time.Second*time.Duration(rt.Exptime)).Err(); err != nil {
			return rt.ReplyServerError(err)
		}
		if err := rt.Client.Set(ctx, rt.CalculateFlagsKey(rt.Key), rt.Flags, time.Second*time.Duration(rt.Exptime)).Err(); err != nil {
			return rt.ReplyServerError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "incr", "decr":
		var (
			err error
			val int64
		)
		if rt.Command == "incr" {
			val, err = rt.Client.IncrBy(ctx, rt.CalculateKey(rt.Key), rt.Value).Result()
		} else {
			val, err = rt.Client.DecrBy(ctx, rt.CalculateKey(rt.Key), rt.Value).Result()
		}
		if err != nil {
			return rt.ReplyServerError(err)
		} else {
			return rt.ReplyCode(strconv.FormatInt(val, 10))
		}
	case "touch":
		if err := rt.Client.Expire(ctx, rt.CalculateKey(rt.Key), time.Second*time.Duration(rt.Exptime)).Err(); err != nil {
			if err == redis.Nil {
				return rt.ReplyCode(memwire.CodeNotFound)
			} else {
				return rt.ReplyServerError(err)
			}
		}
		if err := rt.Client.Expire(ctx, rt.CalculateFlagsKey(rt.Key), time.Second*time.Duration(rt.Exptime)).Err(); err != nil {
			if err != redis.Nil {
				return rt.ReplyServerError(err)
			}
		}
		return rt.ReplyCode(memwire.CodeTouched)
	case "version":
		return rt.ReplyCode("VERSION", "1")
	default:
		// force send response
		rt.Noreply = false
		return rt.ReplyCode(memwire.CodeErr, rt.Command, "not implemented")
	}
}
