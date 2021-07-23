package main

import (
	"bufio"
	"context"
	"github.com/go-redis/redis/v8"
	"go.guoyk.net/redmemd/memwire"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type RoundTripper struct {
	*memwire.Request
	Debug          bool
	Prefix         string
	Client         *redis.Client
	ResponseWriter *bufio.Writer
}

func (rt *RoundTripper) CalculateExpires() time.Duration {
	return time.Second * time.Duration(rt.Exptime)
}

func (rt *RoundTripper) CalculateKey(key string) string {
	return rt.Prefix + key
}

func (rt *RoundTripper) CalculateFlagsKey(key string) string {
	return rt.Prefix + "__FLAGS." + key
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
	if err == redis.Nil {
		return rt.ReplyCode(memwire.CodeNotFound)
	}
	if strings.Contains(err.Error(), "is not an integer or out of range") {
		return rt.ReplyCode(memwire.CodeClientErr, err.Error())
	}
	return rt.ReplyCode(memwire.CodeServerErr, err.Error())
}

func (rt *RoundTripper) Do(ctx context.Context) error {
	if rt.Debug {
		log.Println("[debug] request:", rt.Command, rt.Key, strings.Join(rt.Keys, ","), rt.Exptime)
	}
	switch rt.Command {
	case "get":
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
					return rt.ReplyError(err)
				}
			}
			if flags, err = rt.Client.Get(ctx, rt.CalculateFlagsKey(key)).Result(); err != nil {
				if err == redis.Nil {
					flags = "0"
				} else {
					return rt.ReplyError(err)
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
			err := rt.Client.Del(
				ctx,
				rt.CalculateKey(key),
				rt.CalculateFlagsKey(key),
			).Err()
			if err != nil {
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
	case "set":
		if err := rt.Client.Set(
			ctx,
			rt.CalculateKey(rt.Key),
			string(rt.Data),
			rt.CalculateExpires(),
		).Err(); err != nil {
			return rt.ReplyError(err)
		}
		if err := rt.Client.Set(
			ctx,
			rt.CalculateFlagsKey(rt.Key),
			rt.Flags,
			rt.CalculateExpires(),
		).Err(); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "incr", "decr":
		var (
			err error
			val int64
		)
		if rt.Command == "incr" {
			val, err = rt.Client.IncrBy(
				ctx,
				rt.CalculateKey(rt.Key),
				rt.Value,
			).Result()
		} else {
			val, err = rt.Client.DecrBy(
				ctx,
				rt.CalculateKey(rt.Key),
				rt.Value,
			).Result()
		}
		if err != nil {
			return rt.ReplyError(err)
		} else {
			return rt.ReplyCode(strconv.FormatInt(val, 10))
		}
	case "append":
		if err := rt.Client.Append(
			ctx,
			rt.CalculateKey(rt.Key),
			string(rt.Data),
		).Err(); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "prepend":
		if err := rt.Client.Watch(ctx, func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				var (
					err error
					val string
				)
				if val, err = p.Get(ctx, rt.CalculateKey(rt.Key)).Result(); err != nil {
					return err
				}
				if err = p.Set(
					ctx,
					rt.CalculateKey(rt.Key),
					string(rt.Data)+val,
					redis.KeepTTL,
				).Err(); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, rt.CalculateKey(rt.Key)); err != nil {
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case `add`:
		var (
			err    error
			stored bool
		)
		if stored, err = rt.Client.SetNX(
			ctx,
			rt.CalculateKey(rt.Key),
			string(rt.Data),
			rt.CalculateExpires(),
		).Result(); err != nil {
			return rt.ReplyError(err)
		}
		if stored {
			if err = rt.Client.Set(
				ctx,
				rt.CalculateFlagsKey(rt.Key),
				rt.Flags,
				rt.CalculateExpires(),
			).Err(); err != nil {
				return rt.ReplyError(err)
			}
			return rt.ReplyCode(memwire.CodeStored)
		} else {
			return rt.ReplyCode(memwire.CodeNotStored)
		}
	case `replace`:
		if err := rt.Client.Watch(ctx, func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				var err error
				if err = p.Get(ctx, rt.CalculateKey(rt.Key)).Err(); err != nil {
					return err
				}
				if err = p.Set(
					ctx,
					rt.CalculateKey(rt.Key),
					string(rt.Data),
					redis.KeepTTL,
				).Err(); err != nil {
					return err
				}
				if err = p.Set(
					ctx,
					rt.CalculateFlagsKey(rt.Key),
					rt.Flags,
					redis.KeepTTL,
				).Err(); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, rt.CalculateKey(rt.Key)); err != nil {
			if err == redis.Nil {
				return rt.ReplyCode(memwire.CodeNotStored)
			}
			return rt.ReplyError(err)
		}
		return rt.ReplyCode(memwire.CodeStored)
	case "touch":
		if err := rt.Client.Expire(
			ctx,
			rt.CalculateKey(rt.Key),
			rt.CalculateExpires(),
		).Err(); err != nil {
			return rt.ReplyError(err)
		}
		if err := rt.Client.Expire(
			ctx,
			rt.CalculateFlagsKey(rt.Key),
			rt.CalculateExpires(),
		).Err(); err != nil {
			if err != redis.Nil {
				return rt.ReplyError(err)
			}
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
