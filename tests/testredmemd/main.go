package main

import (
	"context"
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
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
	r := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	r.FlushDB(context.Background())
	mc := memcache.New("127.0.0.1:11211")
	if err = mc.Set(&memcache.Item{Key: "hello", Value: []byte("world")}); err != nil {
		return
	}
	var item *memcache.Item
	if item, err = mc.Get("hello"); err != nil {
		return
	}
	if string(item.Value) != "world" {
		err = errors.New("value mismatch")
		return
	}
	if err = mc.Add(&memcache.Item{Key: "hello", Value: []byte("world")}); err != nil {
		if err != memcache.ErrNotStored {
			err = errors.New("should be not stored")
			return
		}
		err = nil
	} else {
		err = errors.New("should be not stored")
		return
	}
	if err = mc.Add(&memcache.Item{Key: "hello1", Value: []byte("world1")}); err != nil {
		return
	}
	if err = mc.Replace(&memcache.Item{Key: "hello2", Value: []byte("world2")}); err != nil {
		if err != memcache.ErrNotStored {
			err = errors.New("should be not stored")
			return
		}
		err = nil
	} else {
		err = errors.New("should be not stored")
		return
	}
}
