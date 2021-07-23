package main

import (
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
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
}
