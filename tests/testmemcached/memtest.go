package main

import (
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

	var nv uint64
	if nv, err = mc.Increment("hello", 2); err != nil {
		return
	}

	log.Println(nv)
}
