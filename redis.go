//////////////////////////////////////////////////////////////////////////////////
// redis.go - redis abstraction
//////////////////////////////////////////////////////////////////////////////////
//  db 0: Unused
//  db 1: etag
//				cip:{cip} = {eTag}
//				etag:{etag} = {value}
//  db 2: ...

package main

import (
	"github.com/go-redis/redis"
)

var redisClient *redis.Client

func redisInit() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       1,
	})
	pong, err := redisClient.Ping().Result()
	if err != nil && pong == "PONG" {
		panic("Unable to ping redis!")
	}
	log("redis.go:redisInit()", "Initialization Complete!")

}
