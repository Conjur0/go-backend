//////////////////////////////////////////////////////////////////////////////////
// etag.go - eTag Interface
//////////////////////////////////////////////////////////////////////////////////
//  readEtags(): reads json `etagFile`, and unmarshals into `etag`
//  writeEtags(): Marshals contents of `etag` to json `etagFile` if `etagDirty`
//  getEtag(cip): returns the Entity Tag for the CIP
//  getEtagData(cip): returns the stored data for the given CIP
//  setEtag(cip, tag, value): removes existing eTags for the given CIP, records the new tag and data, and marks the file as dirty
//  etagWriteTimerInit(): Timer Init (called once from main)

package main

import (
	"time"
)

func getEtag(cip string) string {
	data, err := redisClient.Get("cip:" + cip).Result()
	if err != nil {
		return ""
	}
	go redisClient.Expire("cip:"+cip, 1*time.Hour).Result()
	return data
}

func getEtagData(tag string) []byte {
	data, err := redisClient.Get("etag:" + tag).Result()
	if err != nil {
		return []byte("")
	}
	go redisClient.Expire("etag:"+tag, 1*time.Hour).Result()
	return []byte(data)
}

func setEtag(cip string, tag string, value []byte) {
	redisClient.SetNX("cip:"+cip, tag, 1*time.Hour).Err()
	redisClient.SetNX("etag:"+tag, value, 1*time.Hour).Err()
}
