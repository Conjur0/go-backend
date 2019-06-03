package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var etag map[string]map[string]string
var etagMutex = sync.RWMutex{}
var etagFile = "./etag.json"
var etagLastWrite int64
var etagDirty = false
var etagWriteTimer *time.Timer

func readEtags() {
	addMetric(etagFile)
	jsonFile, err := os.Open(etagFile)
	if err != nil {
		log("etag.go:readEtags() os.Open", err)
		return
	}
	defer jsonFile.Close()
	byteValue, errr := ioutil.ReadAll(jsonFile)
	if errr != nil {
		log("etag.go:readEtags() ioutil.ReadAll", err)
		return
	}
	if err = json.Unmarshal(byteValue, &etag); err != nil {
		log("etag.go:readEtags() json.Unmarshal", err)
		return
	}
	log("etag.go:readEtags()", fmt.Sprintf("Parsed %s %db, in %dms", etagFile, len(byteValue), getMetric(etagFile)))
}
func writeEtags() {
	addMetric(etagFile)
	if etagDirty == false {
		return
	}
	jsonFile, err := os.Create(etagFile + ".tmp")
	if err != nil {
		log("etag.go:writeEtags() os.Create", err)
		return
	}

	defer jsonFile.Close()
	jsonData, err := json.Marshal(etag)
	if err != nil {
		log("etag.go:writeEtags() json.Marshal", err)
		return
	}
	if _, err := jsonFile.Write(jsonData); err != nil {
		log("etag.go:writeEtags() jsonFile.Write", err)
		return
	}
	jsonFile.Close()
	safeMove(etagFile+".tmp", etagFile)
	etagLastWrite = time.Now().Unix()
	etagDirty = false
	log("etag.go:writeEtags()", fmt.Sprintf("Wrote %s %db, in %dms", etagFile, len(jsonData), getMetric(etagFile)))
}

func getEtag(uri string, id string) string {
	etagMutex.RLock()
	defer etagMutex.RUnlock()
	_, ok := etag[uri]
	if ok {
		_, ok := etag[uri][id]
		if ok {
			return etag[uri][id]
		}
	}
	return ""
}
func setEtag(uri string, id string, value string) {
	etagMutex.Lock()
	if _, ok := etag[uri]; ok == false {
		etag[uri] = make(map[string]string)
	}
	if etag[uri][id] != value {
		etag[uri][id] = value
		etagDirty = true
	}
	etagMutex.Unlock()
}
func etagWriteTimerInit() {
	etagWriteTimer = time.NewTimer(300 * time.Second)
	go func() {
		for range etagWriteTimer.C {
			writeEtags()
		}
	}()
}
