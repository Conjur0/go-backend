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
	byteValue, errr := ioutil.ReadAll(jsonFile)
	jsonFile.Close()
	if errr != nil {
		log("etag.go:readEtags() ioutil.ReadAll", err)
		return
	}
	if err = json.Unmarshal(byteValue, &etag); err != nil {
		log("etag.go:readEtags() json.Unmarshal", err)
		return
	}
	byteValue = []byte("")
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

func getEtag(cip string) string {
	etagMutex.Lock()
	defer etagMutex.Unlock()
	_, ok := etag[cip]
	if ok {
		for key := range etag[cip] {
			return key
		}
	}
	return ""
}

func getEtagData(cip string) []byte {
	etagMutex.Lock()
	defer etagMutex.Unlock()
	_, ok := etag[cip]
	if ok {
		for key := range etag[cip] {
			return []byte(etag[cip][key])
		}
	}
	return []byte{}
}

func setEtag(cip string, tag string, value []byte) {
	etagMutex.Lock()
	if _, ok := etag[cip]; ok == false {
		etag[cip] = make(map[string]string)
	}
	var key = "none"
	for pey := range etag[cip] {
		if pey != tag {
			delete(etag[cip], key)
		}
		key = pey
	}
	if key != tag {
		etag[cip][tag] = string(value)
		etagDirty = true
	}
	etagMutex.Unlock()
}
func etagWriteTimerInit() {
	etagWriteTimer = time.NewTimer(30 * time.Second)
	go func() {
		for range etagWriteTimer.C {
			writeEtags()
		}
	}()
	log("etag.go:etagWriteTimerInit()", "Timer Initialized!")
}
