package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

const esiURL = "https://esi.evetech.net"

var logMutex = sync.RWMutex{}
var client http.Client

func log(caller string, message interface{}) {
	logMutex.Lock()
	fmt.Printf("%.3f [%s] ", float64(ktime())/1000, caller)
	fmt.Println(message)
	logMutex.Unlock()
}
func ktime() int64 {
	return time.Now().UnixNano() / 1000000
}

func safeMove(src string, dst string) {
	os.Remove(dst + ".bak")
	os.Rename(dst, dst+".bak")
	if err := os.Rename(src, dst); err != nil {
		log("main.go:safeMove() os.Rename(src, dst)", err)
		return
	}
	os.Remove(dst + ".bak")
}

func main() {
	log("main.go:main()", "Hello World")
	spec = make(map[string]interface{})
	etag = make(map[string]map[string]string)
	metrics = make(map[string]int64)
	readEtags()

	client := &http.Client{}
	client.Transport = &http2.Transport{}

	go updateSpec()
	go readSpec("v1")
	go readSpec("v2")
	go readSpec("v3")
	go readSpec("v4")
	go readSpec("v5")
	go readSpec("v6")

	go kjobQueueInit()
	go kpageQueueInit()
	go etagWriteTimerInit()

	tock := time.NewTimer(3000000000) // 3s
	go func() {
		for range tock.C {
			//newKjob("get", "/v4", "/characters/{character_id}/skills/", map[string]string{"character_id": "1120048880"}, 0)
			for i := 10000001; i < 10000003; i++ {
				newKjob("get", "/v1", "/markets/{region_id}/orders/", map[string]string{"region_id": strconv.Itoa(i)}, 0)
			}
		}
	}()

	for {
		time.Sleep(1 * time.Second)
	}
}
