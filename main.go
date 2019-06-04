//////////////////////////////////////////////////////////////////////////////////
// main.go - Entry point, Universal methods
//////////////////////////////////////////////////////////////////////////////////
//	log(caller, message): Abstraction for all log messages; so they can be piped to alternate locations (IRC, SQL, /dev/null)
//  ktime(): Source for the time format used throughout (unixtime*1000)
//  safeMove(src, dst): OS-Independant safe file move
//  main(): Hello World!

package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/http2"
)

const esiURL = "https://esi.evetech.net"

var client http.Client

func log(caller string, message interface{}) {
	fmt.Printf("%.3f [%s] %s\n", float64(ktime())/1000, caller, message)
}
func ktime() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

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
	log("main.go:main()", "Hello World!")
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
			for i := 10000001; i < 10000070; i++ {
				newKjob("get", "/v1", "/markets/{region_id}/orders/", map[string]string{"region_id": strconv.Itoa(i)}, 1)
			}
		}
	}()

	for {
		time.Sleep(1 * time.Second)
	}
}
