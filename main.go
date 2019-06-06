//////////////////////////////////////////////////////////////////////////////////
// main.go - Entry point, Universal methods
//////////////////////////////////////////////////////////////////////////////////
//	log(caller, message): Abstraction for all log messages; so they can be piped to alternate locations (IRC, SQL, /dev/null)
//  ktime(): Source for the time format used throughout (unixtime*1000)
//  safeMove(src, dst): OS-Independant safe file move
//  main(): Hello World!

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/http2"
)

type conf struct {
	Domain      string  `json:"domain"`
	Email       string  `json:"email"`
	MaxInFlight int     `json:"max_in_flight"`
	Mariadb     mariadb `json:"mariadb"`
	Oauth       []oauth `json:"oauth"`
}

type mariadb struct {
	User string `json:"user"`
	Pass string `json:"pass"`
}

type oauth struct {
	Name         string `json:"name"`
	ClientID     string `json:"clientID"`
	ClientSecret string `json:"clientSecret"`
	Callback     string `json:"callback"`
	Redirect     string `json:"redirect"`
	AuthURL      string `json:"authURL"`
	RefererURL   string `json:"refererURL"`
	TokenURL     string `json:"tokenURL"`
	VerifyURL    string `json:"verifyURL"`
	RevokeURL    string `json:"revokeURL"`
}

const esiURL = "https://esi.evetech.net"

var client http.Client
var c conf

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

func readConfigJSON() {
	jsonFile, err := os.Open("./config.json")
	if err != nil {
		log("main.go:readConfigJSON() os.Open", err)
		panic(err)
	}
	defer jsonFile.Close()
	byteValue, errr := ioutil.ReadAll(jsonFile)
	if errr != nil {
		log("main.go:readConfigJSON() ioutil.ReadAll", err)
		panic(err)
	}
	if err := json.Unmarshal(byteValue, &c); err != nil {
		log("main.go:readConfigJSON() json.Unmarshal", err)
		panic(err)
	}
	log("main.go:readConfigJSON()", fmt.Sprintf("Read %db from ./config.json", len(byteValue)))
	byteValue = nil
}

func main() {
	log("main.go:main()", "Hello World!")
	readConfigJSON()
	spec = make(map[string]interface{})
	metrics = make(map[string]int64)
	redisInit()

	client := &http.Client{}
	client.Transport = &http2.Transport{}

	updateSpec()
	readSpec("v1")
	readSpec("v2")
	readSpec("v3")
	readSpec("v4")
	readSpec("v5")
	readSpec("v6")

	kjobQueueInit()
	kpageQueueInit()
	sqlInit()
	tablesInit()

	tock := time.NewTimer(3 * time.Second) // 3s
	go func() {
		for range tock.C {
			//newKjob("get", "/v4", "/characters/{character_id}/skills/", map[string]string{"character_id": "1120048880"}, 0)

			for i := 10000001; i < 10000070; i++ {
				newKjob("get", "/v1", "/markets/{region_id}/orders/", map[string]string{"region_id": strconv.Itoa(i)}, 0, tables["orders"])
			}
		}
	}()

	select {}
}
