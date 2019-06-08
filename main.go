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
	EsiURL      string  `json:"esi_url"`
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

var eveRegions = []int{10000001, 10000002, 10000003, 10000004, 10000005, 10000006, 10000007, 10000008, 10000009, 10000010, 10000011, 10000012, 10000013, 10000014, 10000015, 10000016, 10000017, 10000018, 10000019, 10000020, 10000021, 10000022, 10000023, 10000025, 10000027, 10000028, 10000029, 10000030, 10000031, 10000032, 10000033, 10000034, 10000035, 10000036, 10000037, 10000038, 10000039, 10000040, 10000041, 10000042, 10000043, 10000044, 10000045, 10000046, 10000047, 10000048, 10000049, 10000050, 10000051, 10000052, 10000053, 10000054, 10000055, 10000056, 10000057, 10000058, 10000059, 10000060, 10000061, 10000062, 10000063, 10000064, 10000065, 10000066, 10000067, 10000068, 10000069}

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
	blah := make(map[int]string, 500000000)
	blah[31337] = "poo"
}

func rmain() {
	log("main.go:main()", "Hello World!")
	readConfigJSON()
	metrics = make(map[string]int64)

	client := &http.Client{}
	client.Transport = &http2.Transport{}

	kjobQueueInit()
	kpageQueueInit()
	sqlInit()
	tablesInit()
	etagInit()

	tock := time.NewTimer(3 * time.Second) // 3s
	go func() {
		for range tock.C {
			//newKjob("get", "/v4", "/characters/{character_id}/skills/", map[string]string{"character_id": "1120048880"}, 0)

			for i := range eveRegions {
				newKjob("get", "/v1", "/markets/{region_id}/orders/", map[string]string{"region_id": strconv.Itoa(eveRegions[i])}, 0, tables["orders"])
			}
			for i := range eveRegions {
				newKjob("get", "/v1", "/contracts/public/{region_id}/", map[string]string{"region_id": strconv.Itoa(eveRegions[i])}, 0, tables["contracts"])
			}

		}
	}()

	select {}
}
