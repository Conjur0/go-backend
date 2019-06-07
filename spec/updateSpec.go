package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var configFile = "../config.json"

type conf struct {
	EsiURL  string  `json:"esi_url"`
	Mariadb mariadb `json:"mariadb"`
}

type mariadb struct {
	User string `json:"user"`
	Pass string `json:"pass"`
}

var database *sql.DB
var c conf
var client http.Client
var wg sync.WaitGroup

func readConfigJSON() {
	jsonFile, err := os.Open(configFile)
	if err != nil {
		log("updateSpec.go:readConfigJSON() os.Open", err)
		panic(err)
	}
	defer jsonFile.Close()
	byteValue, errr := ioutil.ReadAll(jsonFile)
	if errr != nil {
		log("updateSpec.go:readConfigJSON() ioutil.ReadAll", err)
		panic(err)
	}
	if err := json.Unmarshal(byteValue, &c); err != nil {
		log("updateSpec.go:readConfigJSON() json.Unmarshal", err)
		panic(err)
	}
	log("updateSpec.go:readConfigJSON()", fmt.Sprintf("Read %db from %s", len(byteValue), configFile))
	byteValue = nil
}
func log(caller string, message interface{}) {
	fmt.Printf("%.3f [%s] %s\n", float64(ktime())/1000, caller, message)
}
func ktime() int64 { return time.Now().UnixNano() / int64(time.Millisecond) }

func main() {
	readConfigJSON()

	var err error
	database, err = sql.Open("mysql", c.Mariadb.User+":"+c.Mariadb.Pass+"@tcp(127.0.0.1:3306)/?maxAllowedPacket=0")
	fmt.Println("Hello")
	if err != nil {
		panic("Unable to connect to sql!")
	}
	log("sql.go:sqlInit()", "Initialization Complete!")
	//parseSpec("v2")
	paths := getSpecPaths()
	wg.Add(len(paths))
	for it := range paths {
		go parseSpec(paths[it])
	}
	wg.Wait()

}

func getSpecPaths() []string {
	var tmp []string
	resp, err := http.Get(c.EsiURL + "/versions/")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	byteValue, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(byteValue, &tmp); err != nil {
		panic(err)
	}
	return tmp
}

func parseSpec(path string) {
	if path == "dev" || path == "latest" || path == "legacy" {
		wg.Done()
		return
	}
	resp, err := http.Get(c.EsiURL + "/" + path + "/swagger.json")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	byteValue, err := ioutil.ReadAll(resp.Body)
	var tmp specdef
	if err := json.Unmarshal([]byte(byteValue), &tmp); err != nil {
		log("spec.go:parseSpec() json.Unmarshal", err)
		return
	}
	var b strings.Builder
	spec := tmp.BasePath
	comma := ""
	entries := 0
	for endpoint := range tmp.Paths {
		for method := range tmp.Paths[endpoint] {
			security := ""
			if len(tmp.Paths[endpoint][method].Security) == 1 && len(tmp.Paths[endpoint][method].Security[0].Evesso) == 1 {
				security = tmp.Paths[endpoint][method].Security[0].Evesso[0]
			}
			//find responses
			var ok bool
			var resp Responses
			if resp, ok = tmp.Paths[endpoint][method].Responses["200"]; !ok {
				if resp, ok = tmp.Paths[endpoint][method].Responses["201"]; !ok {
					if resp, ok = tmp.Paths[endpoint][method].Responses["204"]; !ok {
						// well poo. not ok.
					}
				}
			}

			paged := 1
			if resp.Headers.XPages.Type == "integer" {
				paged = 0
			}
			maxitems := resp.Schema.MaxItems
			cache := tmp.Paths[endpoint][method].XCachedSeconds

			fmt.Fprintf(&b, "%s('%s','%s','%s','%s',%d,%d,%d)", comma, method, spec, endpoint, security, cache, maxitems, paged)
			entries++
			comma = ","
		}
	}
	if b.Len() > 0 {
		database.Query(fmt.Sprintf("INSERT INTO `karkinos`.`spec` (method,spec,endpoint,security,cache,items,paged) VALUES %s ON DUPLICATE KEY UPDATE security=VALUES(security),cache=VALUES(cache),items=VALUES(items),paged=VALUES(paged)", b.String()))
	}
	log("spec.go:parseSpec()", fmt.Sprintf("Processed %d items for %s://%s%s\n\n", entries, tmp.Schemes[0], tmp.Host, tmp.BasePath))

	wg.Done()
}

type specdef struct {
	BasePath string                          `json:"basePath"`
	Host     string                          `json:"host"`
	Paths    map[string]map[string]Operation `json:"paths"`
	Schemes  []string                        `json:"schemes"`
}

type Operation struct {
	Responses      map[string]Responses `json:"responses"`
	Security       []Security           `json:"security"`
	XCachedSeconds int64                `json:"x-cached-seconds"`
}

type Responses struct {
	Headers Headers `json:"headers"`
	Schema  Schema  `json:"schema"`
}
type Security struct {
	Evesso []string `json:"evesso"`
}

type Headers struct {
	XPages Headerdef `json:"X-Pages"`
}
type Headerdef struct {
	Type string `json:"type"`
}

type Schema struct {
	MaxItems int64 `json:"maxItems"`
}
