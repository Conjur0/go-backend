package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var wg sync.WaitGroup

func pmain() {
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
			var resp responses
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
	log("spec.go:parseSpec()", fmt.Sprintf("Processed %d items for %s://%s%s\n", entries, tmp.Schemes[0], tmp.Host, tmp.BasePath))

	wg.Done()
}

type specdef struct {
	BasePath string                          `json:"basePath"`
	Host     string                          `json:"host"`
	Paths    map[string]map[string]operation `json:"paths"`
	Schemes  []string                        `json:"schemes"`
}

type operation struct {
	Responses      map[string]responses `json:"responses"`
	Security       []security           `json:"security"`
	XCachedSeconds int64                `json:"x-cached-seconds"`
}

type responses struct {
	Headers headers `json:"headers"`
	Schema  schema  `json:"schema"`
}
type security struct {
	Evesso []string `json:"evesso"`
}

type headers struct {
	XPages headerdef `json:"X-Pages"`
}
type headerdef struct {
	Type string `json:"type"`
}

type schema struct {
	MaxItems int64 `json:"maxItems"`
}
