// entry point

package main

import (
	"strconv"
	"time"
)

func fmain() {
	log(nil, "Hello World!")
}

func main() {
	log(nil, "Hello World!")
	initConfig()
	initClient()
	specInit()
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
