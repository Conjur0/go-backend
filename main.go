// entry point

package main

func fmain() {
	log(nil, "Hello World!")
}

var runningID = 0

func main() {
	log(nil, "Hello World!")
	initConfig()
	sqlInit()
	tablesInit()
	initClient()
	kjobInit()
	kpageInit()
	etagInit()
	tokensInit()

	getJobs()
	// tock := time.NewTimer(3 * time.Second) // 3s
	// go func() {
	// 	for range tock.C {
	// 		// //newKjob("get", "/v4", "/characters/{character_id}/skills/", map[string]string{"character_id": "1120048880"}, 0)
	// 		for i := range eveRegions {
	// 			runningID++
	// 			newKjob(runningID, "get", "/v1", "/markets/{region_id}/orders/", fmt.Sprintf("{\"region_id\": \"%d\"}", eveRegions[i]), 0, "orders", 0)
	// 		}
	// 	}
	// }()
	// cock := time.NewTimer(20 * time.Second) // 3s
	// go func() {
	// 	for range cock.C {
	// 		// //newKjob("get", "/v4", "/characters/{character_id}/skills/", map[string]string{"character_id": "1120048880"}, 0)
	// 		for i := range eveRegions {
	// 			runningID++
	// 			newKjob(runningID, "get", "/v1", "/contracts/public/{region_id}/", fmt.Sprintf("{\"region_id\": \"%d\"}", eveRegions[i]), 0, "contracts", 0)
	// 		}
	// 	}
	// }()
	//createJob("get", "/v1", "/markets/{region_id}/orders/", "{\"region_id\": \"10000002\"}", 0, "orders", ktime()+86400000)
	select {}
}
