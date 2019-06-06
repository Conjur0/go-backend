//////////////////////////////////////////////////////////////////////////////////
// kjob.go - ESI Job Management
//////////////////////////////////////////////////////////////////////////////////
//  kjobQueueInit():  Timer Init (called once from main)
//  gokjobQueueTick(t):  Timer tick function
//  newKjob(method, specnum, endpoint, entity, pages):  Initializes a new ESI Job, and adds it to the revolving door
//  kjob.beat():  Zombie Hunter- goal is for this to never be called.
//  kjob.print(string):  Universal "print all the things" function for debugging
//  kjob.start():  Brings kjob out of hibernation
//  kjob.stop():  Places kjob into hibernation
//  kjob.run():  Performs the next step, to process kjob
//  kjob.queuePages():  Adds all currently un-queued pages to the pages queue
//  kjob.requestHead():  Pulls a HEAD for the page, to identify expiration and pagecount
//  kjob.updateExp():  Centralized method for updating kjob expiration

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var minCachePct float64 = 10

var kjobStack = make(map[int]*kjob, 4096)
var kjobStackMutex = sync.RWMutex{}
var kjobQueueLen int
var kjobQueueTick *time.Ticker

type kjobQueueStruct struct {
	elements chan kjob
}

func kjobQueueInit() {
	kjobQueueTick = time.NewTicker(500 * time.Millisecond) //500ms
	go func() {
		for t := range kjobQueueTick.C {
			gokjobQueueTick(t)
		}
	}()
	log("kjob.go:kjobQueueInit()", "Timer Initialized!")
}
func gokjobQueueTick(t time.Time) {
	nowtime := ktime()
	kjobStackMutex.Lock()
	for itt := range kjobStack {
		if kjobStack[itt].running == false && kjobStack[itt].NextRun < nowtime {
			go kjobStack[itt].start()
		}
	}
	kjobStackMutex.Unlock()
}

type kjob struct {
	Method   string            `json:"method"`   //Method: 'get', 'put', 'update', 'delete'
	Spec     string            `json:"spec"`     //Spec: '/v1', '/v2', '/v3', '/v4', '/v5', '/v6'
	Endpoint string            `json:"endpoint"` //Endpoint: '/markets/{region_id}/orders/', '/characters/{character_id}/skills/'
	Entity   map[string]string `json:"entity"`   //Entity: "region_id": "10000002", "character_id": "1120048880"
	URL      string            `json:"url"`      //URL: concat of Spec and Endpoint, with entity processed
	CI       string            `json:"ci"`       //CI: concat of endpoint:JSON.Marshall(entity)
	Cache    float64           `json:"cache"`    //Cache: milliseconds from cache miss

	Security string `json:"security"` //Security: EVESSO Token required or "none"
	Token    string `json:"token"`    //evesso access_token
	MaxItems int    `json:"maxItems"`

	Ins       []string `json:"ins"`    //array of data waiting to be processed by sql
	InsIds    []string `json:"insIds"` //array of ids seen in this request (ones not here will be purged from sql upon completion of request)
	NextRun   int64    `json:"nextRun"`
	Expires   int64    `json:"expires"`    //milliseconds since 1970, when cache miss will occur
	ExpiresIn float64  `json:"expires_in"` //milliseconds until expiration, at completion of first page (or head) pulled

	APICalls  uint16 `json:"apiCalls"`  //count of API Calls (including head, errors, etc)
	APICache  uint16 `json:"apiCache"`  //count of 304 responses received
	APIErrors uint16 `json:"apiErrors"` //count of >304 statuses received

	BytesDownloaded int `json:"bytesDownloaded"` //total bytes downloaded
	BytesCached     int `json:"bytesCached"`     //total bytes cached

	PullType       uint16 `json:"pullType"`
	Pages          uint16 `json:"pages"`          //Pages: 0, 1
	PagesProcessed uint16 `json:"pagesProcessed"` //count of completed pages (excluding heads)
	PagesQueued    uint16 `json:"pagesQueued"`    //cumlative count of pages queued

	Records      uint16        `json:"records"`      //cumlative count of records
	ChangedRows  uint16        `json:"changedRows"`  //cumlative count of changed records
	AffectedRows uint16        `json:"affectedRows"` //cumlative count of affected records
	RemovedRows  uint16        `json:"removedRows"`  //cumlative count of removed records
	QueriesDone  uint16        `json:"queries_done"` //cumlative count of completed SQL Queries
	Queries      uint16        `json:"queries"`      //cumlative count of fired SQL Queries
	Runs         int           `json:"runs"`
	heart        *time.Timer   //heartbeat timer
	req          *http.Request //http request
	running      bool
	mutex        sync.RWMutex
	page         map[uint16]*kpage
	table        *table
}

func newKjob(method string, specnum string, endpoint string, entity map[string]string, pages uint16, table *table) {
	l1, ok := spec[specnum].(map[string]interface{})
	if ok == false {
		log("kjob.go:newKjob()", "Invalid job received: SPEC "+specnum+" invalid")
		return
	}
	l2, ok := l1["paths"].(map[string]interface{})
	if ok == false {
		log("kjob.go:newKjob()", "Invalid job received: PATHS not found in "+specnum)
		return
	}
	l3, ok := l2[endpoint].(map[string]interface{})
	if ok == false {
		log("kjob.go:newKjob()", "Invalid job received: route "+endpoint+" does not exist at "+specnum)
		return
	}
	tspec, ok := l3[method].(map[string]interface{})
	if ok == false {
		log("kjob.go:newKjob()", "Invalid job received: "+method+" is invalid for "+specnum+endpoint)
		return
	}

	sec, ok := tspec["security"]
	ecurity := "none"
	if ok {
		ecurity = sec.([]interface{})[0].(map[string]interface{})["evesso"].([]interface{})[0].(string)
	}
	cac, ok := tspec["x-cached-seconds"].(float64)
	if ok == false {
		log("kjob.go:newKjob()", "Invalid job received")
		return
	}
	ciString, _ := json.Marshal(entity)
	axItems := 1
	if xItems, ok := tspec["responses"].(map[string]interface{})["200"].(map[string]interface{})["schema"].(map[string]interface{})["maxItems"].(float64); ok {
		axItems = int(xItems)
	}
	tmp := kjob{
		Method:   method,
		Spec:     specnum,
		Endpoint: endpoint,
		Entity:   entity,
		Token:    "none",
		Pages:    pages,
		PullType: pages,
		URL:      fmt.Sprintf("%s%s?datasource=tranquility", specnum, endpoint),
		heart:    time.NewTimer(30 * time.Second),
		CI:       fmt.Sprintf("%s|%s", endpoint, ciString),
		Cache:    cac * 1000,
		Security: ecurity,
		MaxItems: axItems,
		mutex:    sync.RWMutex{},
		page:     make(map[uint16]*kpage),
		table:    table}
	for se, sed := range entity {
		tmp.URL = strings.Replace(tmp.URL, "{"+se+"}", sed, -1)
	}

	go func() {
		for range tmp.heart.C {
			tmp.beat()
		}
	}()
	//tmp.insIds = make([]int64, 1048576)
	kjobStackMutex.Lock()
	kjobStack[kjobQueueLen] = &tmp
	kjobQueueLen++
	kjobStackMutex.Unlock()
}
func (k *kjob) beat() {
	k.print("ZOMBIE")
	k.mutex.Lock()
	k.stop(true)
	k.mutex.Unlock()
	k.start()
}
func (k *kjob) print(msg string) {
	// jsonData, err := json.MarshalIndent(&k, "", "    ")
	// if err != nil {
	// 	panic(err)
	// }
	// log("kjob.go:k.print("+msg+")", fmt.Sprintf("%s %s cache:%.0f security:%s token:%s nextRun:%d, expires:%d, expires_in:%.0f APICalls:%d, APICache:%d, APIErrors:%d bytesDownloaded:%d, bytesCached: %d PullType:%d, Pages:%d, pagesProcessed:%d, pagesQueued:%d, runs:%d, kjobQueue Len:%d Processed:%d Finished:%d Runtime:%dms",
	// 	k.Method, k.CI,
	// 	k.Cache, k.Security, k.Token,
	// 	k.NextRun, k.Expires, k.ExpiresIn,
	// 	k.APICalls, k.APICache, k.APIErrors,
	// 	k.BytesDownloaded, k.BytesCached,
	// 	k.PullType, k.Pages, k.PagesProcessed, k.PagesQueued, k.Runs,
	// 	kjobQueueLen, kjobQueueProcessed, kjobQueueFinished, getMetric(k.CI)))
	log("kjob.go:k.print("+msg+")", fmt.Sprintf("%s %s %.0f %s %s %d %d %.0f %d %d %d %d %d %d %d %d %d %d %d %d",
		k.Method, k.CI,
		k.Cache, k.Security, k.Token,
		k.NextRun, k.Expires, k.ExpiresIn,
		k.APICalls, k.APICache, k.APIErrors,
		k.BytesDownloaded, k.BytesCached,
		k.PullType, k.Pages, k.PagesProcessed, k.PagesQueued, k.Runs,
		kjobQueueLen, getMetric(k.CI)))
}
func (k *kjob) start() {
	k.mutex.Lock()
	k.heart.Reset(30 * time.Second)
	k.running = true
	k.Runs++
	addMetric(k.CI)
	k.mutex.Unlock()
	k.run()
}
func (k *kjob) stop(zombie bool) {
	//todo: final sql write, store metrics
	//k.mutex.Lock() **Should be locked in a wrapper around the call to this.
	k.heart.Stop()
	for it := range k.page {
		k.page[it].destroy()
	}
	k.running = false
	k.Token = "none"
	k.Ins = nil
	k.InsIds = nil
	k.Expires = 0
	k.APICalls = 0
	k.APICache = 0
	k.APIErrors = 0
	k.BytesDownloaded = 0
	k.BytesCached = 0
	k.Pages = k.PullType
	k.PagesProcessed = 0
	k.PagesQueued = 0
	k.Records = 0
	k.ChangedRows = 0
	k.AffectedRows = 0
	k.RemovedRows = 0
	k.QueriesDone = 0
	k.Queries = 0
	//k.mutex.Unlock()
}
func (k *kjob) run() {
	//k.heart.Reset(30 * time.Second)
	if k.Security != "none" && len(k.Token) < 5 {
		log("kjob.go:k.run() "+k.CI, "todo: get token.")
		return
	}
	if k.Pages == 0 {
		go k.requestHead()
		return
	}
	if k.Pages != k.PagesQueued {
		k.queuePages()
	}
	return
}
func (k *kjob) queuePages() {
	k.mutex.Lock()
	//log("kjob.go:k.queuePages()", fmt.Sprintf("%s Queueing pages %d to %d", k.CI, k.PagesQueued+1, k.Pages))
	for i := k.PagesQueued + 1; i <= k.Pages; i++ {
		k.newPage(i)
	}
	k.mutex.Unlock()
}
func (k *kjob) requestHead() {
	//log("kjob.go:k.requestHead("+k.CI+")", "requesting head "+k.CI)
	addMetric("HEAD:" + k.CI)
	//k.heart.Reset(30 * time.Second)
	k.APICalls++
	if backoff {
		time.Sleep(5 * time.Second)
		go k.requestHead()
		return
	}
	etaghdr := getEtag(k.CI)

	req, err := http.NewRequest("HEAD", esiURL+k.URL, nil)
	if err != nil {
		log("kjob.go:k.requestHead("+k.CI+") http.NewRequest", err)
		return
	}
	k.req = req
	if len(etaghdr) > 0 {
		k.req.Header.Add("If-None-Match", etaghdr)
	}
	if k.Security != "none" && len(k.Token) > 5 {
		k.req.Header.Add("Authorization", "Bearer "+k.Token)
	}
	resp, err := client.Do(k.req)
	if err != nil {
		log("kjob.go:k.requestHead("+k.CI+") client.Do", err)
		return
	}
	/*
				TODO: re-add error_limit/backoff
				      if (this.response_headers['x-esi-error-limit-remain'] && (this.response_headers[':status'] > 399)) {
		        error_remain = this.response_headers['x-esi-error-limit-remain'];
		        clearTimeout(error_reset_timer);
		        error_reset_timer = setTimeout(() => { error_remain = 100; }, (parseInt(this.response_headers['x-esi-error-limit-reset']) * 1000));

		        if (error_remain < 30) {
		          console.log("Backing off!");
		          backoff = true;
		          setTimeout(() => { backoff = false; console.log("Resuming..."); }, 20000);
		        }

					}
	*/
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		if _, ok := resp.Header["Etag"]; ok {
			go setEtag(k.CI, resp.Header["Etag"][0], []byte(""))
		}
		//log("kjob.go:k.requestHead("+k.CI+") )", fmt.Sprintf("RCVD (200) %s in %dms", k.URL, getMetric("HEAD:"+k.CI)))
		//k.print("")
	} else if resp.StatusCode == 304 {
		//log("kjob.go:k.requestHead("+k.CI+") )", fmt.Sprintf("RCVD (304) %s in %dms", k.URL, getMetric("HEAD:"+k.CI)))
		//k.print("")
	}

	if resp.StatusCode == 200 || resp.StatusCode == 304 {
		k.updateExp(resp.Header["Expires"][0])

		var timepct float64
		if k.ExpiresIn > 0 {
			timepct = 100 * (float64(k.ExpiresIn) / float64(k.Cache))
		} else {
			timepct = 0
		}
		if timepct < minCachePct {
			log("kjob.go:k.requestHead("+k.CI+") )", k.CI+" expires too soon, recycling!")
			k.mutex.Lock()
			k.stop(false)
			k.mutex.Unlock()
			return
		}
		if pgs, ok := resp.Header["X-Pages"]; ok {
			pgss, err := strconv.Atoi(pgs[0])
			if err == nil {
				k.Pages = uint16(pgss)
			} else {
				k.Pages = 1
			}

		}
		//log("kjob.go:k.requestHead("+k.CI+") )", fmt.Sprintf("RCVD (%d) HEAD(%d) %s in %dms", resp.StatusCode, k.Pages, k.URL, getMetric("HEAD:"+k.CI)))
		go k.run()
	}

}

//"2006-01-02T15:04:05Z"
//"Mon, 02 Jan 2006 15:04:05 MST"
func (k *kjob) updateExp(expire string) {
	exp, err := time.Parse("Mon, 02 Jan 2006 15:04:05 MST", expire)
	if err == nil {
		k.mutex.Lock()
		k.Expires = int64(exp.UnixNano() / int64(time.Millisecond))
		k.ExpiresIn = float64(k.Expires - ktime())
		k.NextRun = k.Expires + 750
		k.mutex.Unlock()
	}
}
