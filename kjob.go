package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var kjobStack = make(map[int]*kjob, 4096)
var kjobQueueLen int
var kjobQueueProcessed int
var kjobQueueFinished int
var kjobQueueTick time.Ticker
var minCachePct float64 = 10

type kjobQueueStruct struct {
	elements chan kjob
}

func kjobQueueInit() {
	kjobQueueTick := time.NewTicker(500 * time.Millisecond) //500ms
	go func() {
		for t := range kjobQueueTick.C {
			gokjobQueueTick(t)
		}
	}()
	log("kjob.go:kjobQueueInit()", "Timer Initialized!")
}
func gokjobQueueTick(t time.Time) {
	nowtime := ktime()
	for itt := range kjobStack {
		if kjobStack[itt].running == false && kjobStack[itt].NextRun < nowtime {
			log("kjob.go:gokjobQueueTick()", "Starting "+kjobStack[itt].CI)
			go kjobStack[itt].start()
		}
	}
}

type kjob struct {
	Method    string            `json:"method"`    //Method: 'get', 'put', 'update', 'delete'
	Spec      string            `json:"spec"`      //Spec: '/v1', '/v2', '/v3', '/v4', '/v5', '/v6'
	Endpoint  string            `json:"endpoint"`  //Endpoint: '/markets/{region_id}/orders/', '/characters/{character_id}/skills/'
	Entity    map[string]string `json:"entity"`    //Entity: "region_id": "10000002", "character_id": "1120048880"
	URL       string            `json:"url"`       //URL: concat of Spec and Endpoint, with entity processed
	StartTime int64             `json:"startTime"` //StartTime: milliseconds since 1970
	CI        string            `json:"ci"`        //CI: concat of endpoint:JSON.Marshall(entity)
	Cache     float64           `json:"cache"`     //Cache: milliseconds from cache miss

	Security string `json:"security"` //Security: EVESSO Token required or "none"
	Token    string `json:"token"`    //evesso access_token

	Ins    []string `json:"ins"`    //array of data waiting to be processed by sql
	InsIds []uint64 `json:"insIds"` //array of ids seen in this request (ones not here will be purged from sql upon completion of request)

	NextRun   int64   `json:"nextRun"`
	Expires   int64   `json:"expires"`    //milliseconds since 1970, when cache miss will occur
	ExpiresIn float64 `json:"expires_in"` //milliseconds until expiration, at completion of first page (or head) pulled

	APICalls  uint16 `json:"apiCalls"`  //count of API Calls (including head, errors, etc)
	APICache  uint16 `json:"apiCache"`  //count of 304 responses received
	APIErrors uint16 `json:"apiErrors"` //count of >304 statuses received

	BytesDownloaded int `json:"bytesDownloaded"` //total bytes downloaded
	BytesCached     int `json:"bytesCached"`     //total bytes cached

	PullType       uint16 `json:"pullType"`
	Pages          uint16 `json:"pages"`          //Pages: 0, 1
	PagesProcessed uint16 `json:"pagesProcessed"` //count of completed pages (excluding heads)
	PagesQueued    uint16 `json:"pagesQueued"`    //cumlative count of pages queued

	Records      uint16 `json:"records"`      //cumlative count of records
	ChangedRows  uint16 `json:"changedRows"`  //cumlative count of changed records
	AffectedRows uint16 `json:"affectedRows"` //cumlative count of affected records
	RemovedRows  uint16 `json:"removedRows"`  //cumlative count of removed records
	QueriesDone  uint16 `json:"queries_done"` //cumlative count of completed SQL Queries
	Queries      uint16 `json:"queries"`      //cumlative count of fired SQL Queries

	page    []kpage       //array of child kpages
	heart   *time.Timer   //heartbeat timer
	req     *http.Request //http request
	running bool
}

func newKjob(method string, specnum string, endpoint string, entity map[string]string, pages uint16) {

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
	tmp := kjob{
		Method:    method,
		Spec:      specnum,
		Endpoint:  endpoint,
		Entity:    entity,
		Pages:     pages,
		PullType:  pages,
		URL:       fmt.Sprintf("%s%s?datasource=tranquility", specnum, endpoint),
		heart:     time.NewTimer(2 * time.Second),
		StartTime: ktime(),
		CI:        fmt.Sprintf("%s|%s", endpoint, ciString),
		Cache:     cac * 1000,
		Security:  ecurity,
		running:   false}
	for se, sed := range entity {
		tmp.URL = strings.Replace(tmp.URL, "{"+se+"}", sed, -1)
	}

	go func() {
		for range tmp.heart.C {
			tmp.beat()
		}
	}()
	//tmp.insIds = make([]int64, 1048576)
	kjobStack[kjobQueueLen] = &tmp
	kjobQueueLen++
}
func (k *kjob) beat() {
	log("kjob.go:k.beat()", k.CI+" ZOMBIE!")
}
func (k *kjob) print() {
	jsonData, err := json.MarshalIndent(&k, "", "    ")
	if err != nil {
		panic(err)
	}
	log("kjob.go:k.print()", fmt.Sprintf("%s kjobQueue Len:%d Processed:%d Finished:%d", jsonData, kjobQueueLen, kjobQueueProcessed, kjobQueueFinished))
}
func (k *kjob) start() {
	if k.running {
		return
	}
	k.heart.Reset(2 * time.Second)
	k.running = true
	k.run()
}
func (k *kjob) stop() {
	//todo: final sql write, store metrics
	k.heart.Stop()
	k.running = false
	k.print()
	k.Token = "none"
	k.Ins = []string{}
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

	k.page = []kpage{}
	kjobQueueFinished++
}
func (k *kjob) run() {
	k.heart.Reset(2000000000)
	if k.Security != "none" && len(k.Token) < 5 {
		log("kjob.go:k.run() "+k.CI, "todo: get token.")
		return
	}
	if len(k.page) > 0 {
		log("kjob.go:k.run() "+k.CI, "ERROR: Already have pages?")
		return
	}
	if k.Pages == 0 {
		go k.requestHead()
		return
	}
	for i := uint16(1); i <= k.Pages; i++ {
		k.newPage(i)
	}
	return
}
func (k *kjob) requestHead() {
	//log("kjob.go:k.requestHead("+k.CI+")", "requesting head "+k.CI)
	addMetric("HEAD:" + k.CI)
	k.heart.Reset(2 * time.Second)
	k.APICalls++
	if backoff {
		k.heart.Reset(7 * time.Second)
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
		//k.print()
	} else if resp.StatusCode == 304 {
		//log("kjob.go:k.requestHead("+k.CI+") )", fmt.Sprintf("RCVD (304) %s in %dms", k.URL, getMetric("HEAD:"+k.CI)))
		//k.print()
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
			k.heart.Reset((2 * time.Second) + (time.Duration(k.ExpiresIn) * time.Millisecond))
			time.Sleep((500 * time.Millisecond) + (time.Duration(k.ExpiresIn) * time.Millisecond))
			go k.requestHead()
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
		log("kjob.go:k.requestHead("+k.CI+") )", fmt.Sprintf("RCVD (%d) HEAD(%d) %s in %dms", resp.StatusCode, k.Pages, k.URL, getMetric("HEAD:"+k.CI)))
		go k.run()
	}

}
func (k *kjob) updateExp(expire string) {
	exp, err := time.Parse("Mon, 02 Jan 2006 15:04:05 MST", expire)
	if err == nil {
		k.Expires = int64(exp.UnixNano() / int64(time.Millisecond))
		k.ExpiresIn = float64(k.Expires - ktime())
		k.NextRun = k.Expires + 1000
	}
}
