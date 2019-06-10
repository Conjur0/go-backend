//////////////////////////////////////////////////////////////////////////////////
// kjob.go - ESI Job Management
//////////////////////////////////////////////////////////////////////////////////
//  kjobQueueInit():  Timer Init (called once from main)
//  gokjobQueueTick(t):  Timer tick function
//  newKjob(method, specnum, endpoint, entity, pages):  Initializes a new ESI Job, and adds it to the revolving door
//  kjob.beat():  Zombie Hunter- goal is for this to never be called.
//  kjob.print(string):  Universal "print all the things" function for debugging
//  kjob.start():  Brings kjob out of hibernation
//  kjob.stopJob():  Places kjob into hibernation
//  kjob.run():  Performs the next step, to process kjob
//  kjob.queuePages():  Adds all currently un-queued pages to the pages queue
//  kjob.requestHead():  Pulls a HEAD for the page, to identify expiration and pagecount
//  kjob.updateExp():  Centralized method for updating kjob expiration

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var kjobStack = make(map[int]*kjob, 4096)
var kjobStackMutex debugOnlyMutex
var kjobQueueLen int
var kjobQueueTick *time.Ticker
var joblog *sql.Stmt

type kjobQueueStruct struct {
	elements chan kjob
}

// initialize kjob queue ticker
func kjobInit() {
	kjobQueueTick = time.NewTicker(500 * time.Millisecond) //500ms
	go gokjobQueueTick()

	tables["jobs"] = &table{
		database:   "karkinos",
		name:       "jobs",
		primaryKey: "id",
		uniqueKeys: map[string]string{
			"msee": "method:spec:endpoint:entity",
		},
		_columnOrder: []string{
			"method",
			"spec",
			"endpoint",
			"entity",
			"pages",
			"`table`",
			"nextrun",
		},
		proto: []string{
			"id INT NOT NULL AUTO_INCREMENT",
			"method VARCHAR(12) NOT NULL",
			"spec VARCHAR(10) NOT NULL",
			"endpoint VARCHAR(110) NOT NULL",
			"entity VARCHAR(250) NOT NULL",
			"pages TINYINT NOT NULL",
			"`table` VARCHAR(12) NOT NULL",
			"nextrun BIGINT NOT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}

	tables["job_log"] = &table{
		database:   "karkinos",
		name:       "job_log",
		primaryKey: "id",
		keys: map[string]string{
			"job_id": "job_id",
		},
		_columnOrder: []string{
			"`time`",
			"job_id",
			"`pages`",
			"records",
			"affected",
			"removed",
			"runtime",
			"download",
			"cache",
		},
		proto: []string{
			"id BIGINT NOT NULL AUTO_INCREMENT",
			"`time` BIGINT NOT NULL",
			"`job_id` INT NOT NULL",
			"`pages` SMALLINT NOT NULL",
			"`records` INT NOT NULL",
			"`affected` INT NOT NULL",
			"`removed` INT NOT NULL",
			"`runtime` INT NOT NULL",
			"`download` INT NOT NULL",
			"`cache` INT NOT NULL",
		},
		tail: " ENGINE=InnoDB DEFAULT CHARSET=latin1;",
	}

	log(nil, "kjob timer started")
}
func kjobQueryInit() {
	var err error
	joblog, err = database.Prepare(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (?,?,?,?,?,?,?,?,?)", tables["job_log"].database, tables["job_log"].name, tables["job_log"].columnOrder()))
	if err != nil {
		log(nil, err)
		panic(err)
	}
}

func gokjobQueueTick() {
	for range kjobQueueTick.C {
		nowtime := ktime()
		kjobStackMutex.Lock()
		for itt := range kjobStack {
			if kjobStack[itt].running == false && kjobStack[itt].NextRun < nowtime {
				go kjobStack[itt].start()
			}
		}
		kjobStackMutex.Unlock()
	}
}

type kjob struct {

	// "read-only" variables (not mutex'd, only written by newKjob())
	id     int
	Method string `json:"method"` //Method: 'get', 'put', 'update', 'delete'
	Source string `json:"source"` //Entity: "region_id": "10000002", "character_id": "1120048880"
	Owner  string `json:"owner"`
	URL    string `json:"url"` //URL: concat of Spec and Endpoint, with entity processed
	CI     string `json:"ci"`  //CI: concat of endpoint:JSON.Marshall(entity)
	spec   specS
	Token  string `json:"token"` //evesso access_token
	RunTag int64  `json:"runTag"`

	NextRun   int64 `json:"nextRun"`
	Expires   int64 `json:"expires"`    //milliseconds since 1970, when cache miss will occur
	ExpiresIn int64 `json:"expires_in"` //milliseconds until expiration, at completion of first page (or head) pulled

	ins             strings.Builder // builder of records pending insert
	RecordsIns      int64           `json:"records_ins"`     // count of records pending insert
	BytesDownloaded int             `json:"bytesDownloaded"` //total bytes downloaded

	upd         strings.Builder // builder of records pending update
	RecordsUpd  int64           `json:"records_upd"` // count of records pending update
	BytesCached int             `json:"bytesCached"` //total bytes cached

	PullType       uint16 `json:"pullType"`
	Pages          uint16 `json:"pages"`          //Pages: 0, 1
	PagesProcessed uint16 `json:"pagesProcessed"` //count of completed pages (excluding heads)
	PagesQueued    uint16 `json:"pagesQueued"`    //cumlative count of pages queued

	Records      int64 `json:"records"`      //cumlative count of records
	AffectedRows int64 `json:"affectedRows"` //cumlative count of affected records
	RemovedRows  int64 `json:"removedRows"`  //cumlative count of removed records

	heart    *time.Timer   //heartbeat timer
	req      *http.Request //http request
	running  bool
	jobMutex debugOnlyMutex
	page     map[uint16]*kpage // hashmap of [pagenumber]*kpage for all child elements
	table    *table
	sqldata  map[uint64]uint64
}

func newKjob(id int, method string, specnum string, endpoint string, ciString string, pages uint16, table string) {
	tspec := getSpec(method, specnum, endpoint)
	if tspec.invalid {
		log(nil, "Invalid job received: SPEC invalid")
		return
	}
	var entity map[string]string
	json.Unmarshal([]byte(ciString), &entity)

	var sentity, owner string
	var ok bool
	if sentity, ok = entity["region_id"]; !ok {
		if sentity, ok = entity["structure_id"]; !ok {
			if sentity, ok = entity["character_id"]; !ok {
				log(nil, "Invalid job received: unable to resolve entity")
				return
			}
			owner = sentity
		} else {
			owner = "NULL"
		}
	} else {
		owner = "NULL"
	}

	tmp := kjob{
		id:       id,
		Method:   method,
		Source:   sentity,
		Owner:    owner,
		Token:    "none",
		Pages:    pages,
		PullType: pages,
		URL:      fmt.Sprintf("%s%s?datasource=tranquility", specnum, endpoint),
		heart:    time.NewTimer(30 * time.Second),
		CI:       fmt.Sprintf("%s|%s", endpoint, ciString),
		spec:     tspec,
		page:     make(map[uint16]*kpage),
		table:    tables[table]}
	for se, sed := range entity {
		tmp.URL = strings.Replace(tmp.URL, "{"+se+"}", sed, -1)
	}

	go func() {
		for range tmp.heart.C {
			tmp.beat()
		}
	}()
	kjobStackMutex.Lock()
	kjobStack[kjobQueueLen] = &tmp
	kjobQueueLen++
	kjobStackMutex.Unlock()
}

func (k *kjob) beat() {
	k.forceNextRun(30000)
	k.stopJob(true)
}

func (k *kjob) start() {
	k.jobMutex.Lock()
	k.heart.Reset(30 * time.Second)
	k.RunTag = ktime()
	k.running = true
	addMetric(k.CI)
	if err := k.table.handleStart(k); err != nil {
		log(k.CI, err)
		k.jobMutex.Unlock()
		k.forceNextRun(86400000)
		k.stopJob(true)
		return
	}
	k.jobMutex.Unlock()
	k.run()
}
func (k *kjob) stopJob(failed bool) {
	k.jobMutex.Lock()
	if failed {
		k.table.handleEndFail(k)
	} else {
		k.RemovedRows += k.table.handleEndGood(k)
		log(k.CI, fmt.Sprintf("Pages:%d/%d Records:%d Affected:%d Removed:%d nextRun:%dms runtime:%dms", k.PagesProcessed, k.Pages, k.Records, k.AffectedRows, k.RemovedRows, k.Expires-ktime(), getMetric(k.CI)))
		joblog.Exec(ktime(), k.id, k.PagesProcessed, k.Records, k.AffectedRows, k.RemovedRows, getMetric(k.CI), k.BytesDownloaded, k.BytesCached)

	}
	k.heart.Stop()
	for it := range k.page {
		k.page[it].destroy()
	}
	k.running = false
	k.Token = "none"
	k.Expires = 0
	k.Pages = k.PullType
	k.PagesProcessed = 0
	k.PagesQueued = 0
	k.Records = 0
	k.AffectedRows = 0
	k.RemovedRows = 0
	k.ins.Reset()
	k.upd.Reset()
	k.jobMutex.Unlock()
	//defer runtime.GC()
}
func (k *kjob) run() {
	//k.heart.Reset(30 * time.Second)
	if k.spec.security != "" && len(k.Token) < 5 {
		log(k.CI, "todo: get token.")
		return
	}
	if k.Pages == 0 {
		go k.requestHead()
		return
	}
}
func (k *kjob) queuePages() {
	k.jobMutex.Lock()
	//log("kjob.go:k.queuePages()", fmt.Sprintf("%s Queueing pages %d to %d", k.CI, k.PagesQueued+1, k.Pages))
	for i := k.PagesQueued + 1; i <= k.Pages; i++ {
		k.newPage(i, false)
	}
	k.PagesQueued = k.Pages
	k.jobMutex.Unlock()
}
func (k *kjob) requestHead() {
	var err error
	if backoff {
		time.Sleep(5 * time.Second)
		go k.requestHead()
		return
	}
	k.req, err = http.NewRequest("HEAD", c.EsiURL+k.URL, nil)
	if err != nil {
		log(k.CI, err)
		return
	}

	if k.spec.security != "" && len(k.Token) > 5 {
		k.req.Header.Add("Authorization", "Bearer "+k.Token)
	}
	resp, err := client.Do(k.req)
	if err != nil {
		log(k.CI, err)
		k.stopJob(true)
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
		k.updateExp(resp.Header["Expires"][0])

		var timepct float64
		if k.ExpiresIn > 0 {
			timepct = 100 * (float64(k.ExpiresIn) / float64(k.spec.cache*1000))
		} else {
			timepct = 0
		}
		if timepct < c.MinCachePct {
			log(k.CI, " expires too soon, recycling!")
			k.stopJob(true)
			return
		}

		if pgs, ok := resp.Header["X-Pages"]; ok {
			k.updatePageCount(pgs[0])
		}

	}
}

func (k *kjob) updateExp(expire string) {
	exp, err := time.Parse("Mon, 02 Jan 2006 15:04:05 MST", expire)
	if err == nil {
		k.jobMutex.Lock()
		k.Expires = int64(exp.UnixNano() / int64(time.Millisecond))
		k.ExpiresIn = k.Expires - ktime()
		k.NextRun = k.Expires + 1250
		k.jobMutex.Unlock()
	}
}

func (k *kjob) forceNextRun(exp int64) {
	log(k.CI, fmt.Sprintf("next run forced to %dms", exp))
	k.jobMutex.Lock()
	k.Expires = ktime() + exp
	k.ExpiresIn = exp
	k.NextRun = ktime() + exp
	k.jobMutex.Unlock()
}
func (k *kjob) updatePageCount(pages string) {
	pgss, _ := strconv.Atoi(pages)
	if k.Pages != uint16(pgss) {
		k.jobMutex.Lock()
		k.Pages = uint16(pgss)
		k.jobMutex.Unlock()
		if k.Pages != k.PagesQueued {
			k.queuePages()
		}
	}
}
func (k *kjob) processPage() {
	k.jobMutex.Lock()
	k.PagesProcessed++
	pagesFinished++
	//update last_seen on cached entries if
	//  15000+ cached ids ready, or
	//  non-zero cached ids ready, and this is the last page
	if (k.upd.Len() >= 1000000) || ((k.upd.Len() > 0) && k.PagesProcessed == k.Pages) {
		k.AffectedRows += k.table.handleWriteUpd(k)
		k.RecordsUpd = 0
		k.upd.Reset()
	}

	//insert records if:
	//  15000+ records ready, or
	//  non-zero records ready, and this is the last page
	if (k.ins.Len() >= 1000000) || ((k.ins.Len() > 0) && k.PagesProcessed == k.Pages) {
		k.AffectedRows += k.table.handleWriteIns(k)
		k.RecordsIns = 0
		k.ins.Reset()
	}

	k.jobMutex.Unlock()
	if k.PagesProcessed == k.Pages {
		k.stopJob(false)
	}
}
