// ESI Job Management

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	kjobStack      = make(map[int]*kjob, 4096)
	kjobStackMutex sync.Mutex
	kjobQueueTick  *time.Ticker
)
var kjobsTicker *time.Ticker

var joblog *sql.Stmt
var jobrun *sql.Stmt
var jobadd *sql.Stmt
var jobget *sql.Stmt

type kjobQueueStruct struct {
	elements chan kjob
}

//  initialize kjob
func kjobInit() {
	kjobQueueTick = time.NewTicker(500 * time.Millisecond)
	go gokjobQueueTick()
	var err error
	joblog, err = database.Prepare(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", c.Tables["job_log"].DB, c.Tables["job_log"].Name, c.Tables["job_log"].columnOrder()))
	if err != nil {
		log(err)
		panic(err)
	}
	jobrun, err = database.Prepare(fmt.Sprintf("UPDATE `%s`.`%s` SET nextRun=? WHERE id=?", c.Tables["jobs"].DB, c.Tables["jobs"].Name))
	if err != nil {
		log(err)
		panic(err)
	}
	jobadd, err = database.Prepare(fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES(?,?,?,?,?,?,?)", c.Tables["jobs"].DB, c.Tables["jobs"].Name, c.Tables["jobs"].columnOrder()))
	if err != nil {
		log(err)
		panic(err)
	}
	jobget, err = database.Prepare(fmt.Sprintf("SELECT id,method,spec,endpoint,entity,pages,`table`,nextrun FROM `%s`.`%s` WHERE err_disabled=0 ORDER BY nextrun", c.Tables["jobs"].DB, c.Tables["jobs"].Name))
	if err != nil {
		log(err)
		panic(err)
	}

	kjobsTicker = time.NewTicker(time.Second * 5)
	go getJobsTick()

	log("kjob init complete")
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

func createJob(method string, specnum string, endpoint string, ciString string, pages uint16, table string, nextRun int64) int {
	res, err := jobadd.Exec(method, specnum, endpoint, ciString, pages, table, nextRun)
	if err != nil {
		log(err)
		return -1
	}
	aff, err := res.RowsAffected()
	if err != nil {
		log(err)
		return -1
	}
	if aff < 1 {
		log("no rows inserted")
		return -1
	}
	id, err := res.LastInsertId()
	if err != nil {
		log(err)
		return -1
	}
	newKjob(int(id), method, specnum, endpoint, ciString, pages, table, nextRun)
	return int(id)
}

func getJobs() {
	ress, err := jobget.Query()
	if err != nil {
		log(err)
		return
	}
	var (
		id                                         int
		method, specnum, endpoint, ciString, table string
		pages                                      uint16
		nextRun                                    int64
	)
	existingJobs := make(map[int]bool)
	for it := range kjobStack {
		existingJobs[it] = true //add all jobs to this map
	}
	var addedJobs int
	var iter int64
	stime := ktime()
	for ress.Next() {
		ress.Scan(&id, &method, &specnum, &endpoint, &ciString, &pages, &table, &nextRun)
		if _, ok := kjobStack[id]; ok {
			delete(existingJobs, id) //remove all jobs that were seen by sql
			continue
		}
		newKjob(id, method, specnum, endpoint, ciString, pages, table, max((iter*10)+stime, nextRun))
		addedJobs++
		iter++
	}
	var nukedJobs int
	for it := range existingJobs {
		if kjobStack[it].running {
			kjobStack[it].forceNextRun(1000 * 60 * 60 * 24 * 365) //temporarily set the nextrun for 1 year
			kjobStack[it].stopJob(true)
		}
		kjobStackMutex.Lock()
		delete(kjobStack, it)
		kjobStackMutex.Unlock()
		nukedJobs++
	}
	if addedJobs > 0 || nukedJobs > 0 {
		logf("Added %d, removed %d Jobs", addedJobs, nukedJobs)
	}
}

func getJobsTick() {
	for range kjobsTicker.C {
		getJobs()
	}
}

type kjob struct {
	id      int
	Method  string `json:"method"` //Method: 'get', 'put', 'update', 'delete'
	Source  string `json:"source"`
	Owner   string `json:"owner"`
	URL     string `json:"url"` //URL: concat of Spec and Endpoint, with entity processed
	CI      string `json:"ci"`
	spec    specS
	TokenID uint64 `json:"token_id"`
	RunTag  int64  `json:"runTag"`

	NextRun   int64 `json:"nextRun"`
	Expires   int64 `json:"expires"`
	ExpiresIn int64 `json:"expires_in"`

	SeqErrors int64 `json:"seq_errors"`

	insJob          strings.Builder // builder of records pending insert
	RecordsIns      int64           `json:"records_ins"`     // count of records pending insert
	BytesDownloaded metricu         `json:"bytesDownloaded"` //total bytes downloaded
	BytesCached     metricu         `json:"bytesCached"`     //total bytes cached

	PullType       uint16 `json:"pullType"`
	Pages          uint16 `json:"pages"`          //Pages: 0, 1
	PagesProcessed uint16 `json:"pagesProcessed"` //count of completed pages (excluding heads)
	PagesQueued    uint16 `json:"pagesQueued"`    //cumlative count of pages queued

	Records      int64 `json:"records"`      //cumlative count of records
	AffectedRows int64 `json:"affectedRows"` //cumlative count of affected records
	RemovedRows  int64 `json:"removedRows"`  //cumlative count of removed records

	records        metricu
	recordsStale   metricu
	recordsNew     metricu
	recordsChanged metricu

	heart      *time.Timer //heartbeat timer
	running    bool
	jobMutex   sync.Mutex
	page       map[uint16]*kpage // hashmap of [pagenumber]*kpage for all child elements
	table      *table
	sqldata    map[uint64]uint64
	allsqldata map[uint64]uint64
}

func newKjob(id int, method string, specnum string, endpoint string, ciString string, pages uint16, table string, nextRun int64) {
	tspec := getSpec(method, specnum, endpoint)
	if tspec.invalid {
		logf("Invalid job received: SPEC invalid (%s %s%s)", method, specnum, endpoint)
		return
	}
	if _, ok := c.Tables[table]; !ok {
		logf("Invalid job received: table \"%s\" invalid", table)
		return
	}
	var entity map[string]string
	json.Unmarshal([]byte(ciString), &entity)

	var sentity, owner, tokenid string
	var ok bool
	if sentity, ok = entity["region_id"]; !ok {
		if sentity, ok = entity["corporation_id"]; !ok {
			if sentity, ok = entity["structure_id"]; !ok {
				if sentity, ok = entity["character_id"]; !ok {
					logf("Invalid job received: unable to resolve entity (%s %s%s) %s", method, specnum, endpoint, ciString)
					sentity = "1"
					//return
				}
				owner = sentity
			} else {
				owner = "NULL"
			}
		} else {
			owner = "NULL"
		}
	} else {
		owner = "NULL"
	}

	tokenid, ok = entity["token_id"]
	if !ok {
		tokenid, ok = entity["character_id"]
		if !ok {
			tokenid = "0"
		}
	}
	tid, _ := strconv.Atoi(tokenid)
	tmp := kjob{
		id:       id,
		Method:   method,
		Source:   sentity,
		Owner:    owner,
		TokenID:  uint64(tid),
		Pages:    pages,
		PullType: pages,
		URL:      fmt.Sprintf("%s%s?datasource=tranquility", specnum, endpoint),
		heart:    time.NewTimer(30 * time.Second),
		CI:       fmt.Sprintf("%s|%s", endpoint, ciString),
		spec:     tspec,
		page:     make(map[uint16]*kpage),
		table:    c.Tables[table],
		NextRun:  nextRun,
	}
	for se, sed := range entity {
		tmp.URL = strings.Replace(tmp.URL, "{"+se+"}", sed, -1)
	}

	go func() {
		for range tmp.heart.C {
			tmp.beat()
		}
	}()
	kjobStackMutex.Lock()
	kjobStack[id] = &tmp
	kjobStackMutex.Unlock()
}

func (k *kjob) beat() {
	k.forceNextRun(30000)
	k.stopJob(true)
}

func (k *kjob) start() {
	k.jobMutex.Lock()
	k.heart.Reset(1800 * time.Second)
	k.RunTag = ktime()
	k.running = true
	addMetric(k.CI)
	k.table.getAllData(k)
	k.jobMutex.Unlock()
	k.run()
}
func (k *kjob) stopJob(failed bool) {
	k.jobMutex.Lock()
	for it := range k.page {
		k.page[it].destroy()
		delete(k.page, it)
	}
	if failed {
		k.sqldata = make(map[uint64]uint64)
	} else {
		k.SeqErrors = 0
		if len(k.sqldata) > 0 {
			var b strings.Builder
			comma := ""
			for it := range k.sqldata {
				fmt.Fprintf(&b, "%s%d", comma, it)
				delete(k.allsqldata, it)
				comma = ","
			}
			k.RemovedRows += safeExec(fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s=%s AND %s IN (%s)", k.table.DB, k.table.Name, k.table.JobKey, k.Source, k.table.Changed, b.String()))
		}
		k.sqldata = make(map[uint64]uint64)
		joblog.Exec(ktime(), k.id, k.PagesProcessed, k.Records, k.AffectedRows, k.RemovedRows,
			k.records.Get(),
			k.recordsNew.Get(),
			k.recordsChanged.Get(),
			k.recordsStale.Get(),
			getMetric(k.CI), k.BytesDownloaded.Get(), k.BytesCached.Get())
		jobrun.Exec(k.NextRun, k.id)
	}
	k.heart.Stop()
	k.Expires = 0
	k.Pages = k.PullType
	k.PagesProcessed = 0
	k.PagesQueued = 0
	k.Records = 0
	k.AffectedRows = 0
	k.RemovedRows = 0
	k.BytesCached.Reset()
	k.BytesDownloaded.Reset()
	k.insJob.Reset()
	k.RecordsIns = 0

	k.records.Reset()
	k.recordsChanged.Reset()
	k.recordsNew.Reset()
	k.recordsStale.Reset()

	k.running = false
	k.jobMutex.Unlock()
}
func (k *kjob) run() {
	if k.Pages == 0 {
		go k.requestHead()
		return
	}
	go k.queuePages()
}
func (k *kjob) queuePages() {
	k.jobMutex.Lock()
	for i := k.PagesQueued + 1; i <= k.Pages; i++ {
		k.newPage(i, false)
	}
	k.PagesQueued = k.Pages
	k.jobMutex.Unlock()
}
func (k *kjob) requestHead() {
	for backoff && k.running {
		time.Sleep(5 * time.Second)
	}
	if !k.running {
		return
	}
	req, err := http.NewRequest("HEAD", c.EsiURL+k.URL, nil)
	if err != nil {
		log(k.CI, err)
		return
	}

	if k.spec.security != "" {
		tok := getAccessToken(k.TokenID, k.spec.security)
		if len(tok) == 0 {
			k.errDisable()
			return
		}
		req.Header.Add("Authorization", "Bearer "+tok)
	}
	resp, err := client.Do(req)
	if err != nil {
		log(k.CI, err)
		k.stopJob(true)
		return
	}
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
			//log(k.CI, " expires too soon, recycling!")
			k.stopJob(true)
			return
		}
		if pgs, ok := resp.Header["X-Pages"]; ok {
			k.updatePageCount(pgs[0])
		} else {
			k.updatePageCount("1")
		}
	} else {
		log(k.CI, fmt.Sprintf("RCVD (%s) HEAD %s", resp.Status, k.URL))
		if processBackoff(resp.Header, k) {
			k.requestHead()
		}
	}
}

func (k *kjob) updateExp(expire string) {
	exp, err := time.Parse("Mon, 02 Jan 2006 15:04:05 MST", expire)
	if err == nil {
		k.jobMutex.Lock()
		k.Expires = int64(exp.UnixNano() / int64(time.Millisecond))
		k.ExpiresIn = k.Expires - ktime()
		k.NextRun = k.Expires
		k.jobMutex.Unlock()
	}
}

func (k *kjob) forceNextRun(exp int64) {
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
	pagesFinished.Inc()
	if (k.insJob.Len() >= 500000) || ((k.insJob.Len() > 0) && k.PagesProcessed == k.Pages) {
		k.AffectedRows += k.table.handleWriteIns(k)
		k.RecordsIns = 0
		k.insJob.Reset()
	}

	k.jobMutex.Unlock()
	if k.PagesProcessed == k.Pages {
		k.stopJob(false)
	}
}
