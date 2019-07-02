// ESI Page Loading

package main

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var kpageQueue *kpageQueueS
var kpageQueueTick *time.Ticker

var errorRemain metricu
var errorResetTimer *time.Timer
var backoffTimer *time.Timer

var backoff = false
var queueTicker *time.Ticker

var curInFlight metricu
var lastInFlight uint64
var inFlight = make(map[uint64]*kpage, c.MaxInFlight)
var inFlightMutex sync.Mutex

var pagesFinished metricu
var lastFinished uint64

var lastSQL uint64

var bCached metricu
var bDownload metricu

type kpageQueueS struct {
	elements chan *kpage
	len      metricu
}
type kpage struct {
	job       *kjob
	page      uint16
	cip       string
	body      []byte
	running   metrict
	dead      bool
	pageMutex sync.Mutex
	ids       strings.Builder
	ins       strings.Builder
	recs      int64
	insrecs   int64
	inscomma  string
	idscomma  string
}

func (kpageQueueS *kpageQueueS) Push(element *kpage) {
	select {
	case kpageQueueS.elements <- element:
		kpageQueueS.len.Inc()
	default:
		panic("Queue full")
	}
}
func (kpageQueueS *kpageQueueS) Pop() *kpage {
	select {
	case e := <-kpageQueueS.elements:
		kpageQueueS.len.Dec()
		return e
	default:
		panic("Queue empty")
	}
}
func gokpageQueueTick() {
	for ctime := range kpageQueueTick.C {
		for !backoff && kpageQueue.len.Get() > 0 && (curInFlight.Get() < c.MaxInFlight) {
			qitem := kpageQueue.Pop()
			if qitem.dead == false {
				var availableSlot uint64
				inFlightMutex.Lock()
				for it := uint64(0); it < c.MaxInFlight; it++ {
					if inFlight[it].dead {
						availableSlot = it
						break
					}
				}
				inFlight[availableSlot] = qitem
				inFlight[availableSlot].running.Lock()
				curInFlight.Inc()
				inFlight[availableSlot].running._val = ctime.UnixNano() / int64(time.Millisecond)
				go inFlight[availableSlot].requestPage()
				inFlight[availableSlot].running.Unlock()
				inFlightMutex.Unlock()
			}
		}
	}
}
func backoffReset() {
	for range backoffTimer.C {
		backoff = false
		log("backoff reset")
	}
}
func errorReset() {
	for range errorResetTimer.C {
		errorRemain.Set(100)
	}
}

// initialize kpage queue tickers
func kpageInit() {

	kpageQueue = &kpageQueueS{
		elements: make(chan *kpage, 16383),
	}

	kpageQueueTick = time.NewTicker(10 * time.Millisecond)
	go gokpageQueueTick()

	for i := uint64(0); i < c.MaxInFlight; i++ {
		inFlight[i] = &kpage{dead: true}
	}

	queueTicker = time.NewTicker(2 * time.Second)
	go queueLog()

	backoffTimer = time.NewTimer(time.Second * 60 * 60 * 24 * 365) // 1 year.
	go backoffReset()

	errorResetTimer = time.NewTimer(time.Second * 60 * 60 * 24 * 365) // 1 year.
	go errorReset()
	log("kpage tickers started")
}

func queueLog() {
	for range queueTicker.C {
		if !backoff && (kpageQueue.len.Get() > 0 || lastFinished != pagesFinished.Get() || lastInFlight != curInFlight.Get()) {
			timenow := ktime()
			lastFinished = pagesFinished.Get()
			lastInFlight = curInFlight.Get()
			var b strings.Builder
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Fprintf(&b, "<Q> A:%7s OS:%7s Cache:%7s DL:%7s Q:%5d Done:%6s Hot(%2d)", byt(m.Alloc), byt(m.Sys), byt(bCached.Get()), byt(bDownload.Get()), kpageQueue.len.Get(), bytn(lastFinished), lastInFlight)
			inFlightMutex.Lock()
			for it := uint64(0); it < c.MaxInFlight; it++ {
				if inFlight[it].dead {
					b.WriteString(" ****")
				} else {
					fmt.Fprintf(&b, " %4d", timenow-inFlight[it].running.Get())
				}
			}
			inFlightMutex.Unlock()
			logq(b.String())
		}
	}
}

func (k *kjob) newPage(page uint16, requeue bool) {
	if requeue {
		k.jobMutex.Lock()
		defer k.jobMutex.Unlock()
	}
	k.page[page] = &kpage{
		job:  k,
		page: page,
		cip:  fmt.Sprintf("%s|%d", k.CI, page),
	}
	kpageQueue.Push(k.page[page])
}

func (k *kpage) destroy() {
	k.running.Lock()
	if k.running._val > 0 {
		k.running._val = 0
		curInFlight.Dec()
	}
	k.running.Unlock()
	k.insrecs = 0
	k.ins.Reset()
	k.ids.Reset()
	k.dead = true
}
func (k *kpage) requestPage() {
	var err error
	defer k.destroy()
	if k.dead {
		return
	}

	if backoff {
		log(k.cip, "backoff trigger")
		k.job.stopJob(true)
		return
	}

	req, err := http.NewRequest(strings.ToUpper(k.job.Method), c.EsiURL+k.job.URL+"&page="+strconv.Itoa(int(k.page)), nil)
	if err != nil {
		log(k.cip, err)
		k.job.forceNextRun(86400000)
		k.job.stopJob(true)
		return
	}
	req.Header.Add("Accept-Encoding", "gzip")
	etaghdr := getEtag(k.cip)
	if len(etaghdr) > 0 {
		req.Header.Add("If-None-Match", etaghdr)
	}
	if k.job.spec.security != "" {
		tok := getAccessToken(k.job.TokenID, k.job.spec.security)
		if len(tok) == 0 {
			k.job.errDisable()
			return
		}
		req.Header.Add("Authorization", "Bearer "+tok)
	}
	resp, err := client.Do(req)
	if err != nil {
		log(k.cip, err)
		k.job.newPage(k.page, true)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		if a, ok := resp.Header["Content-Encoding"]; ok {
			if a[0] == "gzip" {
				re, err := gzip.NewReader(resp.Body)

				if err != nil {
					log(k.cip, err)
					k.job.newPage(k.page, true)
					return
				}
				k.body, err = ioutil.ReadAll(re)
				if err != nil {
					log(k.cip, err)
					k.job.newPage(k.page, true)
					return
				}
			}
		} else {
			k.body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				log(k.cip, err)
				k.job.newPage(k.page, true)
				return
			}
		}
		bDownload.Add(uint64(len(k.body)))
		k.job.BytesDownloaded.Add(uint64(len(k.body)))
		if k.dead {
			return
		}
		var err error
		if err = k.job.table.handlePageData(k); err != nil {
			log(k.cip, err)
			k.job.newPage(k.page, true)
			return
		}
		k.job.jobMutex.Lock()
		k.pageMutex.Lock()
		if k.insrecs > 0 {
			if k.job.RecordsIns > 0 {
				k.job.insJob.WriteString(",")
			}
			k.job.insJob.WriteString(k.ins.String())
			k.job.RecordsIns += k.insrecs
		}
		k.job.jobMutex.Unlock()
		k.pageMutex.Unlock()

		if etag, ok := resp.Header["Etag"]; ok {
			setEtag(k.cip, etag[0], k.ids.String(), len(k.body))
		}
	} else if resp.StatusCode == 304 {
		ids, length := getEtagIds(k.cip)
		if length == 0 {
			killEtag(k.cip)
			log(k.cip, "No Data returned!")
			k.job.newPage(k.page, true)
			return
		}
		if len(ids) > 0 {
			k.pageMutex.Lock()
			k.job.jobMutex.Lock()
			ids := strings.Split(ids, ",")
			k.recs = int64(len(ids))
			var id int
			for it := range ids {
				id, _ = strconv.Atoi(ids[it])
				if _, ok := k.job.sqldata[uint64(id)]; ok {
					delete(k.job.sqldata, uint64(id)) //remove matched items from the map
				} else {
					killEtag(k.cip)
					log(k.cip, "etag data does not match table %d not found in table")
					k.pageMutex.Unlock()
					k.job.jobMutex.Unlock()
					k.job.newPage(k.page, true)
					return
				}
			}
			k.pageMutex.Unlock()
			k.job.jobMutex.Unlock()
		}
		bCached.Add(uint64(length))
		k.job.BytesCached.Add(uint64(length))
	}

	if resp.StatusCode == 200 || resp.StatusCode == 304 {
		if exp, ok := resp.Header["Expires"]; ok {
			k.job.updateExp(exp[0])
		}
		if pgs, ok := resp.Header["X-Pages"]; ok {
			k.job.updatePageCount(pgs[0])
		}
		k.job.jobMutex.Lock()
		k.job.Records += k.recs
		k.job.jobMutex.Unlock()
		if k.dead {
			return
		}
		go k.job.processPage()
	} else {
		log(k.cip, fmt.Sprintf("RCVD (%d) %s(%d of %d) %s&page=%d %db", resp.StatusCode, k.job.Method, k.page, k.job.Pages, k.job.URL, k.page, len(k.body)))
		if processBackoff(resp.Header, k.job) {
			k.job.newPage(k.page, true)
		}
	}

}
func processBackoff(hdr http.Header, k *kjob) (enabled bool) {
	errorlimitremain, okerrorlimitremain := hdr["X-Esi-Error-Limit-Remain"]
	errorlimitreset, okerrorlimitreset := hdr["X-Esi-Error-Limit-Reset"]
	if okerrorlimitremain && okerrorlimitreset {
		k.SeqErrors++
		logf("Processing %s:%t/%s:%t Errors:%d", errorlimitremain, okerrorlimitremain, errorlimitreset, okerrorlimitreset, k.SeqErrors)
		errt, _ := strconv.Atoi(errorlimitreset[0])
		errr, _ := strconv.Atoi(errorlimitremain[0])
		errorRemain.Set(uint64(errr))
		errorResetTimer.Reset(time.Duration(errt) * time.Second)
		if errr <= c.BackoffThreshold {
			backoff = true
			backoffTimer.Reset(c.BackoffSeconds * time.Second)
			logf("backing off for %d seconds", c.BackoffSeconds)
		}
		if k.SeqErrors >= c.ErrDisableThreshold {
			log(k.CI, "ERR_DISABLE")
			k.errDisable()
			return false
		}
		k.heart.Reset(40 * time.Second)
		time.Sleep(30 * time.Second)

	}
	return true
}
func (k *kjob) errDisable() {
	log(k.CI, "ERR_DISABLE")
	stmt := safePrepare(fmt.Sprintf("UPDATE `%s`.`%s` set err_disabled=1 WHERE id=?", c.Tables["jobs"].DB, c.Tables["jobs"].Name))
	stmt.Exec(k.id)
	stmt.Close()
	k.stopJob(true)
	kjobStackMutex.Lock()
	delete(kjobStack, k.id)
	kjobStackMutex.Unlock()
}
