//////////////////////////////////////////////////////////////////////////////////
// kpage.go - ESI Page Loading
//////////////////////////////////////////////////////////////////////////////////
//  kpageQueueS.Push(element):  Adds element to the FIFO queue
//  kpageQueueS.Pop():  Returns the oldest element from the queue
//  gokpageQueueTick(t):  Timer tick function
//  kpageQueueInit(): Timer/Queue Init (called once from main)
//  kjob.newPage(page): Queues page on behalf of kjob
//  curInFlightmm(): Silly Mechanics (defer cal for decrementing curInFlight)
//  kpage.requestPage(): Launches kpage request

package main

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var kpageQueue *kpageQueueS
var kpageQueueTick *time.Ticker

var errorRemain metric
var errorResetTimer *time.Timer
var backoffTimer *time.Timer

var backoff = false
var queueTicker *time.Timer

var curInFlight metric
var lastInFlight uint64
var inFlight = make(map[uint64]*kpage, c.MaxInFlight)
var inFlightMutex sync.Mutex

var pagesFired metric
var lastFired uint64

var pagesFinished metric
var lastFinished uint64

var bCached metric
var bDownload metric

type kpageQueueS struct {
	elements chan *kpage
	len      metric
}
type kpage struct {
	job       *kjob
	page      uint16
	cip       string
	body      []byte
	req       *http.Request
	resp      *http.Response
	running   int64
	dead      bool
	pageMutex sync.Mutex
	ids       strings.Builder
	ins       strings.Builder
	upd       strings.Builder
	recs      int64
	insrecs   int64
	updrecs   int64
	inscomma  string
	updcomma  string
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
				var err uint64
				inFlightMutex.Lock()
				for it := uint64(0); it < c.MaxInFlight; it++ {
					if inFlight[it].dead {
						err = it
						break
					}
				}
				curInFlight.Inc()
				pagesFired.Inc()
				inFlight[err] = qitem
				inFlight[err].running = ctime.UnixNano() / int64(time.Millisecond)
				go inFlight[err].requestPage()
				inFlightMutex.Unlock()
			}
		}
	}
}
func backoffReset() {
	for range backoffTimer.C {
		backoff = false
		log(nil, "backoff reset")
	}
}
func errorReset() {
	for range errorResetTimer.C {
		errorRemain.Set(100)
	}
}

//initialize kpage queue tickers
func kpageInit() {

	kpageQueue = &kpageQueueS{
		elements: make(chan *kpage, 16383),
	}

	kpageQueueTick = time.NewTicker(10 * time.Millisecond)
	go gokpageQueueTick()

	for i := uint64(0); i < c.MaxInFlight; i++ {
		inFlight[i] = &kpage{dead: true}
	}

	queueTicker = time.NewTimer(5 * time.Second)
	go queueLog()

	backoffTimer = time.NewTimer(time.Second * 60 * 60 * 24 * 365) // 1 year.
	go backoffReset()

	errorResetTimer = time.NewTimer(time.Second * 60 * 60 * 24 * 365) // 1 year.
	go errorReset()
	log(nil, "kpage tickers started")
}

func queueLog() {
	for range queueTicker.C {
		if !backoff && (kpageQueue.len.Get() > 0 || lastFinished != pagesFinished.Get() || lastFired != pagesFired.Get() || lastInFlight != curInFlight.Get()) {
			timenow := ktime()
			lastFinished = pagesFinished.Get()
			lastFired = pagesFired.Get()
			lastInFlight = curInFlight.Get()
			var b strings.Builder
			fmt.Fprintf(&b, "%12d/%12d Q:%6d Fired:%6d Done:%6d Hot(%2d/%d) ", bCached.Get(), bDownload.Get(), kpageQueue.len.Get(), lastFired, lastFinished, lastInFlight, c.MaxInFlight)
			inFlightMutex.Lock()
			for it := uint64(0); it < c.MaxInFlight; it++ {
				if inFlight[it].dead {
					b.WriteString("**** ")
				} else {
					fmt.Fprintf(&b, "%4d ", timenow-inFlight[it].running)
				}
			}
			inFlightMutex.Unlock()
			log("<QUEUE>", b.String())
		}
		// pctfull := uint8(100 * (float64(lastInFlight) / float64(c.MaxInFlight)))
		queueTicker.Reset(125 * time.Millisecond)

		// if pctfull > 90 {
		// 	queueTicker.Reset(125 * time.Millisecond)
		// } else if pctfull > 80 {
		// 	queueTicker.Reset(250 * time.Millisecond)
		// } else if pctfull > 70 {
		// 	queueTicker.Reset(500 * time.Millisecond)
		// } else if pctfull > 60 {
		// 	queueTicker.Reset(500 * time.Millisecond)
		// } else if pctfull > 50 {
		// 	queueTicker.Reset(1 * time.Second)
		// } else if pctfull > 40 {
		// 	queueTicker.Reset(1 * time.Second)
		// } else if pctfull > 30 {
		// 	queueTicker.Reset(2 * time.Second)
		// } else if pctfull > 20 {
		// 	queueTicker.Reset(2 * time.Second)
		// } else if pctfull > 10 {
		// 	queueTicker.Reset(4 * time.Second)
		// } else {
		// 	queueTicker.Reset(4 * time.Second)
		// }
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
	if k.running > 0 {
		curInFlight.Dec()
		k.running = 0
	}
	k.insrecs = 0
	k.updrecs = 0
	k.ins.Reset()
	k.ids.Reset()
	k.upd.Reset()
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

	k.req, err = http.NewRequest(strings.ToUpper(k.job.Method), c.EsiURL+k.job.URL+"&page="+strconv.Itoa(int(k.page)), nil)
	if err != nil {
		log(k.cip, err)
		k.job.forceNextRun(86400000)
		k.job.stopJob(true)
		return
	}
	k.req.Header.Add("Accept-Encoding", "gzip")
	etaghdr := getEtag(k.cip)
	if len(etaghdr) > 0 {
		k.req.Header.Add("If-None-Match", etaghdr)
	}
	if k.job.spec.security != "" {
		k.req.Header.Add("Authorization", "Bearer "+getAccessToken(k.job.TokenID, k.job.spec.security))
	}
	k.resp, err = client.Do(k.req)
	if err != nil {
		log(k.cip, err)
		k.job.newPage(k.page, true)
		return
	}

	defer k.resp.Body.Close()
	// ce := ""
	if a, ok := k.resp.Header["Content-Encoding"]; ok {
		// ce = a[0]
		if a[0] == "gzip" {
			re, err := gzip.NewReader(k.resp.Body)
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
			// log(k.cip, fmt.Sprintf("got Proto:%s Status:%s Content-Length:%d TransferEncoding:%v Uncompressed:%t Content-Encoding:%s len:%d", k.resp.Proto, k.resp.Status, k.resp.ContentLength, k.resp.TransferEncoding, k.resp.Uncompressed, ce, len(k.body)))

		}
	} else {
		k.body, err = ioutil.ReadAll(k.resp.Body)
		if err != nil {
			log(k.cip, err)
			k.job.newPage(k.page, true)
			return
		}
	}

	if k.dead {
		return
	}

	if k.resp.StatusCode == 200 {
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
		if k.updrecs > 0 {
			if k.job.RecordsUpd > 0 {
				k.job.updJob.WriteString(",")
			}
			k.job.updJob.WriteString(k.upd.String())
			k.job.RecordsUpd += k.updrecs
		}
		k.job.jobMutex.Unlock()
		k.pageMutex.Unlock()

		if etag, ok := k.resp.Header["Etag"]; ok {
			setEtag(k.cip, etag[0], k.ids.String(), len(k.body))
		}
		bDownload.Add(uint64(len(k.body)))
		k.job.BytesDownloaded.Add(uint64(len(k.body)))
	} else if k.resp.StatusCode == 304 {
		ids, length := getEtagIds(k.cip)
		if length == 0 {
			killEtag(k.cip)
			log(k.cip, "No Data returned!")
			k.job.newPage(k.page, true)
			return
		}
		k.ids.WriteString(ids)
		if k.ids.Len() > 0 {
			if err = k.job.table.handlePageCached(k); err != nil {
				killEtag(k.cip)
				log(k.cip, err)
				k.job.newPage(k.page, true)
				return
			}
		}
		bCached.Add(uint64(length))
		k.job.BytesCached.Add(uint64(length))
	}

	if k.resp.StatusCode == 200 || k.resp.StatusCode == 304 {
		if exp, ok := k.resp.Header["Expires"]; ok {
			k.job.updateExp(exp[0])
		}
		if pgs, ok := k.resp.Header["X-Pages"]; ok {
			k.job.updatePageCount(pgs[0])
		}

		k.job.jobMutex.Lock()
		k.job.Records += k.recs
		k.job.jobMutex.Unlock()
		if k.dead {
			return
		}

		k.job.processPage()
	} else {
		log(k.cip, fmt.Sprintf("RCVD (%d) %s(%d of %d) %s&page=%d %db", k.resp.StatusCode, k.job.Method, k.page, k.job.Pages, k.job.URL, k.page, len(k.body)))
		if processBackoff(k.resp.Header, k.job) {
			k.job.newPage(k.page, true)
		}
	}

}
func processBackoff(hdr http.Header, k *kjob) (enabled bool) {
	errorlimitremain, okerrorlimitremain := hdr["X-Esi-Error-Limit-Remain"]
	errorlimitreset, okerrorlimitreset := hdr["X-Esi-Error-Limit-Reset"]
	if okerrorlimitremain && okerrorlimitreset {
		k.SeqErrors++
		log(nil, fmt.Sprintf("Processing %s:%t/%s:%t Errors%d", errorlimitremain, okerrorlimitremain, errorlimitreset, okerrorlimitreset, k.SeqErrors))
		errt, _ := strconv.Atoi(errorlimitreset[0])
		errr, _ := strconv.Atoi(errorlimitremain[0])
		errorRemain.Set(uint64(errr))
		errorResetTimer.Reset(time.Duration(errt) * time.Second)
		if errr <= c.BackoffThreshold {
			backoff = true
			backoffTimer.Reset(c.BackoffSeconds * time.Second)
			log(nil, fmt.Sprintf("backing off for %d seconds", c.BackoffSeconds))
		}
		if k.SeqErrors >= c.ErrDisableThreshold {
			log(k.CI, "ERR_DISABLE")
			stmt := safePrepare(fmt.Sprintf("UPDATE `%s`.`%s` set err_disabled=1 WHERE id=?", c.Tables["jobs"].DB, c.Tables["jobs"].Name))
			stmt.Exec(k.id)
			k.stopJob(true)
			kjobStackMutex.Lock()
			delete(kjobStack, k.id)
			kjobStackMutex.Unlock()
			return false
		}
		k.heart.Reset(30 * time.Second)
		time.Sleep(5 * time.Second)

	}
	return true
}
