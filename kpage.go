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
var kpageQueueMutex = sync.RWMutex{}

var errorRemain = 100
var errorResetTimer *time.Ticker

var backoff = false

var curInFlight = 0
var lastInFlight = 0
var inFlight = make(map[int]*kpage, c.MaxInFlight)
var inFlightMutex = sync.RWMutex{}
var pagesFired = 0
var lastFired = 0

var pagesFinished = 0
var lastFinished = 0

var bCached = 0
var bDownload = 0

type kpageQueueS struct {
	elements chan *kpage
	len      int
}
type kpage struct {
	job       *kjob
	page      uint16
	cip       string
	body      []byte
	req       *http.Request
	running   int64
	dead      bool
	pageMutex sync.Mutex
	Ins       strings.Builder
	InsIds    strings.Builder
	InsReady  bool
	etag      string
}

func (kpageQueueS *kpageQueueS) Push(element *kpage) {
	kpageQueueMutex.Lock()
	defer kpageQueueMutex.Unlock()
	select {
	case kpageQueueS.elements <- element:
		kpageQueueS.len++
	default:
		panic("Queue full")
	}
}
func (kpageQueueS *kpageQueueS) Pop() *kpage {
	kpageQueueMutex.Lock()
	defer kpageQueueMutex.Unlock()
	select {
	case e := <-kpageQueueS.elements:
		kpageQueueS.len--
		return e
	default:
		panic("Queue empty")
	}
}
func gokpageQueueTick(t time.Time) {
	kpageQueueMutex.Lock()
	kpageQueueLen := kpageQueue.len
	kpageQueueMutex.Unlock()

	for kpageQueueLen > 0 && (curInFlight < c.MaxInFlight) && !backoff {
		qitem := kpageQueue.Pop()
		if qitem.dead == false {
			err := -1
			inFlightMutex.Lock()
			for it := 0; it < c.MaxInFlight; it++ {
				if inFlight[it].dead {
					err = it
					break
				}
			}
			curInFlight++
			pagesFired++
			inFlight[err] = qitem
			inFlight[err].running = ktime()
			go inFlight[err].requestPage()
			inFlightMutex.Unlock()
		} else {
			// log("kpage.go:gokpageQueueTick()", fmt.Sprintf("Encountered dead cip in queue %s", qitem.cip))
		}
		kpageQueueMutex.Lock()
		kpageQueueLen = kpageQueue.len
		kpageQueueMutex.Unlock()
	}
}
func kpageQueueInit() {

	kpageQueue = &kpageQueueS{
		elements: make(chan *kpage, 8192),
	}
	kpageQueueTick = time.NewTicker(10 * time.Millisecond) //500ms
	go func() {
		for t := range kpageQueueTick.C {
			gokpageQueueTick(t)
		}
	}()
	for i := 0; i < c.MaxInFlight; i++ {
		inFlight[i] = &kpage{dead: true}
	}
	temp := time.NewTicker(1 * time.Second)
	go func() {
		for range temp.C {
			kpageQueueMutex.Lock()
			fff := kpageQueue.len
			kpageQueueMutex.Unlock()
			if fff > 0 || lastFinished != pagesFinished || lastFired != pagesFired || lastInFlight != curInFlight {
				timenow := ktime()
				lastFinished = pagesFinished
				lastFired = pagesFired
				lastInFlight = curInFlight
				entry := fmt.Sprintf("%12d/%12d Q:%6d Fired:%6d Done:%6d Hot(%3d of %3d) ", bCached, bDownload, fff, lastFired, lastFinished, lastInFlight, c.MaxInFlight)
				// fntry := ""
				gntry := ""
				// hntry := ""
				inFlightMutex.Lock()
				for it := 0; it < c.MaxInFlight; it++ {
					// fntry = fmt.Sprintf("%s%12s", fntry, inFlight[it].whatDo)
					// hntry = fmt.Sprintf("%s%11d ", hntry, inFlight[it].job.entity)
					if inFlight[it].dead {
						gntry = gntry + "**** "
					} else {
						gntry = fmt.Sprintf("%s%4d ", gntry, timenow-inFlight[it].running)
					}
				}
				inFlightMutex.Unlock()
				log("<QUEUE>", entry+gntry)
				// log("       ", fntry)
				// log("       ", gntry)
				// log("       ", hntry)
			}
		}
	}()

	log("kpage.go:kpageQueueInit()", "Timer Initialized!")
}
func (k *kjob) newPage(page uint16) {
	k.PagesQueued++
	k.page[page] = &kpage{
		job:  k,
		page: page,
		cip:  fmt.Sprintf("%s|%d", k.CI, page),
	}
	kpageQueue.Push(k.page[page])
}
func (k *kpage) destroy() {
	if k.running > 0 {
		curInFlight--
		k.running = 0
	}
	k.dead = true
}
func (k *kpage) requestPage() {
	defer k.destroy()
	if k.dead {
		return
	}
	addMetric(k.cip)
	if backoff {
		log("kpage.go:k.requestPage("+k.cip+") backoff", "backoff trigger")
		k.job.LockJob("kpage.go:194")
		k.job.stopJob(false)
		k.job.APIErrors++
		k.job.UnlockJob()
		return
	}
	etaghdr := getEtag(k.cip)

	req, err := http.NewRequest(strings.ToUpper(k.job.Method), c.EsiURL+k.job.URL+"&page="+strconv.Itoa(int(k.page)), nil)
	if err != nil {
		log("kpage.go:k.requestPage("+k.cip+") http.NewRequest", err)
		k.job.LockJob("kpage.go:205")
		k.job.stopJob(false)
		k.job.APIErrors++
		k.job.UnlockJob()
		return
	}
	k.req = req
	if len(etaghdr) > 0 {
		k.req.Header.Add("If-None-Match", etaghdr)
		//log("kpage.go:k.requestPage("+k.cip+") etaghdr", "Attach "+etaghdr)
	}
	if k.job.spec.security != "" && len(k.job.Token) > 5 {
		k.req.Header.Add("Authorization", "Bearer "+k.job.Token)
		//log("kpage.go:k.requestPage("+k.cip+") Security", "Attach "+k.job.Token)
	}
	resp, err := client.Do(k.req)
	if err != nil {
		log("kpage.go:k.requestPage("+k.cip+") client.Do", err)
		k.job.LockJob("kpage.go:224")
		k.job.PagesQueued--
		k.job.newPage(k.page)
		k.job.UnlockJob()
		return
	}

	defer resp.Body.Close()
	if k.dead {
		return
	}
	k.job.LockJob("kpage.go:236")
	k.job.APICalls++
	k.job.UnlockJob()
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

	//k.job.heart.Reset(30 * time.Second)

	if resp.StatusCode == 200 {
		var err error
		k.body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log("kpage.go:k.requestPage("+k.cip+") ioutil.ReadAll", err)
			k.job.LockJob("kpage.go:262")
			k.job.PagesQueued--
			k.job.newPage(k.page)
			k.job.UnlockJob()
		}
		if _, ok := resp.Header["Etag"]; ok {
			k.etag = resp.Header["Etag"][0]
		}
		bDownload += len(k.body)
		k.job.LockJob("kpage.go:272")
		k.job.BytesDownloaded += len(k.body)
		k.job.UnlockJob()
	} else if resp.StatusCode == 304 {
		if k.job.table.prune {
			ids, length := getEtagIds(k.cip)
			k.InsIds.WriteString(ids)
			if length == 0 || len(ids) == 0 {
				log("kpage.go:k.requestPage("+k.cip+") getEtagData", "No Data returned!")
				killEtag(k.cip)
				k.job.LockJob("kpage.go:280")
				k.job.PagesQueued--
				k.job.newPage(k.page)
				k.job.UnlockJob()
				return
			}
			foo := strings.Split(ids, ",")
			bCached += length
			k.job.LockJob("kpage.go:288")
			k.job.BytesCached += length
			k.job.IDLength += len(foo)
			k.job.APICache++
			k.job.UnlockJob()
		} else {
			k.job.LockJob("kpage.go:289")
			k.job.BytesCached++
			k.job.APICache++
			k.job.UnlockJob()
		}
	}

	if resp.StatusCode == 200 || resp.StatusCode == 304 {
		k.job.updateExp(resp.Header["Expires"][0])
		if pgs, ok := resp.Header["X-Pages"]; ok {
			pgss, err := strconv.Atoi(pgs[0])
			if err == nil {
				if k.job.Pages != uint16(pgss) {
					k.job.LockJob("kpage.go:302")
					k.job.Pages = uint16(pgss)
					k.job.UnlockJob()
					if k.job.Pages != k.job.PagesQueued {
						k.job.queuePages()
					}
				}
			}
		}

		if k.dead {
			return
		}

		if len(k.etag) > 0 {
			if k.writeData() {
				k.job.LockJob("kpage.go:319")
				k.job.APIErrors++
				k.job.PagesQueued--
				k.job.newPage(k.page)
				k.job.UnlockJob()
			}
			if !k.job.table.prune {
				k.InsIds.WriteString("1")
			}
			setEtag(k.cip, k.etag, k.InsIds.String(), len(k.body))
		} else {
			k.InsReady = true
		}

		go k.job.processPage()
	} else {
		log("kpage.go:k.requestPage("+k.cip+")", fmt.Sprintf("RCVD (%d) %s(%d of %d) %s&page=%d %db in %dms", resp.StatusCode, k.job.Method, k.page, k.job.Pages, k.job.URL, k.page, len(k.body), getMetric(k.cip)))

		k.job.LockJob("kpage.go:321")
		k.job.APIErrors++
		k.job.PagesQueued--
		k.job.newPage(k.page)
		k.job.UnlockJob()
	}
}

func (k *kpage) writeData() bool {
	//log("kpage.go:writeData("+k.cip+")", fmt.Sprintf("called with %db", len(k.body)))
	if err := k.job.table.transform(k.job.table, k); err != nil {
		return true
	}
	return false
}
