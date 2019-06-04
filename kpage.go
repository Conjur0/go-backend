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
var kpageQueueTick time.Ticker
var kpageQueueMutex = sync.RWMutex{}

var errorRemain = 100
var errorResetTimer time.Ticker

var backoff = false

var maxInFlight = 4
var curInFlight = 0
var inFlight = make(map[int]*kpage, maxInFlight)
var inFlightMutex = sync.RWMutex{}
var pagesFired = 0
var pagesFinished = 0

type kpageQueueS struct {
	elements chan *kpage
	len      int
}
type kpage struct {
	job     *kjob
	page    uint16
	cip     string
	body    []byte
	req     *http.Request
	running int64
	dead    bool
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

	if kpageQueue.len > 0 && curInFlight < maxInFlight && !backoff {
	Start:
		qitem := kpageQueue.Pop()
		if qitem.dead == false {
			err := -1
			inFlightMutex.Lock()
			for it := range inFlight {
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
		}
		//log("kpage.go:gokpageQueueTick()", fmt.Sprintf("Got page %s, %d remaining", qitem.cip, kpageQueueS.len))
		if kpageQueue.len > 0 && curInFlight < maxInFlight && !backoff {
			goto Start
		}
		//fmt.Println(qitem)
	}
}
func kpageQueueInit() {
	kpageQueue = &kpageQueueS{
		elements: make(chan *kpage, 8192),
	}
	kpageQueueTick := time.NewTicker(10 * time.Millisecond) //500ms
	go func() {
		for t := range kpageQueueTick.C {
			gokpageQueueTick(t)
		}
	}()
	for i := 0; i < maxInFlight; i++ {
		inFlight[i] = &kpage{dead: true}
	}
	temp := time.NewTicker(1 * time.Second)
	go func() {
		for range temp.C {
			kpageQueueMutex.Lock()
			fff := kpageQueue.len
			kpageQueueMutex.Unlock()
			if fff > 0 {
				timenow := ktime()
				entry := fmt.Sprintf("Queue:%6d Fired:%6d Finished:%6d Hot: %3d of %3d  ", fff, pagesFired, pagesFinished, curInFlight, maxInFlight)
				inFlightMutex.Lock()
				for it := range inFlight {
					if inFlight[it].dead {
						entry = entry + "******* "
					} else {
						entry = entry + fmt.Sprintf("%7d ", timenow-inFlight[it].running)
					}
				}
				inFlightMutex.Unlock()
				log("kpage.go:kpageQueueInit() seat Stats", entry)
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
		cip:  fmt.Sprintf("%s|%d", k.CI, page)}
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
		k.job.mutex.Lock()
		kjobQueueFinished--
		k.job.stop(false)
		k.job.mutex.Unlock()
		k.job.APIErrors++
		return
	}
	etaghdr := getEtag(k.cip)

	req, err := http.NewRequest(strings.ToUpper(k.job.Method), esiURL+k.job.URL+"&page="+strconv.Itoa(int(k.page)), nil)
	if err != nil {
		log("kpage.go:k.requestPage("+k.cip+") http.NewRequest", err)
		k.job.mutex.Lock()
		kjobQueueFinished--
		k.job.stop(false)
		k.job.mutex.Unlock()
		k.job.APIErrors++
		return
	}
	k.req = req
	if len(etaghdr) > 0 {
		k.req.Header.Add("If-None-Match", etaghdr)
		//log("kpage.go:k.requestPage("+k.cip+") etaghdr", "Attach "+etaghdr)
	}
	if k.job.Security != "none" && len(k.job.Token) > 5 {
		k.req.Header.Add("Authorization", "Bearer "+k.job.Token)
		//log("kpage.go:k.requestPage("+k.cip+") Security", "Attach "+k.job.Token)
	}
	resp, err := client.Do(k.req)
	if err != nil {
		log("kpage.go:k.requestPage("+k.cip+") client.Do", err)
		k.job.PagesQueued--
		k.job.newPage(k.page)
		return
	}
	defer resp.Body.Close()
	if k.dead {
		return
	}
	k.job.APICalls++
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
			k.job.PagesQueued--
			k.job.newPage(k.page)
		}
		if _, ok := resp.Header["Etag"]; ok {
			go setEtag(k.cip, resp.Header["Etag"][0], k.body)
		}
		k.job.BytesDownloaded += len(k.body)
	} else if resp.StatusCode == 304 {
		k.body = getEtagData(k.cip)
		k.job.BytesCached += len(k.body)
		k.job.APICache++
	}

	if resp.StatusCode == 200 || resp.StatusCode == 304 {
		if k.job.Expires == 0 {
			k.job.updateExp(resp.Header["Expires"][0])
		}
		if pgs, ok := resp.Header["X-Pages"]; ok {
			pgss, err := strconv.Atoi(pgs[0])
			if err == nil {
				if k.job.Pages != uint16(pgss) {
					k.job.Pages = uint16(pgss)
					if k.job.Pages != k.job.PagesQueued {
						k.job.queuePages()
					}
				}
			}

		}

		if k.dead {
			return
		}
		k.job.mutex.Lock()
		k.job.PagesProcessed++
		if k.job.PagesProcessed == k.job.Pages {
			k.job.stop(false)
		}
		k.job.mutex.Unlock()
		pagesFinished++
	} else {
		log("kpage.go:k.requestPage("+k.cip+")", fmt.Sprintf("RCVD (%d) %s(%d of %d) %s&page=%d %db in %dms", resp.StatusCode, k.job.Method, k.page, k.job.Pages, k.job.URL, k.page, len(k.body), getMetric(k.cip)))

		k.job.PagesQueued--
		k.job.newPage(k.page)
		k.job.APIErrors++
	}
}
