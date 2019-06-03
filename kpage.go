package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var kpageQueue *kpageQueueS
var kpageQueueLen int
var kpageQueueProcessed int
var kpageQueueTick time.Ticker

var errorRemain = 100
var errorResetTimer time.Ticker

var backoff = false

var maxInFlight = 120
var curInFlight = 0

type kpageQueueS struct {
	elements chan kpage
}

type kpage struct {
	job  *kjob
	page uint16
	cip  string
	body []byte
	req  *http.Request
}

func (kpageQueueS *kpageQueueS) Push(element *kpage) {
	select {
	case kpageQueueS.elements <- *element:
		kpageQueueLen++
	default:
		panic("Queue full")
	}
}

func (kpageQueueS *kpageQueueS) Pop() *kpage {
	select {
	case e := <-kpageQueueS.elements:
		kpageQueueLen--
		kpageQueueProcessed++
		return &e
	default:
		//panic("Queue empty")
	}
	return &kpage{}
	//return nil
}

func gokpageQueueTick(t time.Time) {

	if kpageQueueLen > 0 && curInFlight < maxInFlight && !backoff {
	Start:
		qitem := kpageQueue.Pop()
		curInFlight++
		go qitem.requestPage()
		//log("kpage.go:gokpageQueueTick()", fmt.Sprintf("Got page %s, %d remaining, %d processed", qitem.cip, kpageQueueLen, kpageQueueProcessed))
		if kpageQueueLen > 0 && curInFlight < maxInFlight && !backoff {
			goto Start
		}
		//fmt.Println(qitem)
	}
}

func kpageQueueInit() {
	kpageQueue = &kpageQueueS{
		elements: make(chan kpage, 8192),
	}
	kpageQueueTick := time.NewTicker(500 * time.Millisecond) //500ms
	go func() {
		for t := range kpageQueueTick.C {
			gokpageQueueTick(t)
		}
	}()
	log("kpage.go:kpageQueueInit()", "Timer Initialized!")
}

func (k *kjob) newPage(page uint16) {
	tmp := kpage{
		job:  k,
		page: page,
		cip:  fmt.Sprintf("%s|%d", k.CI, page)}
	kpageQueue.Push(&tmp)
	k.PagesQueued++
	//log("kpage.go:newPage()", "Queued page "+tmp.cip)
}
func curInFlightmm() {
	curInFlight--
}

func (k *kpage) requestPage() {
	defer curInFlightmm()
	addMetric(k.cip)
	if backoff {
		kpageQueue.Push(k)
		k.job.APIErrors++
		return
	}
	etaghdr := getEtag(k.cip)

	req, err := http.NewRequest(strings.ToUpper(k.job.Method), esiURL+k.job.URL+"&page="+strconv.Itoa(int(k.page)), nil)
	if err != nil {
		log("kpage.go:k.requestPage("+k.cip+") http.NewRequest", err)
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
	defer resp.Body.Close()

	k.job.heart.Reset(30 * time.Second)

	if resp.StatusCode == 200 {
		var err error
		k.body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log("kpage.go:k.requestPage("+k.cip+") ioutil.ReadAll", err)
			kpageQueue.Push(k)
			return
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
		//log("kpage.go:k.requestPage("+k.cip+")", fmt.Sprintf("RCVD (%d) %s(%d of %d) %s&page=%d %db in %dms", resp.StatusCode, k.job.Method, k.page, k.job.Pages, k.job.URL, k.page, len(k.body), getMetric(k.cip)))
		k.job.mutex.Lock()
		k.job.PagesProcessed++
		if k.job.PagesProcessed == k.job.Pages {
			k.job.stop()
		}
		k.job.mutex.Unlock()
		//todo: call kjobIsDone() ... and write it.
	} else {
		kpageQueue.Push(k)
		k.job.APIErrors++
	}
}
