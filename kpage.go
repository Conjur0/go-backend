package main

import (
	"fmt"
	"net/http"
	"time"
)

var kpageQueue *kpageQueueS
var kpageQueueLen int
var kpageQueueProcessed int
var kpageQueueTick time.Ticker

var errorRemain = 100
var errorResetTimer time.Ticker

var backoff = false

var maxInFlight = 2
var curInFlight = 0

type kpageQueueS struct {
	elements chan kpage
}

type kpage struct {
	job    *kjob
	page   uint16
	cip    string
	buffer string // http
	req    *http.Request
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

	if kpageQueueLen > 0 {
		//fmt.Print("Tick at", t.UnixNano(), " \n")
	Start:
		qitem := kpageQueue.Pop()
		//qitem.heart.Stop()
		log("kpage.go:gokpageQueueTick()", fmt.Sprintf("Got page %s, %d remaining, %d processed", qitem.cip, kpageQueueLen, kpageQueueProcessed))
		//qitem.print()
		if kpageQueueLen > 0 {
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
}

func (k *kjob) newPage(page uint16) {
	tmp := kpage{
		job:  k,
		page: page,
		cip:  fmt.Sprintf("%s|%d", k.CI, page)}

	kpageQueue.Push(&tmp)
	k.page = append(k.page, tmp)
	k.PagesQueued++
	log("kpage.go:newPage()", "Queued page "+tmp.cip)
}
