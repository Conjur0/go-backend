// Metrics
package main

import "sync"

var (
	metrics      = make(map[string]int64)
	metricsMutex sync.Mutex
)

//  addMetric(obj): adds a tag at the current time
func addMetric(obj string) {
	metricsMutex.Lock()
	metrics[obj] = ktime()
	metricsMutex.Unlock()
}

//  getMetric(obj): returns the number of milliseconds since addMetric(obj) was called, or -1 if it was never called
func getMetric(obj string) int {
	metricsMutex.Lock()
	v, ok := metrics[obj]
	metricsMutex.Unlock()
	if ok {
		return int(ktime() - v)
	}
	return -1
}

type metricu struct {
	sync.Mutex
	_val uint64
}

func (metricu *metricu) Get() uint64 {
	metricu.Lock()
	defer metricu.Unlock()
	return metricu._val
}
func (metricu *metricu) Set(val uint64) {
	metricu.Lock()
	metricu._val = val
	metricu.Unlock()
}
func (metricu *metricu) Add(val uint64) {
	metricu.Lock()
	metricu._val += val
	metricu.Unlock()
}
func (metricu *metricu) Sub(val uint64) {
	metricu.Lock()
	metricu._val -= val
	metricu.Unlock()
}
func (metricu *metricu) Reset() {
	metricu.Lock()
	metricu._val = 0
	metricu.Unlock()
}
func (metricu *metricu) Inc() {
	metricu.Lock()
	metricu._val++
	metricu.Unlock()
}
func (metricu *metricu) Dec() {
	metricu.Lock()
	metricu._val--
	metricu.Unlock()
}

type metrict struct {
	sync.Mutex
	_val int64
}

func (metrict *metrict) Get() int64 {
	metrict.Lock()
	defer metrict.Unlock()
	return metrict._val
}
func (metrict *metrict) Set(val int64) {
	metrict.Lock()
	metrict._val = val
	metrict.Unlock()
}
