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

type metric struct {
	_val uint64
	lock sync.Mutex
}

func (metric *metric) Get() uint64 {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	return metric._val
}
func (metric *metric) Set(val uint64) {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val = val
}
func (metric *metric) Add(val uint64) {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val += val
}
func (metric *metric) Sub(val uint64) {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val -= val
}
func (metric *metric) Reset() {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val = 0
}
func (metric *metric) Inc() {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val++
}
func (metric *metric) Dec() {
	metric.lock.Lock()
	defer metric.lock.Unlock()
	metric._val--
}
