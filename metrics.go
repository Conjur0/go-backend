// Metrics
package main

import (
	"sync"
)

//globals
var (
	metrics      = make(map[string]int64)
	metricsMutex = sync.RWMutex{}
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
