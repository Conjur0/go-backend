package main

import (
	"testing"
	"time"
)

//useless test.... but a test.
func TestDebugOnlyMutex(t *testing.T) {
	t.Run("Lock", func(t *testing.T) {
		var test debugOnlyMutex
		if test.lock != 0 {
			t.Errorf("debugOnlyMutex.lock: got %d want 0", test.lock)
		}
		preruntime := time.Now().UnixNano()
		test.Lock()
		if test.lock != 1 {
			t.Errorf("debugOnlyMutex.lock: got %d want 1", test.lock)
		}
		if len(test.lockByFile) == 0 {
			t.Errorf("debugOnlyMutex.lockByFile: got %s want filename.go", test.lockByFile)
		}
		if len(test.lockByFunc) == 0 {
			t.Errorf("debugOnlyMutex.lockByFunc: got %s want funcName", test.lockByFunc)
		}
		if test.lockByLine == 0 {
			t.Errorf("debugOnlyMutex.lockByLine: got %d want line number", test.lockByLine)
		}
		if test.lockByTime < preruntime {
			t.Errorf("debugOnlyMutex.lockByTime: got %d want >= %d", test.lockByTime, preruntime)
		}

	})

}
