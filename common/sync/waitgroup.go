package sync

import (
	"sync"
	"sync/atomic"
)

type WaitGroup struct {
	sync.WaitGroup
	waiting int32
	mu      sync.Mutex
}

func (wg *WaitGroup) Add(delta int) {
	wg.mu.Lock()
	if delta > 0 && atomic.AddInt32(&wg.waiting, int32(delta)) > 0 {
		wg.WaitGroup.Add(delta)
	} else if delta < 0 && atomic.AddInt32(&wg.waiting, int32(delta)) >= 0 {
		wg.WaitGroup.Add(delta)
	}
	wg.mu.Unlock()
}

func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

// func (wg *WaitGroup) Wait() {
// 	if wg.IsWaiting() {
// 		wg.WaitGroup.Wait()
// 	}
// }

func (wg *WaitGroup) IsWaiting() bool {
	return atomic.LoadInt32(&wg.waiting) > 0
}

func (wg *WaitGroup) NumWaiting() int {
	return int(atomic.LoadInt32(&wg.waiting))
}

func (wg *WaitGroup) Reset() {
	if atomic.LoadInt32(&wg.waiting) < 0 {
		atomic.StoreInt32(&wg.waiting, int32(0))
	}
}
