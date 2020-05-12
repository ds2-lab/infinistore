package sync

import (
	"sync"
	"sync/atomic"
)

type WaitGroup struct {
	sync.WaitGroup
	mu sync.Mutex
	waiting uint32
	ch chan struct{}
	nonlazy bool
}

func (wg *WaitGroup) Add(delta int) {
	if delta > 0 {
		atomic.AddUint32(&wg.waiting, uint32(delta))
	}
	wg.WaitGroup.Add(delta)
	if delta < 0 {
		atomic.AddUint32(&wg.waiting, uint32(delta))
	}

	// wg.mu.Lock()
	// select {
	// case <-wg.getLocked():
	// 	wg.ch = make(chan struct{})
	// 	go wg.wait()
	// default:
	// }
	// wg.mu.Unlock()
}

func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

func (wg *WaitGroup) Wait() {
	if wg.IsWaiting() {
		wg.WaitGroup.Wait()
	}
}

func (wg *WaitGroup) IsWaiting() bool {
	return atomic.LoadUint32(&wg.waiting) > 0
}
