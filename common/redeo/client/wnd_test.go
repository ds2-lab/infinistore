package client

import (
	"log"
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

type testDispatcher struct {
	ch     chan Request
	closed int32
}

func (t *testDispatcher) Close() {
	if !atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		return
	}

	close(t.ch)
}

var _ = Describe("Window", func() {
	It("should thread safe", func() {
		wnd := NewWindow()
		concurrency := 30

		dispatcher := &testDispatcher{ch: make(chan Request, concurrency)}
		var wg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				// defer GinkgoRecover()
				defer func() {
					wg.Done()
					dispatcher.Close()
				}()

				for req := range dispatcher.ch {
					wnd.AddRequest(req)
					// <-time.After(time.Duration(rand.Int63n(100)+1) * time.Millisecond)
					// wnd.AckRequest(req.Seq())
					rsp, err := wnd.AckRequest(req.Seq())
					Expect(err).To(BeNil())
					Expect(rsp).To(Equal(req))
				}
			}()
		}

		for i := 0; i < 10000; i++ {
			dispatcher.ch <- NewRequest()
		}
		dispatcher.Close()

		wg.Wait()
		Expect(wnd.Len()).To(Equal(0))
		log.Printf("buckets %d", (wnd.tail.seq-wnd.top.seq)/BucketSize+1)
	})
})
