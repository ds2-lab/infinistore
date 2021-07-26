package client

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

func shouldNotTimeout(test func() interface{}, expects ...bool) interface{} {
	expect := false
	if len(expects) > 0 {
		expect = expects[0]
	}
	timer := time.NewTimer(time.Second)
	timeout := false
	responeded := make(chan interface{})
	var ret interface{}
	go func() {
		responeded <- test()
	}()
	select {
	case <-timer.C:
		timeout = true
	case ret = <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}
	Expect(timeout).To(Equal(expect))
	return ret
}

// func shouldTimeout(test func() interface{}) {
// 	shouldNotTimeout(test, true)
// }

var _ = Describe("Window", func() {
	It("should bucket reset work", func() {
		var testBucket = &ReqBucket{}
		testBucket.seq = 8
		testBucket.acked = 5
		testBucket.filled = 5
		testBucket.requests = make([]*RequestMeta, 8)
		testBucket.next = &ReqBucket{}
		testBucket.refs = 3

		testBucket.Reset()
		//fmt.Printf("%v",testBucket.refs)
		for i := 0; i < len(testBucket.requests); i++ {
			Expect(testBucket.requests[i]).To(BeNil())
		}
		Expect(testBucket.refs).To(Equal(int32(0)))
		Expect(testBucket.acked).To(Equal(int64(0)))
		Expect(testBucket.filled).To(Equal(int64(0)))
	})

	It("should Add request return nil", func() {
		wnd := NewWindow()
		defer wnd.Close()

		req := NewRequest()
		//err := wnd.AddRequest(req)
		err := shouldNotTimeout(func() interface{} {
			_, err := wnd.AddRequest(req)
			return err
		})
		Expect(err).To(BeNil())

		//wnd.Close()
		//err := wnd.AddRequest(req)
	})

	It("should report error after close", func() {
		wnd := NewWindow()
		req := NewRequest()
		wnd.Close()
		_, err := wnd.AddRequest(req)
		Expect(err).To(Not(BeNil()))
	})

	It("should seek deref bucket correctly", func() {
		wnd := NewWindow()
		defer wnd.Close()
		wnd.mu.RLock()
		defer wnd.mu.RUnlock()

		// Create bucket on demand.
		bucket, i, _ := wnd.seek(wnd.active.seq+BucketSize*SeqStep+SeqStep, wnd.active.Ref(), true)
		Expect(bucket).To(Not(Equal(wnd.active)))
		Expect(bucket).To(Equal(wnd.active.next))
		Expect(i).To(Equal(int64(1)))
		Expect(wnd.active.Refs()).To(Equal(0))
		Expect(bucket.Refs()).To(Equal(0))

		// Seek created bucket.
		bucket, i, _ = wnd.seek(wnd.active.seq+BucketSize*SeqStep+SeqStep, wnd.active.Ref(), false)
		Expect(bucket).To(Not(Equal(wnd.active)))
		Expect(bucket).To(Equal(wnd.active.next))
		Expect(i).To(Equal(int64(1)))
		Expect(wnd.active.Refs()).To(Equal(0))
		Expect(bucket.Refs()).To(Equal(0))

		// Seek nonexisted bucket.
		bucket, i, _ = wnd.seek(wnd.active.seq+2*BucketSize*SeqStep+SeqStep, wnd.active.Ref(), false)
		Expect(bucket).To(Equal(NilBucket))
		Expect(i).To(Equal(int64(BucketSize + 1)))
		Expect(wnd.active.Refs()).To(Equal(0))
		Expect(wnd.active.next.Refs()).To(Equal(0))
		Expect(bucket.Refs()).To(Equal(0))

		active := *wnd.active.next // copy one
		wnd.mu.RUnlock()
		wnd.Close()
		wnd.mu.RLock()
		var err error
		bucket, i, err = wnd.seek(active.seq+BucketSize*SeqStep+SeqStep, (&active).Ref(), true)
		Expect(bucket).To(Equal(NilBucket))
		Expect(i).To(Equal(int64(BucketSize + 1)))
		Expect(err).To(Equal(ErrConnectionClosed))
		Expect(active.Refs()).To(Equal(0))
		Expect(bucket.Refs()).To(Equal(0))
	})

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
					// 1/100 to have no response
					// if rand.Intn(100) != 1 {
					// <-time.After(time.Duration(rand.Int63n(100)+1) * time.Millisecond)
					// wnd.AckRequest(req.Seq())
					rsp, err := wnd.AckRequest(req.Seq())
					Expect(err).To(BeNil())
					Expect(rsp).To(Equal(req))
					// } else {
					// 	log.Printf("Omit response of %d", req.Seq())
					// }
				}
			}()
		}

		for i := 0; i < 10000; i++ {
			dispatcher.ch <- NewRequest()
		}
		dispatcher.Close()

		wg.Wait()
		Expect(wnd.Len()).To(Equal(0))
		log.Printf("buckets %d", (wnd.tail.seq-wnd.top.seq)/(BucketSize*SeqStep)+1)
	})
})
