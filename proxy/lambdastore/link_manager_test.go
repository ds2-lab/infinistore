package lambdastore

import (
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var (
	errRequestSent = errors.New("request sent")
)

type testLink struct {
}

func (l *testLink) SendRequest(_ *types.Request, _ ...interface{}) error {
	return errRequestSent
}

func (l *testLink) Close() error {
	return nil
}

type productiveLink struct {
	alinks *AvailableLinks
}

func (l *productiveLink) SendRequest(_ *types.Request, _ ...interface{}) error {
	l.alinks.AddAvailable(l, false)
	l.alinks.AddAvailable(&productiveLink{alinks: l.alinks}, false)
	return nil
}

func (l *productiveLink) Close() error {
	return nil
}

func (l *AvailableLinks) count() (int, int) {
	total := len(l.bottom.links)
	buckets := 1
	for b := l.top; b != l.bottom; b = b.next {
		total += len(b.links)
		buckets++
	}
	for elem := l.linkRequests.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.(*AvailableLink).link != nil {
			total += 1
		}
	}
	for b := l.bottom.next; b != nil; b = b.next {
		buckets++
	}
	return total, buckets
}

var _ = Describe("AvailableLinks", func() {
	It("should list expands correctly", func() {
		list := newAvailableLinks()
		list.SetLimit(UnlimitedActiveLinks)
		Expect(list.Len()).To(Equal(0))

		for i := 0; i < LinkBucketSize*2; i++ {
			list.AddAvailable(&testLink{}, false)
		}
		Expect(list.Len()).To(Equal(LinkBucketSize * 2))

		list.Reset()
		total, buckets := list.count()
		Expect(list.Len()).To(Equal(0))
		Expect(total).To(Equal(0))
		Expect(buckets).To(Equal(2)) // This test covered buckets are recycled.
	})

	It("should pipe wait and consume the link", func() {
		list := newAvailableLinks()

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer GinkgoRecover()

			al := list.GetRequestPipe()
			al.Request() <- &types.Request{}
			Expect(al.Error()).To(Equal(errRequestSent))
			Expect(al.link).To(Not(BeNil()))
			Expect(list.linkRequests.Len()).To(Equal(0))
			wg.Done()
		}()

		shouldTimeout(func() {
			wg.Wait()
		}, true)

		list.AddAvailable(&testLink{}, false)
		shouldTimeout(func() {
			wg.Wait()
		}, false)
		Expect(list.Len()).To(Equal(0))
	})

	It("should pipe wait for link and terminate on reset", func() {
		list := newAvailableLinks()

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer GinkgoRecover()

			al := list.GetRequestPipe()
			select {
			case al.Request() <- &types.Request{}:
			case <-al.Closed():
			}
			Expect(al.link).To(BeNil())
			Expect(al.err).To(Equal(ErrLinkManagerReset))
			Expect(list.linkRequests.Len()).To(Equal(0))
			wg.Done()
		}()

		shouldTimeout(func() {
			wg.Wait()
		}, true)

		list.Reset()
		shouldTimeout(func() {
			wg.Wait()
		}, false)
		Expect(list.Len()).To(Equal(0))
	})

	It("should pipe wait for request and terminate on reset", func() {
		list := newAvailableLinks()
		al := list.GetRequestPipe()

		link := &testLink{}
		list.AddAvailable(link, false)
		<-time.After(100 * time.Millisecond) // Wait for a while
		Expect(list.Len()).To(Equal(1))
		expectedLink, _ := al.link.(*testLink)
		Expect(expectedLink).To(Equal(link))
		Expect(list.linkRequests.Len()).To(Equal(1))
		Expect(list.linkRequests.Front().Value.(*AvailableLink)).To(Equal(al))

		list.Reset()
		Expect(list.Len()).To(Equal(0))
		runtime.Gosched()
		Expect(list.Len()).To(Equal(0))
		Expect(al.link).To(BeNil())
		Expect(al.err).To(Equal(ErrLinkManagerReset))
		Expect(list.linkRequests.Len()).To(Equal(0))
	})

	It("should limit links", func() {
		list := newAvailableLinks()
		list.SetLimit(1)

		Expect(list.AddAvailable(&testLink{}, false)).To(BeTrue())
		Expect(list.Len()).To(Equal(1))

		Expect(list.AddAvailable(&testLink{}, false)).To(BeFalse())
		Expect(list.Len()).To(Equal(1))

		al := list.GetRequestPipe()
		runtime.Gosched()
		Expect(list.Len()).To(Equal(1))
		Expect(list.AddAvailable(&testLink{}, false)).To(BeFalse())
		Expect(list.Len()).To(Equal(1))

		al.Request() <- &types.Request{}
		<-al.Closed() // Wait until closed
		Expect(list.Len()).To(Equal(0))

		Expect(list.AddAvailable(al.link, false)).To(BeTrue())
		Expect(list.Len()).To(Equal(1))

		list.Reset()
		Expect(list.Len()).To(Equal(0))
	})

	It("should no limit works", func() {
		list := newAvailableLinks()
		list.SetLimit(1)

		Expect(list.AddAvailable(&testLink{}, false)).To(BeTrue())
		Expect(list.Len()).To(Equal(1))

		Expect(list.AddAvailable(&testLink{}, true)).To(BeTrue())
		Expect(list.Len()).To(Equal(2))

		al := list.GetRequestPipe()
		runtime.Gosched()
		Expect(list.Len()).To(Equal(2))

		al.Request() <- &types.Request{}
		<-al.Closed() // Wait until closed
		Expect(list.Len()).To(Equal(1))

		Expect(list.AddAvailable(al.link, false)).To(BeFalse())
		Expect(list.Len()).To(Equal(1))

		list.Reset()
		Expect(list.Len()).To(Equal(0))
	})

	It("should get request err successfully", func() {
		list := newAvailableLinks()

		list.AddAvailable(&testLink{}, false)

		al := list.GetRequestPipe()
		runtime.Gosched()
		select {
		case al.Request() <- &types.Request{}:
			Expect(al.Error()).To(Equal(errRequestSent))
		case <-al.Closed():
			Fail("on successful request, select should not pick Closed()")
		default:
			Fail("should not block")
		}
	})

	It("should multi-source request return same result", func() {
		list := newAvailableLinks()

		list.AddAvailable(&testLink{}, false)

		al := list.GetRequestPipe()
		runtime.Gosched()
		select {
		case al.Request() <- &types.Request{}:
			Expect(al.Error()).To(Equal(errRequestSent))
		case <-al.Closed():
			Fail("on successful request, select should not pick Closed()")
		default:
			Fail("should not block")
		}

		select {
		case al.Request() <- &types.Request{}:
			Fail("for second request, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(errRequestSent))
		default:
			Fail("should not block")
		}

		// Test reset cases
		al = list.GetRequestPipe()
		list.AddAvailable(&testLink{}, false)
		list.Reset()
		select {
		case al.Request() <- &types.Request{}:
			Fail("for request after closed, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrLinkManagerReset))
		default:
			Fail("should not block")
		}
	})

	It("should AddAvailable thread safe", func() {
		list := newAvailableLinks()
		list.SetLimit(UnlimitedActiveLinks)

		// Prefill some
		expected := 0
		for i := 0; i < LinkBucketSize/2; i++ {
			list.AddAvailable(&testLink{}, false)
			expected++
		}
		producer := make(chan *testLink, LinkBucketSize*2)

		// Start multiple consumers
		var wg sync.WaitGroup
		wg.Add(LinkBucketSize)
		for i := 0; i < LinkBucketSize; i++ {
			go func() {
				for link := range producer {
					list.AddAvailable(link, false)
				}
				wg.Done()
			}()
		}

		// Produce number of 2 buckets
		for i := 0; i < LinkBucketSize*2; i++ {
			producer <- &testLink{}
			expected++
		}
		close(producer)

		// Wait for processing
		shouldTimeout(func() {
			wg.Wait()
		}, func(timeout bool) {
			Expect(timeout).To(BeFalse())
		})

		total, buckets := list.count()
		Expect(list.Len()).To(Equal(expected))
		Expect(total).To(Equal(expected))
		Expect(buckets).To(Equal(3))

		list.Reset()
		total, buckets = list.count()
		Expect(list.Len()).To(Equal(0))
		Expect(total).To(Equal(0))
		Expect(buckets).To(Equal(3))
	})

	It("should basic timeout works", func() {
		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		select {
		case al.Request() <- &types.Request{}:
			Fail("for second request, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrLinkRequestTimeout))
		default:
			Fail("should not block")
		}
	})

	It("should timeout compatible with reset", func() {
		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)
		list.Reset()
		select {
		case al.Request() <- &types.Request{}:
			Fail("for second request, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrLinkManagerReset))
		default:
			Fail("should not block")
		}

		time.Sleep(200 * time.Millisecond)
		select {
		case al.Request() <- &types.Request{}:
			Fail("for second request, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrLinkManagerReset))
		default:
			Fail("should not block")
		}

		al = list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		list.Reset()
		select {
		case al.Request() <- &types.Request{}:
			Fail("for third request, select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrLinkRequestTimeout))
		default:
			Fail("should not block")
		}
	})

	It("should timeout stop on available link", func() {
		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)

		// time.Sleep(10 * time.Millisecond)
		list.AddAvailable(&testLink{}, false)
		al.Request() <- &types.Request{}

		<-time.After(200 * time.Millisecond)
		<-al.Closed()
		Expect(al.Error()).To(Equal(errRequestSent))

		al = list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)

		// time.Sleep(10 * time.Millisecond)
		list.AddAvailable(&testLink{}, false)
		<-time.After(200 * time.Millisecond)

		select {
		case al.Request() <- &types.Request{}:
			Expect(al.Error()).To(Equal(errRequestSent))
		case <-al.Closed():
			Fail("on successful request, select should not pick Closed()")
		default:
			Fail("should not block")
		}
	})

	It("should nil link be detected and reported", func() {
		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond)

		list.AddAvailable(nil, false)
		runtime.Gosched()

		select {
		case al.Request() <- &types.Request{}:
			Fail("select should not pick Request()")
		case <-al.Closed():
			Expect(al.Error()).To(Equal(ErrNilLink))
		default:
			Fail("should not block")
		}
	})

	It("should GetRequestPipe thread safe", func() {
		list := newAvailableLinks()
		list.SetLimit(10)
		list.AddAvailable(&productiveLink{alinks: list}, true)

		concurrency := 10
		n := 10

		// Start multiple consumers
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				for j := 0; j < n; j++ {
					al := list.GetRequestPipe()
					al.Request() <- &types.Request{}
					<-al.Closed()
				}
				wg.Done()
			}()
		}

		// Wait for processing
		shouldTimeout(func() {
			wg.Wait()
		}, func(timeout bool) {
			Expect(timeout).To(BeFalse())
		})

		total, _ := list.count()
		Expect(list.Len()).To(Equal(concurrency))
		Expect(total).To(Equal(concurrency))
		// Expect(buckets).To(Equal(3))

		list.Reset()
		total, _ = list.count()
		Expect(list.Len()).To(Equal(0))
		Expect(total).To(Equal(0))
		// Expect(buckets).To(Equal(3))
	})

	It("should ok in rare multi-threaded case: link lost after timeout", func() {
		// Enable slowdown designed for unittest to emulate complex multi-threaded case
		UnitTestMTC1 = true
		defer func() { UnitTestMTC1 = false }()

		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond) // 1: Set link request timeout at 100

		<-time.After(150 * time.Millisecond)  // 2: Timeout at 100, wait another 100 to continue to cancel link request
		list.AddAvailable(&testLink{}, false) // 3: New link added at 150  (Timeout triggered but we still get the link to proceed)
		runtime.Gosched()
		select {
		case al.Request() <- &types.Request{}: // 4: Request wait at 150, wait another 100 to continue to double check and send request.
			<-time.After(75 * time.Millisecond)                 // 5: Link request canceled at 200 (Link request canceled but we still get request to proceed)
			list.GetRequestPipe()                               // 6: New link request added at 225
			<-time.After(75 * time.Millisecond)                 // 7: Double check at 250 (Double check if link request canceled but new link request available)
			Expect(al.Error()).To(Equal(ErrLinkRequestTimeout)) // 8: Should timeout correct reported.
		case <-al.Closed():
			Fail("on successful request, select should not pick Closed()")
		default:
			Fail("should not block")
		}

		Expect(list.linkRequests.Len()).To(Equal(1))
		Expect(list.Len()).To(Equal(1))
		total, _ := list.count()
		Expect(total).To(Equal(1))

		list.Reset()
	})

	It("should ok in rare multi-threaded case: link reuse after timeout", func() {
		// Enable slowdown designed for unittest to emulate complex multi-threaded case
		UnitTestMTC1 = true
		defer func() { UnitTestMTC1 = false }()

		list := newAvailableLinks()

		al := list.GetRequestPipe()
		al.SetTimeout(100 * time.Millisecond) // 1: Set link request timeout at 100

		<-time.After(150 * time.Millisecond)  // 2: Timeout at 100, wait another 100 to continue to cancel link request
		list.AddAvailable(&testLink{}, false) // 3: New link added at 150  (Timeout triggered but we still get the link to proceed)
		runtime.Gosched()
		select {
		case al.Request() <- &types.Request{}: // 4: Request wait at 150, wait another 100 to continue to double check and send request.
			old := al.link
			al.link = nil                       // Mimic multi-threaded situation: al.link not set before timeout exection.
			<-time.After(75 * time.Millisecond) // 5: Link request canceled at 200 (Link request canceled but we still get request to proceed)
			al.link = old                       // Mimic multi-threaded situation: al.link set in link request.
			<-time.After(75 * time.Millisecond) // 6: 250 link request double checks if request has been closed.
			Expect(al.Error()).To(Equal(ErrLinkRequestTimeout))
		case <-al.Closed():
			Fail("on successful request, select should not pick Closed()")
		default:
			Fail("should not block")
		}

		Expect(list.linkRequests.Len()).To(Equal(0))
		Expect(list.Len()).To(Equal(1)) // Link correctly reused
		total, _ := list.count()
		Expect(total).To(Equal(1)) // Double check

		list.Reset()
	})
})
