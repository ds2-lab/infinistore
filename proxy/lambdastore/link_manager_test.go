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

func (l *AvailableLinks) count() (int, int) {
	total := len(l.bottom.links)
	buckets := 1
	for b := l.top; b != l.bottom; b = b.next {
		total += len(b.links)
		buckets++
	}
	if l.linkRequest != nil {
		total += 1
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
			Expect(al.link).To(Not(BeNil()))
			Expect(al.err).To(Equal(errRequestSent))
			Expect(list.linkRequest).To(BeNil())
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
			Expect(list.linkRequest).To(BeNil())
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
		runtime.Gosched()
		Expect(list.Len()).To(Equal(1))
		expectedLink, _ := al.link.(*testLink)
		Expect(expectedLink).To(Equal(link))
		var expectedAl *AvailableLink = list.linkRequest
		Expect(expectedAl).To(Equal(al))

		list.Reset()
		Expect(list.Len()).To(Equal(0))
		runtime.Gosched()
		Expect(list.Len()).To(Equal(0))
		Expect(al.link).To(Not(BeNil()))
		Expect(al.err).To(Equal(ErrLinkManagerReset))
		Expect(list.linkRequest).To(BeNil())
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
		runtime.Gosched()
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
		runtime.Gosched()
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
			Fail("for second request, select should not pick Request()")
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
})
