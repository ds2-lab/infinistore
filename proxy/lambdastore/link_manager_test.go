package lambdastore

import (
	"runtime"
	"sync"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	// . "github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

type testLink struct {
}

func (l *testLink) SendRequest(_ *types.Request, _ ...interface{}) error {
	return nil
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
			al := list.GetRequestPipe()
			al.Request() <- &types.Request{}
			Expect(al.link).To(Not(BeNil()))
			Expect(al.err).To(BeNil())
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
			al := list.GetRequestPipe()
			al.Request() <- &types.Request{}
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
})
