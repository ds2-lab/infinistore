package global

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	errDummyResponse = errors.New("dummy response")
)

type TestRequestCounter struct {
	RequestCounter
	recycled int32
}

func (c *TestRequestCounter) Load() *TestRequestCounter {
	c.RequestCounter.Load()
	return c
}

func (c *TestRequestCounter) Close() {
	if c.close() {
		atomic.AddInt32(&c.recycled, 1)
	}
}

func newRequest(counter *TestRequestCounter, chunk string) *types.Request {
	req := types.GetRequest(nil)
	req.Id = types.Id{ChunkId: chunk}
	req.Cleanup = counter.Load()
	counter.Requests[req.Id.Chunk()] = req
	return req
}

var _ = Describe("RequestCoordinator", func() {
	coordinator := NewRequestCoordinator(10)

	It("should single chunk request be recycled", func() {
		counter := &TestRequestCounter{}
		counter.reset(coordinator, "test", "get", 1, 0)
		req := newRequest(counter, "0")
		err := req.SetResponse(errDummyResponse)

		Expect(err).To(Equal(types.ErrNoClient))
		Expect(counter.recycled).To(Equal(int32(1)))
	})

	It("should timeout in request lock free", func() {
		counter := &TestRequestCounter{}
		counter.reset(coordinator, "test", "get", 1, 0)
		req := newRequest(counter, "0")

		go func() {
			defer GinkgoRecover()

			Expect(func() { req.Timeout(types.DEBUG_INITPROMISE_WAIT) }).ToNot(Panic())
		}()
		runtime.Gosched()

		req.SetResponse(errDummyResponse)

		<-time.After(100 * time.Millisecond)
		Expect(counter.recycled).To(Equal(int32(1)))
	})
})
