package cache

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/redeo/resp"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	cache = NewPersistCache()
)

type TimeoutResult bool

func (t TimeoutResult) String() string {
	if t {
		return "timeout"
	} else {
		return "not timeout"
	}
}

type errorReadAllCloser struct {
	*resp.InlineReader
}

func (r *errorReadAllCloser) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func TestTypes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ProxyCache")
}

func shouldNotTimeout[V any](test func() V, expects ...TimeoutResult) V {
	expect := TimeoutResult(false)
	if len(expects) > 0 {
		expect = expects[0]
	}

	timer := time.NewTimer(time.Second)
	timeout := TimeoutResult(false)
	responeded := make(chan V)
	var ret V
	go func() {
		responeded <- test()
	}()
	select {
	case <-timer.C:
		timeout = TimeoutResult(true)
	case ret = <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}

	Expect(timeout).To(Equal(expect))
	return ret
}

// func shouldTimeout[V any](test func() V) {
// 	shouldNotTimeout(test, TimeoutResult(true))
// }

var _ = Describe("PersistChunk", func() {
	It("should WaitStored always return", func() {
		testStream := "Test me."
		var done sync.WaitGroup

		// Success
		chunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ := chunk.Store(resp.NewInlineReader([]byte(testStream)))
		done.Add(1)
		go func() {
			defer done.Done()
			defer GinkgoRecover()

			err := shouldNotTimeout(chunk.WaitStored)
			Expect(err).To(BeNil())
			Expect(cache.Len()).To(Equal(0))
		}()
		reader.Close()
		done.Wait()

		// Success regardless prematch close and success read.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ = chunk.Store(resp.NewInlineReader([]byte(testStream)))
		reader.(resp.Holdable).Hold()
		done.Add(1)
		go func() {
			defer done.Done()
			defer GinkgoRecover()

			err := shouldNotTimeout(chunk.WaitStored)
			Expect(err).To(BeNil())
			Expect(cache.Len()).To(Equal(0))
		}()
		go reader.Close()
		chunk.Close()
		reader.(resp.Holdable).Unhold()
		done.Wait()

		// Success after stored.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ = chunk.Store(resp.NewInlineReader([]byte(testStream)))
		reader.Close()
		err := shouldNotTimeout(chunk.WaitStored)
		Expect(err).To(BeNil())
		Expect(cache.Len()).To(Equal(0))

		// Failure with prematch close and failed read.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ = chunk.Store(&errorReadAllCloser{InlineReader: resp.NewInlineReader([]byte(testStream))})
		reader.(resp.Holdable).Hold()
		done.Add(1)
		go func() {
			defer done.Done()
			defer GinkgoRecover()

			err := shouldNotTimeout(chunk.WaitStored)
			Expect(err).To(Equal(types.ErrChunkStoreFailed))
			Expect(cache.Len()).To(Equal(0))
		}()
		go reader.Close()
		chunk.Close()
		reader.(resp.Holdable).Unhold()
		done.Wait()

		// Failure bofore Store() being called.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		chunk.Close()
		err = shouldNotTimeout(chunk.WaitStored)
		Expect(err).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(0))
	})
})
