package cache

import (
	"context"
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

func newErrorReadAllCloser(buff []byte) *errorReadAllCloser {
	return &errorReadAllCloser{resp.NewInlineReader(buff)}
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
	It("should WaitStored() always return", func() {
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
			Expect(err).To(Equal(types.ErrChunkStoreFailed))
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
		reader, _ = chunk.Store(newErrorReadAllCloser([]byte(testStream)))
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

	It("should Error() returns err if chunk has been closed before stored", func() {
		testStream := "Test me."

		// Not stored.
		chunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		chunk.Close()
		Expect(chunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(0))

		// Stored with error.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ := chunk.Store(newErrorReadAllCloser([]byte(testStream)))
		reader.Close()
		// chunk.Close() not needed.
		Expect(chunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(0))

		// Stored after closed.
		chunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		chunk.Close()
		reader, _ = chunk.Store(resp.NewInlineReader([]byte(testStream)))
		reader.Close()
		Expect(chunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(0))
	})

	It("should Error() returns nil if chunk has been closed after stored", func() {
		testStream := "Test me."

		chunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ := chunk.Store(resp.NewInlineReader([]byte(testStream)))
		reader.Close()
		chunk.Close() // Extra Close() should not cause error.
		Expect(chunk.Error()).To(BeNil())
		Expect(cache.Len()).To(Equal(0))
	})

	It("should chunk be replaced after err", func() {
		testStream := "Test me."

		// Error chunk
		errChunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		reader, _ := errChunk.Store(newErrorReadAllCloser([]byte(testStream)))
		errChunk.StartPersist(nil, 100*time.Millisecond, nil)
		reader.Close()
		Expect(errChunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(1))

		// Should get new chunk
		chunk, first := cache.GetOrCreate("test", int64(len(testStream)))
		Expect(first).To(BeTrue())
		Expect(chunk).NotTo(Equal(errChunk))
		Expect(chunk.Error()).To(BeNil())
		Expect(cache.Len()).To(Equal(1))

		// Wait for errChunk to be released.
		<-time.After(200 * time.Millisecond)                             // errChunk persisting timeout.
		Expect(errChunk.(*persistChunk).refs.Load()).To(Equal(int32(0))) // errChunk should be released.
		Expect(errChunk.Error()).To(Equal(types.ErrChunkStoreFailed))    // Persisting error will not be set.
		Expect(cache.Len()).To(Equal(1))                                 // The release of errChunk should not cause the cache to be evicted (because the chunk has been replaced).

		// Verify the chunk in the cache.
		chunk2, first2 := cache.GetOrCreate("test", int64(len(testStream)))
		Expect(first2).To(BeFalse())
		Expect(chunk2).To(Equal(chunk))
		Expect(cache.Len()).To(Equal(1))

		// Cleanup
		chunk.Close()
		Expect(cache.Len()).To(Equal(0))
	})

	It("should loaded reader report error if storing failed", func() {
		testStream := "Test me."
		buff := make([]byte, 1)

		// Read after error.
		errChunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		interceptor, _ := errChunk.Store(newErrorReadAllCloser([]byte(testStream)))
		interceptor.Close()
		Expect(errChunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(cache.Len()).To(Equal(0))

		reader, err := errChunk.Load(context.Background())
		Expect(err).To(BeNil())
		_, err = reader.Read(buff)
		Expect(err).To(Equal(types.ErrChunkStoreFailed))
		err = reader.Close()
		Expect(err).To(Equal(types.ErrChunkStoreFailed))
		Expect(errChunk.(*persistChunk).refs.Load()).To(Equal(int32(0)))

		// Error during reading.
		errChunk, _ = cache.GetOrCreate("test", int64(len(testStream)))
		interceptor, _ = errChunk.Store(newErrorReadAllCloser([]byte(testStream)))
		go interceptor.Close()
		Expect(errChunk.Error()).To(BeNil())
		Expect(cache.Len()).To(Equal(1))

		reader, err = errChunk.Load(context.Background())
		Expect(err).To(BeNil())
		_, err = reader.Read(buff)
		Expect(err).To(Equal(types.ErrChunkStoreFailed))
		err = reader.Close()
		Expect(err).To(Equal(types.ErrChunkStoreFailed))

		Expect(err).To(Equal(types.ErrChunkStoreFailed))
		Expect(errChunk.Error()).To(Equal(types.ErrChunkStoreFailed))
		Expect(errChunk.(*persistChunk).refs.Load()).To(Equal(int32(0)))
		Expect(cache.Len()).To(Equal(0))
	})

	It("should cancelling loaded reader will not cause chunk error", func() {
		testStream := "Test me."
		buff := make([]byte, 1)

		chunk, _ := cache.GetOrCreate("test", int64(len(testStream)))
		interceptor, _ := chunk.Store(resp.NewInlineReader([]byte(testStream)))
		Expect(chunk.Error()).To(BeNil())
		Expect(cache.Len()).To(Equal(1))

		ctx, cancel := context.WithCancel(context.Background())
		reader, _ := chunk.Load(ctx)
		reader.(resp.Holdable).Hold()
		cancel() // Any error will automatically cancel the hold.
		_, err := reader.Read(buff)
		Expect(err).To(Equal(context.Canceled))
		err = reader.Close()
		Expect(err).To(Equal(context.Canceled))

		Expect(chunk.Error()).To(BeNil())
		interceptor.Close()
		Expect(cache.Len()).To(Equal(0))
	})
})
