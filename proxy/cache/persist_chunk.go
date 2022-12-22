package cache

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/redeo/resp"

	"github.com/mason-leap-lab/infinicache/common/redeo/server"
	"github.com/mason-leap-lab/infinicache/common/sync/atomic"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	noneIntercepted = &dummyPersistInterceptor{}
	debugIDRemover  = regexp.MustCompile(`:[0-9a-f-]+$`)
)

type persistChunkInterceptor interface {
	BytesIntercepted() int64
}

type dummyPersistInterceptor struct {
}

func (d *dummyPersistInterceptor) BytesIntercepted() int64 { return 0 }

type persistChunk struct {
	protocol.Contextable
	cache *persistCache

	key         string
	data        []byte
	size        int64
	refs        atomic.Int32
	cancel      context.CancelFunc
	interceptor persistChunkInterceptor
	closed      bool

	// We use sync.Cond for readers' synchronization. See lab/blocker for a overhead benchmark.
	mu       sync.Mutex
	notifier *sync.Cond
	once     sync.Once
	err      error
}

func newPersistChunk(c *persistCache, key string, size int64) *persistChunk {
	// TODO: Init with chunk instance.
	chunk := &persistChunk{
		cache:       c,
		data:        nil,
		interceptor: noneIntercepted,
	}
	chunk.reset(key, size)
	chunk.notifier = sync.NewCond(&chunk.mu)
	return chunk
}

// String returns a description of the chunk.
func (b *persistChunk) String() string {
	return fmt.Sprintf("%s:%d/%d", b.Key(), b.BytesStored(), b.Size())
}

func (pc *persistChunk) reset(key string, size int64) {
	pc.key = key
	if global.Options.Debug {
		pc.key = fmt.Sprintf("%s:%s", key, uuid.New().String())
	}
	pc.size = size
}

func (pc *persistChunk) Key() string {
	return pc.key
}

func (pc *persistChunk) Size() int64 {
	return pc.size
}

func (pc *persistChunk) IsStored() bool {
	return pc.BytesStored() == pc.size
}

func (pc *persistChunk) BytesStored() int64 {
	return pc.interceptor.BytesIntercepted()
}

// Store will be called in two situations:
// 1. Before dispatching SET request.
// 1.err.1. If error occurs during reading(setting), the SET request will fail and the chunk will be closed
//          in lambdastore/connection:doneRequest(). The chunk will not be available for GETs.
// 1.err.2. If successfully stored and the SET request fails, the chunk will be closed after DonePersist(err).
// 1.err.3. If SET request succeeds, a retrial is scheduled, and the RE-SET request fails, the chunk will
//          be RE-SET again after StartPersist timeout in waitPersistTimeout(). No repeated Store will be called.
// 2. On getting response of the first of concurrent GET requests.
// 2.err.1. If request fails to send, the chunk will be closed in lambdastore/instance:handleRequest().
// 2.err.1. If error occurs before getting the response, the chunk will be closed immediatly with error in lambdastore/connection:doneRequest().
//          And all Load()ing requests will be notified with error.
// 2.err.2. If error occurs during reading(getting), all Load()ing requests will be notified with error.
func (pc *persistChunk) Store(reader resp.AllReadCloser) (resp.AllReadCloser, error) {
	if pc.interceptor != noneIntercepted {
		return nil, types.ErrStoredTwice
	} else if reader.Len() != pc.size {
		return nil, types.ErrInvalidChunkSize
	}

	pc.cache.log.Debug("%s: Storing initiated.", pc.Key())

	pc.refs.Add(1)
	if pc.data == nil {
		pc.data = make([]byte, pc.size)
	}
	interceptor := server.NewInterceptReaderWithBuffer(reader, pc.data)
	pc.interceptor = interceptor
	interceptor.OnIntercept(pc.notifyData)
	interceptor.OnClose(pc.waitInterceptor)

	return interceptor, nil
	// TODO: Add support to read from break point
}

// WaitStored will wait until the chunk is stored or error occurs.
func (pc *persistChunk) WaitStored() error {
	_, err := pc.waitData(0, nil)
	return err
}

func (pc *persistChunk) Load(ctx context.Context) (resp.AllReadCloser, error) {
	pc.refs.Add(1)
	pc.cache.log.Debug("%s: Loading initiated", pc.Key())
	reader := newPersistChunkReader(ctx, pc)
	return reader, nil
}

func (pc *persistChunk) LoadAll(ctx context.Context) (data []byte, err error) {
	pc.refs.Add(1)
	defer pc.doneRefs()

	done := make(chan struct{})
	stored := pc.BytesStored()
	for stored < pc.Size() {
		stored, err = pc.waitDataWithContext(ctx, stored, done)
		if err != nil {
			break
		}
	}
	data = pc.data[:stored]
	return
}

// StartPersist will be declared before the chunk has been stored during setting.
// It is retry-safe, so declare StartPersist before DonePersist will not increase the reference count.
// Timeout and retry method is specified so it will not retaining the chunk forever.
// In this implmenation, StartPersist will be called:
// 1. In lambdastore/connection:sendRequest().
func (pc *persistChunk) StartPersist(req interface{}, timeout time.Duration, retry types.PersistRetrier) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Validate existing context. Do not add reference if it's already timed out and retrying.
	ctx := pc.Context()
	if err := ctx.Err(); err == context.Canceled {
		// The context has been canceled, we are done. It can only be canceled in DonePersist.
		return
	} else if pc.cancel != nil {
		// We are retrying, cancel the previous context.
		pc.cancel()
	} else {
		// It is the first time calling StartPersist, add reference.
		pc.refs.Add(1)
		pc.cache.log.Debug("%s: Persisting started.", pc.Key())
	}

	ctx, pc.cancel = context.WithTimeout(context.WithValue(context.Background(), types.CtxKeyRequest, req), timeout)
	pc.SetContext(ctx)

	// Monitor timeout asynchronously.
	go pc.waitPersistTimeout(ctx, retry)
}

// DonePersist will be called after the chunk has been stored or failed to store.
// It is reference-count-safe because the pc.cancel is used and nullified.
// Declaring another StartPersist after DonePersist will fail because ctx.Err() will be context.Canceled.
// In this implmenation, DonePersist will be called:
// 1. In lambdastore/connection:persistedHandler(true) to indicate the success.
// 2. In CloseWithError(err) when an error occurs.
func (pc *persistChunk) DonePersist() {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.cancel == nil {
		// StartPersist has not been called, ignore.
		return
	}

	pc.cancel()
	pc.cancel = nil
	left := pc.doneRefs()
	pc.cache.log.Debug("%s: Done persisting, ref counter: %d", pc.Key(), left)
}

func (pc *persistChunk) Error() error {
	return pc.err
}

func (pc *persistChunk) CanClose() bool {
	return pc.refs.Load() <= 0
}

func (pc *persistChunk) Close() {
	pc.CloseWithError(types.ErrChunkClosed)
}

func (pc *persistChunk) CloseWithError(err error) {
	// Clear persisting flag and avoid retrial.
	pc.DonePersist()

	if pc.IsStored() {
		return
	}

	if err != types.ErrChunkClosed {
		pc.cache.log.Warn("%s: Closed with error: %v, affected: %d", pc.Key(), err, pc.refs.Load())
	} else {
		pc.cache.log.Debug("%s: Closed", pc.Key())
	}

	pc.notifyError(err)

	// Close the chunk if no reference left.
	if pc.CanClose() {
		pc.once.Do(pc.close)
	}
}

func (pc *persistChunk) IsClosed() bool {
	return pc.closed
}

func (pc *persistChunk) doneRefs() int32 {
	left := pc.refs.Add(-1)
	if left == 0 {
		pc.once.Do(pc.close)
	}
	return left
}

func (pc *persistChunk) close() {
	key := pc.Key()
	if global.Options.Debug {
		key = debugIDRemover.ReplaceAllString(key, "")
	}
	pc.cache.remove(key, pc)
	pc.closed = true
	pc.cache.log.Debug("%s Removed, remaining %d keys", pc.Key(), pc.cache.Len())
}

func (pc *persistChunk) notifyError(err error) {
	pc.mu.Lock()
	pc.err = types.ErrChunkStoreFailed
	pc.notifier.Broadcast()
	pc.mu.Unlock()
}

func (pc *persistChunk) notifyData(interceptor *server.InterceptReader) {
	err := interceptor.LastError()
	if err != nil {
		pc.cache.log.Warn("%s: Storing with error: %v, affected: %d", pc.Key(), err, pc.refs.Load())
		pc.notifyError(err)
		return
	}

	pc.mu.Lock()
	pc.notifier.Broadcast()
	pc.mu.Unlock()
}

func (pc *persistChunk) waitData(nRead int64, done chan<- struct{}) (int64, error) {
	pc.mu.Lock()
	if pc.BytesStored() <= nRead && pc.err == nil {
		pc.notifier.Wait()
	}
	pc.mu.Unlock()

	if done != nil {
		done <- struct{}{}
	}
	return pc.BytesStored(), pc.err
}

func (pc *persistChunk) waitDataWithContext(ctx context.Context, nRead int64, done chan struct{}) (stored int64, err error) {
	go pc.waitData(nRead, done)
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-done:
		err = pc.err
	}

	return pc.BytesStored(), err
}

func (pc *persistChunk) waitInterceptor(interceptor *server.InterceptReader) {
	left := pc.doneRefs()
	pc.cache.log.Debug("%s: Stored(%d/%d), ref counter: %d", pc.Key(), pc.BytesStored(), pc.Size(), left)
}

func (pc *persistChunk) waitReader(reader resp.AllReadCloser, loaded int64) {
	left := pc.doneRefs()
	pc.cache.log.Debug("%s: Loaded(%d/%d), ref counter: %d", pc.Key(), loaded, pc.Size(), left)
}

func (pc *persistChunk) waitPersistTimeout(ctx context.Context, retry types.PersistRetrier) {
	<-ctx.Done()
	if ctx.Err() == context.DeadlineExceeded {
		if retry != nil {
			pc.cache.log.Debug("%s: Persisting timeout, retry...", pc.Key())
			retry(pc)
		} else {
			// For debug only, warn the user.
			pc.cache.log.Warn("%s: Persisting timeout and no retrier specified. We are done.", pc.Key())
			pc.DonePersist()
		}
	}
}
