package global

import (
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/util/hashmap"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	REQCNT_STATUS_RETURNED  uint64 = 0x0000000000000001
	REQCNT_STATUS_SUCCEED   uint64 = 0x0000000000010000
	REQCNT_STATUS_RECOVERED uint64 = 0x0000000100000000
	REQCNT_STATUS_FLUSHED   uint64 = 0x0001000000000000
	REQCNT_MASK_RETURNED    uint64 = 0x000000000000FFFF
	REQCNT_MASK_SUCCEED     uint64 = 0x00000000FFFF0000
	REQCNT_MASK_RECOVERED   uint64 = 0x0000FFFF00000000
	REQCNT_MASK_FLUSHED     uint64 = 0xFFFF000000000000
	REQCNT_BITS_RETURNED    uint64 = 0
	REQCNT_BITS_SUCCEED     uint64 = 16
	REQCNT_BITS_RECOVERED   uint64 = 32
	REQCNT_BITS_FLUSHED     uint64 = 48
)

type RequestCoordinator struct {
	pool     *sync.Pool
	registry hashmap.HashMap
}

func NewRequestCoordinator(size int) *RequestCoordinator {
	return &RequestCoordinator{
		pool: &sync.Pool{
			New: newRequestCounter,
		},
		registry: hashmap.NewMapWithStringKey(size),
	}
}

func (c *RequestCoordinator) Len() int {
	return c.registry.Len()
}

func (c *RequestCoordinator) Register(reqId string, cmd string, d int, p int, meta interface{}) *RequestCounter {
	prepared := c.pool.Get().(*RequestCounter)
	prepared.initialized.Add(1) // Block in case initalization is need.

	ret, ok := c.registry.LoadOrStore(reqId, prepared)
	// There is potential problem with this late initialization approach!
	// Since the counter will be used to coordinate async responses of the first batch of concurrent
	// chunk requests, it is ok here.
	// FIXME: Fix this if neccessary.
	counter := ret.(*RequestCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.reset(c, reqId, cmd, d, p, meta)
		// Release initalization lock
		counter.initialized.Done()
	} else {
		// Release unused lock
		prepared.initialized.Done()
		c.pool.Put(prepared) // Only put back the counter if it is not used.

		// Wait for counter to be initalized.
		counter.initialized.Wait()
	}
	return counter.Load()
}

func (c *RequestCoordinator) RegisterControl(reqId string, ctrl *types.Control) {
	c.registry.Store(reqId, ctrl)
}

// Load returns the RequestCounter that associated with the specified request id.
// It returns interface{} to be compatible with customized types.
// If a RequestCounter is returned, it calls Load() of the RequestCounter to ensure the reference count is correct.
func (c *RequestCoordinator) Load(reqId string) interface{} {
	item, _ := c.registry.Load(reqId)
	if counter, ok := item.(*RequestCounter); ok {
		return counter.Load()
	} else {
		return item
	}
}

func (c *RequestCoordinator) Clear(reqId string) {
	c.registry.Delete(reqId)
}

// Counter for returned requests.
type RequestCounter struct {
	Cmd          string
	DataShards   int
	ParityShards int
	Meta         interface{}
	Requests     []*types.Request

	coordinator   *RequestCoordinator
	reqId         string
	status        uint64 // int32(succeed) + int32(returned)
	numToFulfill  uint64
	initialized   sync.WaitGroup
	refs          int32
	waitForClient bool
}

func newRequestCounter() interface{} {
	return &RequestCounter{}
}

func (counter *RequestCounter) reset(c *RequestCoordinator, reqId string, cmd string, d int, p int, meta interface{}) {
	counter.Cmd = cmd
	counter.DataShards = d
	counter.ParityShards = p
	counter.Meta = meta
	counter.coordinator = c
	counter.reqId = reqId
	counter.status = 0
	counter.numToFulfill = uint64(d)
	if cmd == protocol.CMD_SET ||
		(Options.Evaluation && Options.NoFirstD) {
		counter.numToFulfill = uint64(d + p)
	}
	l := int(d + p)
	if cap(counter.Requests) < l {
		counter.Requests = make([]*types.Request, l)
	} else if len(counter.Requests) != l {
		counter.Requests = counter.Requests[:l]
	}
	counter.waitForClient = IsClientsideFirstDOptimization() && counter.numToFulfill < uint64(l)
}

func (c *RequestCounter) String() string {
	return c.reqId
}

// AddSucceeded sets specified chunk of the object request as succeeded. If the chunk is recovered from backing store, it will be marked as recovered.
// Returns the updated status of the request and if the chunk request has been fulfilled already.
func (c *RequestCounter) AddSucceeded(chunk int, recovered bool) (uint64, bool) {
	if recovered {
		return c.addReturnedWithOpts(chunk, REQCNT_STATUS_SUCCEED|REQCNT_STATUS_RETURNED|REQCNT_STATUS_RECOVERED)
	} else {
		return c.addReturnedWithOpts(chunk, REQCNT_STATUS_SUCCEED|REQCNT_STATUS_RETURNED)
	}
}

// AddReturned sets specified chunk of the object request as returned but failed.
// Returns the updated status of the request and if the chunk request has been fulfilled already.
func (c *RequestCounter) AddReturned(chunk int) (uint64, bool) {
	return c.addReturnedWithOpts(chunk, REQCNT_STATUS_RETURNED)
}

// AddFlushed sets specified chunk of the object request as successfully flushed to client.
func (c *RequestCounter) AddFlushed(chunk int) uint64 {
	return atomic.AddUint64(&c.status, REQCNT_STATUS_FLUSHED)
}

func (c *RequestCounter) addReturnedWithOpts(chunk int, opts uint64) (uint64, bool) {
	marked := c.markReturnd(chunk)
	if !marked {
		return c.Status(), !marked
	} else {
		return atomic.AddUint64(&c.status, opts), !marked
	}
}

func (c *RequestCounter) Status() uint64 {
	return atomic.LoadUint64(&c.status)
}

func (c *RequestCounter) Returned() uint64 {
	return atomic.LoadUint64(&c.status) & REQCNT_MASK_RETURNED >> REQCNT_BITS_RETURNED
}

func (c *RequestCounter) Succeeded() uint64 {
	return (atomic.LoadUint64(&c.status) & REQCNT_MASK_SUCCEED) >> REQCNT_BITS_SUCCEED
}

func (c *RequestCounter) Recovered() uint64 {
	return (atomic.LoadUint64(&c.status) & REQCNT_MASK_RECOVERED) >> REQCNT_BITS_RECOVERED
}

func (c *RequestCounter) IsFulfilled(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsFulfilled(c.Status())
	}

	if c.waitForClient {
		return (status[0]&REQCNT_MASK_FLUSHED)>>REQCNT_BITS_FLUSHED >= c.numToFulfill
	} else {
		return (status[0]&REQCNT_MASK_SUCCEED)>>REQCNT_BITS_SUCCEED >= c.numToFulfill
	}
}

func (c *RequestCounter) IsLate(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsLate(c.Status())
	}

	if c.waitForClient {
		return (status[0]&REQCNT_MASK_FLUSHED)>>REQCNT_BITS_FLUSHED > c.numToFulfill
	} else {
		return (status[0]&REQCNT_MASK_SUCCEED)>>REQCNT_BITS_SUCCEED > c.numToFulfill
	}
}

func (c *RequestCounter) IsAllReturned(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsAllReturned(c.Status())
	}
	return status[0]&REQCNT_MASK_RETURNED >= uint64(c.DataShards+c.ParityShards)
}

func (c *RequestCounter) IsAllFlushed(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsAllFlushed(c.Status())
	}
	return status[0]&REQCNT_MASK_FLUSHED>>REQCNT_BITS_FLUSHED >= uint64(c.DataShards+c.ParityShards)
}

func (c *RequestCounter) Release() {
	c.coordinator.Clear(c.reqId)
}

func (c *RequestCounter) ReleaseIfAllReturned(status ...uint64) bool {
	if c.IsAllReturned(status...) {
		c.Release()
		return true
	} else {
		return false
	}
}

func (c *RequestCounter) Load() *RequestCounter {
	atomic.AddInt32(&c.refs, 1)
	return c
}

func (c *RequestCounter) MarkReturnd(id *types.Id) (uint64, bool) {
	status, fulfilled := c.AddReturned(id.Chunk())
	return status, !fulfilled
}

func (c *RequestCounter) markReturnd(chunk int) bool {
	req := c.Requests[chunk]
	if req != nil {
		return req.MarkReturned()
	} else {
		return false
	}
}

func (c *RequestCounter) Close() {
	c.close()
}

func (c *RequestCounter) close() bool {
	cnt := atomic.AddInt32(&c.refs, -1)
	if c.ReleaseIfAllReturned() && cnt == 0 {
		// The counter will be recleased if all requests are all returned. However, for GET requests, the counter
		// can only be recycled after all requests are flushed to client.
		// To simplify the counter state machine, we will not try to recycle the counter after released.
		return true
	} else {
		return false
	}
}
