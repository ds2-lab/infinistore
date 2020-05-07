package global

import (
	"sync"
	"sync/atomic"

	"github.com/cornelk/hashmap"

	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	REQCNT_STATUS_SUCCEED  = 0x0000000100000000
	REQCNT_STATUS_RETURNED = 0x0000000000000001
	REQCNT_MASK_SUCCEED    = 0xFFFFFFFF00000000
	REQCNT_MASK_RETURNED   = 0x00000000FFFFFFFF
)

var (
	emptySlice = make([]*types.Request, 100) // Large enough to cover most cases.
)

type RequestCoordinator struct {
	pool     *sync.Pool
	registry *hashmap.HashMap
}

func NewRequestCoordinator(size uintptr) *RequestCoordinator {
	return &RequestCoordinator{
		pool: &sync.Pool{
			New: newRequestCounter,
		},
		registry: hashmap.New(size),
	}
}

func (c *RequestCoordinator) Register(reqId string, cmd string, d int64, p int64) *RequestCounter {
	prepared := c.pool.Get().(*RequestCounter)
	ret, ok := c.registry.GetOrInsert(reqId, prepared)
	// There is potential problem with this late initialization approach!
	// Since the counter will be used to coordinate async responses of the first batch of concurrent
	// chunk requests, it is ok here.
	// FIXME: Fix this if neccessary.
	counter := ret.(*RequestCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.Cmd = cmd
		counter.DataShards = d
		counter.ParityShards = p
		counter.coordinator = c
		counter.reqId = reqId
		counter.status = 0
		l := int(d + p)
		if cap(counter.Requests) < l {
			counter.Requests = make([]*types.Request, l)
		} else {
			if cap(emptySlice) < l {
				emptySlice = make([]*types.Request, cap(emptySlice)*2)
			}
			if len(counter.Requests) != l {
				counter.Requests = counter.Requests[:l]
			}
			copy(counter.Requests, emptySlice[:l])
		}
	} else {
		c.pool.Put(prepared)
	}
	return counter
}

func (c *RequestCoordinator) RegisterControl(reqId string, ctrl *types.Control) {
	c.registry.Insert(reqId, ctrl)
}

func (c *RequestCoordinator) Load(reqId string) interface{} {
	counter, _ := c.registry.Get(reqId)
	return counter
}

func (c *RequestCoordinator) Clear(item interface{}) {
	reqId, ok := item.(string)
	if ok {
		c.registry.Del(reqId)
		return
	}

	c.tryClearCounter(item)
}

func (c *RequestCoordinator) tryClearCounter(item interface{}) bool {
	counter, ok := item.(*RequestCounter)
	if ok {
		c.registry.Del(counter.reqId)
		c.pool.Put(counter)
	}

	return ok
}

// Counter for returned requests.
type RequestCounter struct {
	Cmd          string
	DataShards   int64
	ParityShards int64
	Requests     []*types.Request

	coordinator *RequestCoordinator
	reqId       string
	status      uint64 // int32(succeed) + int32(returned)
}

func newRequestCounter() interface{} {
	return &RequestCounter{}
}

func (c *RequestCounter) String() string {
	return c.reqId
}

func (c *RequestCounter) AddSucceeded(chunk int) uint64 {
	status := atomic.AddUint64(&c.status, REQCNT_STATUS_SUCCEED+REQCNT_STATUS_RETURNED)
	if c.Requests[chunk] != nil {
		c.Requests[chunk].MarkReturned()
	}
	return status
}

func (c *RequestCounter) AddReturned(chunk int) uint64 {
	status := atomic.AddUint64(&c.status, REQCNT_STATUS_RETURNED)
	if c.Requests[chunk] != nil {
		c.Requests[chunk].MarkReturned()
	}
	return status
}

func (c *RequestCounter) Status() uint64 {
	return atomic.LoadUint64(&c.status)
}

func (c *RequestCounter) Succeeded() uint64 {
	return (atomic.LoadUint64(&c.status) & REQCNT_MASK_SUCCEED) >> 32
}

func (c *RequestCounter) Returned() uint64 {
	return atomic.LoadUint64(&c.status) & REQCNT_MASK_RETURNED
}

func (c *RequestCounter) IsFulfilled(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsFulfilled(c.Status())
	}
	return (status[0]&REQCNT_MASK_SUCCEED) >> 32 >= uint64(c.DataShards)
}

func (c *RequestCounter) IsLate(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsLate(c.Status())
	}
	return (status[0]&REQCNT_MASK_SUCCEED) >> 32 > uint64(c.DataShards)
}

func (c *RequestCounter) IsAllReturned(status ...uint64) bool {
	if len(status) == 0 {
		return c.IsAllReturned(c.Status())
	}
	return status[0]&REQCNT_MASK_RETURNED >= uint64(c.DataShards+c.ParityShards)
}

func (c *RequestCounter) Release() {
	c.coordinator.Clear(c)
}

func (c *RequestCounter) ReleaseIfAllReturned(status ...uint64) {
	if c.IsAllReturned(status...) {
		c.Release()
	}
}
