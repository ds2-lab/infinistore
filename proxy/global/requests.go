package global

import (
	"github.com/cornelk/hashmap"
	"sync"

	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	emptySlice = make([]*types.Request, 100)     // Large enough to cover most cases.
)

type RequestCoordinator struct {
	pool         *sync.Pool
	registry     *hashmap.HashMap
}

func NewRequestCoordinator(size uintptr) *RequestCoordinator {
	return &RequestCoordinator{
		pool: &sync.Pool{
			New: newReqCounter,
		},
		registry: hashmap.New(size),
	}
}

func newReqCounter() interface{} {
	return &types.ClientReqCounter{}
}

func (c *RequestCoordinator) Register(reqId string, cmd string, d int64, p int64) *types.ClientReqCounter {
	prepared := c.pool.Get().(*types.ClientReqCounter)
	ret, ok := c.registry.GetOrInsert(reqId, prepared)
	counter := ret.(*types.ClientReqCounter)
	if !ok {
		// New counter registered, initialize values.
		counter.Cmd = cmd
		counter.DataShards = d
		counter.ParityShards = p
		counter.Returned = 0
		l := int(d + p)
		if cap(counter.Requests) < l {
			counter.Requests = make([]*types.Request, l)
		} else {
			if cap(emptySlice) < l {
				emptySlice = make([]*types.Request, cap(emptySlice) * 2)
			}
			if len(counter.Requests) != l {
				counter.Requests = counter.Requests[:l]
			}
			copy(counter.Requests, emptySlice[:l])
		}
	}
	return counter
}

func (c *RequestCoordinator) Load(reqId string) (*types.ClientReqCounter, bool) {
	if counter, ok := c.registry.Get(reqId); ok {
		return counter.(*types.ClientReqCounter), ok
	} else {
		return nil, ok
	}
}

func (c *RequestCoordinator) Clear(reqId string, counter *types.ClientReqCounter) {
	c.registry.Del(reqId)
	c.pool.Put(counter)
}
