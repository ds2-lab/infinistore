package server

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/wangaoone/LambdaObjectstore/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"

	"github.com/cornelk/hashmap"
)

var (
	bucketPool = sync.Pool{
		New: func() interface{} {
			return &bucket{}
		},
	}
)

type bucket struct {
	log         logger.ILogger
	id          int
	m           hashmap.HashMap
	group       *Group
	initialized int32
	ready       chan struct{}
	offset      int
	from        int
}

func bucketStart(id int, ready chan struct{}) *bucket {
	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	//bucket.offset = 0
	bucket.from = 0
	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		idx := i + ActiveInstance

		node := scheduler.GetForGroup(bucket.group, idx, "")
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		go func() {
			node.WarmUp()
			if atomic.AddInt32(&bucket.initialized, 1) == int32(bucket.group.Len()) {
				bucket.log.Info("[Proxy is ready]")
				close(ready)
			}
		}()

		// Begin handle requests
		go node.HandleRequests()

	}

	return bucket
}

func NewBucket(id int) *bucket {
	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	//bucket.offset = 0
	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		node := scheduler.GetForGroup(bucket.group, i, "")
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		go func() {
			node.WarmUp()
			if atomic.AddInt32(&bucket.initialized, 1) == int32(bucket.group.Len()) {
				bucket.log.Info("[Bucket %v is ready]", bucket.id)
			}
		}()

		// Begin handle requests
		go node.HandleRequests()

	}
	bucket.from = id * len(bucket.group.All)
	return bucket
}

func (b *bucket) Ready() chan struct{} {
	return b.ready
}

func (b *bucket) Size() int {
	return b.m.Len()
}

func (b *bucket) close() {
	bucketPool.Put(b)
}

func (b *bucket) scale(group *Group) {
	for i := range group.All {
		b.group.All = append(b.group.All, group.All[i])
	}
	b.from += len(group.All)
}
