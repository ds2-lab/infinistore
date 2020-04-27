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
}

func newBucket(id int, args ...interface{}) *bucket {

	// if this is the initial bucket
	var ready chan struct{}

	bucket := bucketPool.Get().(*bucket)

	bucket.log = &logger.ColorLogger{
		Prefix: fmt.Sprintf("Bucket %d", id),
		Level:  global.Log.GetLevel(),
		Color:  true,
	}
	bucket.m = hashmap.HashMap{}
	bucket.id = id
	// initial corresponding group
	bucket.group = NewGroup(NumLambdaClusters)

	for i := range bucket.group.All {
		node := scheduler.GetForGroup(bucket.group, i)
		node.Meta.Capacity = InstanceCapacity
		node.Meta.IncreaseSize(InstanceOverhead)
		bucket.log.Debug("[adding lambda instance %v]", node.Name())

		// Initialize instance, this is not necessary if the start time of the instance is acceptable.
		go func() {
			node.WarmUp()
			if atomic.AddInt32(&bucket.initialized, 1) == int32(bucket.group.Len()) {
				bucket.log.Info("[Bucket %v is ready]", bucket.id)
				if len(args) > 0 {
					ready = args[0].(chan struct{})
					close(ready)
				}
			}
		}()

		// Begin handle requests
		go node.HandleRequests()

	}

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

func (b *bucket) append(group *Group) {
	for i := 0; i < len(group.All); i++ {
		b.group.All = append(b.group.All, group.All[i])
	}
	b.log.Debug(" scale out, current bucket group len is %v", len(b.group.All))
}
