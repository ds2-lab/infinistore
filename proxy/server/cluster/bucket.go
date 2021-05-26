package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/mason-leap-lab/infinicache/common/logger"

	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	BUCKET_EXPIRE = 0
	BUCKET_COLD   = 1
	BUCKET_ACTIVE = 2
)

var (
	ErrNotActiveBucket = errors.New("scale out failed, not in active bucket")
)

// A bucket is a view of a group
type Bucket struct {
	id int

	// group management
	group       *Group
	instances   []*lambdastore.Instance
	start       DefaultGroupIndex // Start logic index of backend group
	end         DefaultGroupIndex // End logic index of backend group
	activeStart GroupIndex        // Instances end at activeStart - 1 are full.

	//state
	state int

	// utilities
	log   logger.ILogger
	ready sync.WaitGroup
	mu    sync.RWMutex
}

type BucketIndex struct {
	DefaultGroupIndex
	BucketId int
}

func newBucket(id int, group *Group, num int) (bucket *Bucket, err error) {
	if pool.NumAvailable() < num {
		return nil, ErrInsufficientDeployments
	}

	bucket = &Bucket{
		id:        id,
		group:     group,
		log:       global.GetLogger(fmt.Sprintf("Bucket %d:", id)),
		instances: make([]*lambdastore.Instance, 0, num),
	}

	// expand
	bucket.start = group.EndIndex()
	bucket.activeStart = &bucket.start
	bucket.end, err = group.Expand(num)
	if err != nil {
		return nil, err
	}

	bucket.initInstance(bucket.start, bucket.end)
	bucket.state = BUCKET_ACTIVE
	return
}

func (b *Bucket) createNextBucket(num int) (bucket *Bucket, numInherited int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	nextID := b.id + 1

	// Shortcut for insufficient number of spare instances
	if b.activeStart.Idx()+num > b.end.Idx() {
		bucket, err = newBucket(nextID, b.group, num)
		return bucket, 0, err
	}

	// Compose new bucket consists of instances with spare capacity.
	bucket = &Bucket{
		id:          nextID,
		group:       b.group,
		log:         global.GetLogger(fmt.Sprintf("Bucket %d:", nextID)),
		instances:   make([]*lambdastore.Instance, b.end.Idx()-b.activeStart.Idx()),
		start:       DefaultGroupIndex(b.activeStart.Idx()),
		activeStart: b.activeStart,
		end:         b.end,
		state:       BUCKET_ACTIVE,
	}
	copy(bucket.instances, b.instances[b.activeStart.Idx()-b.start.Idx():])

	// Adjust current bucket
	b.end = bucket.start
	for i := b.end.Idx() - b.start.Idx(); i < len(b.instances); i++ {
		b.instances[i] = b.instances[i].GetShadowInstance()
	}

	// Update bucketIndex
	gInstances := b.group.SubGroup(bucket.start, bucket.end)
	for _, gins := range gInstances {
		gins.idx.(*BucketIndex).BucketId = nextID
	}
	return bucket, len(bucket.instances), nil
}

func (b *Bucket) initInstance(from, end DefaultGroupIndex) {
	for i := from; i < end; i = i.Next() {
		node := pool.GetForGroup(b.group, &BucketIndex{
			DefaultGroupIndex: i,
			BucketId:          b.id,
		})
		b.instances = append(b.instances, node)

		// Begin handle requests
		go node.HandleRequests()

		// Initialize instance, Bucket is not necessary if the start time of the instance is acceptable.
		// b.ready.Add(1)
		//
		// go func() {
		// 	node.WarmUp()
		// 	b.ready.Done()
		// }()
	}

	// Build log message
	if global.Options.Debug {
		b.log.Debug("%d nodes added:%v", len(b.instances), logger.NewFunc(func() string {
			var msg strings.Builder
			for _, ins := range b.instances {
				msg.WriteString(" ")
				msg.WriteString(ins.Name())
				msg.WriteString("-")
				msg.WriteString(strconv.FormatUint(ins.Id(), 10))
			}
			return msg.String()
		}))
	} else {
		b.log.Info("%d nodes added: %d ~ %d", len(b.instances), b.instances[0].Id(), b.instances[len(b.instances)-1].Id())
	}
}

func (b *Bucket) waitReady() {
	b.ready.Wait()
	b.log.Info("[Ready]")
}

func (b *Bucket) shouldScale(gins *GroupInstance, num int) bool {
	b.mu.RLock()
	scale := b.end.Idx()-gins.Idx() < num
	b.mu.RUnlock()
	return scale
}

func (b *Bucket) scale(num int) (gall []*GroupInstance, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.end != b.group.EndIndex() {
		return nil, ErrNotActiveBucket
	}

	if pool.NumAvailable() < num {
		return nil, ErrInsufficientDeployments
	}

	// expand
	from := b.end
	b.end, err = b.group.Expand(num)
	if err != nil {
		return nil, err
	}
	scaled := make([]*lambdastore.Instance, len(b.instances), len(b.instances)+num)
	copy(scaled, b.instances)
	b.instances = scaled
	b.initInstance(from, b.end)
	return b.group.SubGroup(from, b.end), nil
}

func (b *Bucket) flagInactive(gins *GroupInstance) {
	if b.activeStart.Idx() > gins.Idx() {
		return
	}

	b.mu.Lock()
	if b.activeStart.Idx() <= gins.Idx() {
		b.activeStart = gins.idx.(*BucketIndex).Next()
	}
	b.mu.Unlock()
}

func (b *Bucket) getInstances() []*lambdastore.Instance {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.instances[:b.end.Idx()-b.start.Idx()]
}

func (b *Bucket) activeInstances(activeNum int) []*lambdastore.Instance {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return all actives
	// Force to create a new slice mapping to avoid the length being changed.
	return b.instances[b.activeStart.Idx()-b.start.Idx() : b.end.Idx()-b.start.Idx()]
}

func (b *Bucket) len() int {
	return b.end.Idx() - b.start.Idx()
}

// types.ClusterStatus implementation
func (b *Bucket) InstanceLen() int {
	return len(b.instances)
}

func (b *Bucket) InstanceStats(idx int) types.InstanceStats {
	return b.instances[idx]
}

func (b *Bucket) AllInstancesStats() types.Iterator {
	all := b.instances
	return types.NewStatsIterator(all, len(all))
}

func (b *Bucket) InstanceStatsFromIterator(iter types.Iterator) (int, types.InstanceStats) {
	i, val := iter.Value()

	var ins *lambdastore.Instance
	switch item := val.(type) {
	case []*lambdastore.Instance:
		ins = item[i]
	case *lambdastore.Instance:
		ins = item
	}

	return i, ins
}
