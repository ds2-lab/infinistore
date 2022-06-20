package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"

	"github.com/mason-leap-lab/infinicache/proxy/config"
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
	ErrNotActiveBucket         = errors.New("scale out failed, not in active bucket")
	BucketFlushInactiveTimeout = time.Second
)

// A bucket is a view of a group
type Bucket struct {
	id int

	// group management
	group         *Group
	instances     []*GroupInstance
	start         DefaultGroupIndex // Start logic index of backend group
	end           DefaultGroupIndex // End logic index of backend group
	activeStart   GroupIndex        // Instances end at activeStart - 1 are full.
	activeChanged bool
	disabled      hashmap.HashMap // Buffer fulled instance
	flushLimit    int             // The max number stored in buffer.
	flushedAt     time.Time       // The last time the "disabled" fulled instance was cleared.

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

	limit := config.NumLambdaClusters
	if global.Options.GetNumFunctions() > 0 {
		limit = global.Options.GetNumFunctions()
	}
	if limit > num {
		limit = num
	}
	bucket = &Bucket{
		id:         id,
		group:      group,
		log:        global.GetLogger(fmt.Sprintf("Bucket %d:", id)),
		instances:  make([]*GroupInstance, 0, num),
		disabled:   hashmap.NewMap(num),
		flushLimit: limit,
	}

	// expand
	bucket.start = group.EndIndex()
	bucket.activeStart = &bucket.start
	bucket.activeChanged = true
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

	// Clean up inactives
	b.flushInactiveLocked()
	// Compose new bucket consists of instances with spare capacity.
	bucket = &Bucket{
		id:            nextID,
		group:         b.group,
		log:           global.GetLogger(fmt.Sprintf("Bucket %d:", nextID)),
		instances:     make([]*GroupInstance, b.end.Idx()-b.activeStart.Idx()),
		disabled:      b.disabled,
		flushLimit:    b.flushLimit,
		start:         DefaultGroupIndex(b.activeStart.Idx()),
		activeStart:   b.activeStart,
		activeChanged: true,
		end:           b.end,
		state:         BUCKET_ACTIVE,
	}
	copy(bucket.instances, b.instances[b.activeStart.Idx()-b.start.Idx():])
	b.disabled = nil

	// Adjust current bucket, keep old instances array intact because it can be referenced somewhere.
	b.end = bucket.start
	newInstances := make([]*GroupInstance, len(b.instances))
	copy(newInstances[:b.end.Idx()-b.start.Idx()], b.instances[:b.end.Idx()-b.start.Idx()])
	for i := b.end.Idx() - b.start.Idx(); i < len(b.instances); i++ {
		newInstances[i] = &GroupInstance{LambdaDeployment: b.instances[i].Instance().GetShadowInstance()}
	}
	b.instances = newInstances

	// Update bucketIndex
	for _, gins := range bucket.instances {
		gins.idx.(*BucketIndex).BucketId = nextID
	}
	return bucket, len(bucket.instances), nil
}

func (b *Bucket) initInstance(from, end DefaultGroupIndex) {
	for i := from; i < end; i = i.Next() {
		gins := pool.GetForGroup(b.group, &BucketIndex{
			DefaultGroupIndex: i,
			BucketId:          b.id,
		})
		b.instances = append(b.instances, gins)

		// Begin handle requests
		go gins.Instance().HandleRequests()

		// Initialize instance, Bucket is not necessary if the start time of the instance is acceptable.
		// b.ready.Add(1)
		//
		// go func() {
		// 	gins.Instance().WarmUp()
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
	scaled := make([]*GroupInstance, len(b.instances), len(b.instances)+num)
	copy(scaled, b.instances)
	b.instances = scaled
	b.initInstance(from, b.end)
	return b.group.SubGroup(from, b.end), nil
}

func (b *Bucket) flagInactive(gins *GroupInstance) {
	if b.activeStart.Idx() > gins.Idx() {
		return
	}

	gins.disabled = true
	b.disabled.Store(gins.Idx(), gins)
	if b.disabled.Len() < b.flushLimit && time.Since(b.flushedAt) < BucketFlushInactiveTimeout {
		return
	}

	b.mu.Lock()
	b.flushInactiveLocked()
	// if b.activeStart.Idx() <= gins.Idx() {
	// 	b.activeStart = gins.idx.(*BucketIndex).Next()
	//  b.activeChanged = true
	// }
	b.mu.Unlock()
}

func (b *Bucket) flushInactiveLocked() {
	if b.disabled.Len() == 0 {
		return
	}

	ginsAll := b.group.SubGroup(b.activeStart, b.end)
	checked := 0
	flushed := 0
	for b.disabled.Len() > 0 {
		for i := checked; i < len(ginsAll); i++ {
			if !ginsAll[i].disabled {
				continue
			}
			b.disabled.Delete(ginsAll[i].Idx())
			b.activeStart = ginsAll[flushed].idx.(*BucketIndex).Next()
			b.activeChanged = true
			// Pack disabled instance
			if flushed != i {
				b.swapLock(ginsAll[flushed], ginsAll[i])
				flushed++
			}
			checked = i + 1
			break
		}
	}
	b.flushedAt = time.Now()
}

func (b *Bucket) getInstances() []*GroupInstance {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get a copy of the slice, so the len will not change later.
	return b.instances[:b.end.Idx()-b.start.Idx()]
}

func (b *Bucket) activeInstances(activeNum int) []*GroupInstance {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return all actives
	// Force to create a new slice mapping to avoid the length being changed.
	return b.instances[b.activeStart.Idx()-b.start.Idx() : b.end.Idx()-b.start.Idx()]
}

func (b *Bucket) redundantInstances() []*GroupInstance {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Return all actives
	// Force to create a new slice mapping to avoid the length being changed.
	if b.activeStart.Idx()+b.flushLimit >= b.end.Idx() {
		return nil
	}
	return b.instances[b.activeStart.Idx()+b.flushLimit-b.start.Idx() : b.end.Idx()-b.start.Idx()]
}

func (b *Bucket) len() int {
	return b.end.Idx() - b.start.Idx()
}

func (b *Bucket) swapLock(gins1 *GroupInstance, gins2 *GroupInstance) {
	b.group.Swap(gins1, gins2)
	// Swapped, update local copy
	b.instances[gins2.Idx()-b.start.Idx()], b.instances[gins1.Idx()-b.start.Idx()] = gins2, gins1
}

// types.ClusterStatus implementation
func (b *Bucket) InstanceLen() int {
	return len(b.instances)
}

func (b *Bucket) InstanceStats(idx int) types.InstanceStats {
	return b.instances[idx].Instance()
}

func (b *Bucket) AllInstancesStats() types.Iterator {
	all := b.instances
	return types.NewStatsIterator(all, len(all))
}

func (b *Bucket) InstanceStatsFromIterator(iter types.Iterator) (int, types.InstanceStats) {
	i, val := iter.Value()

	var ins *lambdastore.Instance
	switch item := val.(type) {
	case []*GroupInstance:
		ins = item[i].Instance()
	case *GroupInstance:
		ins = item.Instance()
	}

	return i, ins
}

func (b *Bucket) MetaStats() types.MetaStoreStats {
	return nil
}
