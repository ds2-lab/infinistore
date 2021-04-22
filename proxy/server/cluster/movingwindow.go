package cluster

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/util/promise"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"

	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	MaxBackingNodes = 5

	ErrInvalidInstance = errors.New("invalid instance")
)

// reuse window and interval should be MINUTES
type MovingWindow struct {
	numFuncSteps   int // Minimum number of functions scale at a time.
	numBufferFuncs int // Number of functions initialized as the buffer.

	log     logger.ILogger
	placer  *metastore.DefaultPlacer
	group   *Group
	buckets []*Bucket
	buff    []*Bucket

	cursor    *Bucket
	startTime time.Time

	scaler chan *types.ScaleEvent
	// scaleCounter int32

	mu   sync.RWMutex
	done chan struct{}

	numActives int
}

func NewMovingWindow() *MovingWindow {
	return NewMovingWindowWithOptions(global.Options.GetNumFunctions())
}

func NewMovingWindowWithOptions(numFuncSteps int) *MovingWindow {
	initPool(numFuncSteps)

	cluster := &MovingWindow{
		numFuncSteps:   numFuncSteps,
		numBufferFuncs: numFuncSteps * (config.BackupsPerInstance/MaxBackingNodes - 1),

		log:       global.GetLogger("MovingWindow: "),
		group:     NewGroup(0),
		buckets:   make([]*Bucket, 0, config.NumAvailableBuckets+2), // Reserve space for new bucket and last expired bucket
		buff:      make([]*Bucket, 0, config.NumAvailableBuckets+2),
		startTime: time.Now(),

		// for scaling out
		scaler: make(chan *types.ScaleEvent, numFuncSteps*100), // Reserve enough space for event queue to pervent blocking.
		// scaleCounter: 0,

		done: make(chan struct{}),
	}
	cluster.placer = metastore.NewDefaultPlacer(metastore.New(), cluster)
	if cluster.numBufferFuncs+cluster.numFuncSteps > config.LambdaMaxDeployments {
		cluster.numBufferFuncs = config.LambdaMaxDeployments - cluster.numFuncSteps
	}
	if cluster.numBufferFuncs < 0 {
		cluster.numBufferFuncs = 0
	}
	return cluster
}

func (mw *MovingWindow) Start() error {
	// init bucket
	bucket, err := newBucket(0, mw.group, mw.numFuncSteps+mw.numBufferFuncs)
	if err != nil {
		return err
	}

	// append to bucket list & append current bucket group to proxy group
	mw.buckets = append(mw.buckets, bucket)
	mw.numActives += bucket.len()

	// assign backup node for all nodes of this bucket
	mw.assignBackupLocked(mw.group.SubGroup(bucket.start, bucket.end))

	// Set cursor to latest bucket.
	mw.cursor = bucket

	// start moving-window and auto-scaling Daemon
	go mw.Daemon()

	return nil
}

func (mw *MovingWindow) WaitReady() {
	mw.GetCurrentBucket().waitReady()
}

func (mw *MovingWindow) GetPlacer() metastore.Placer {
	return mw.placer
}

func (mw *MovingWindow) CollectData() {
	for _, gins := range mw.group.all {
		// send data command
		gins.Instance().CollectData()
	}
	mw.log.Info("Waiting data from Lambda")
	global.DataCollected.Wait()
	if err := collector.Flush(); err != nil {
		mw.log.Error("Failed to save data from lambdas: %v", err)
	} else {
		mw.log.Info("Data collected.")
	}
}

func (mw *MovingWindow) Close() {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	select {
	case <-mw.done:
		return
	default:
	}

	close(mw.done)
	for i, gins := range mw.group.all {
		gins.Instance().Close()
		mw.group.all[i] = nil
	}

	pool.Clear(mw.group)
}

// GroupedClusterStatus implementation
func (mw *MovingWindow) ClusterLen() int {
	return len(mw.buckets)
}

func (mw *MovingWindow) ClusterStats(idx int) types.ClusterStats {
	return mw.buckets[idx]
}

func (mw *MovingWindow) AllClustersStats() types.Iterator {
	all := mw.buckets
	return types.NewStatsIterator(all, len(all))
}

func (mw *MovingWindow) ClusterStatsFromIterator(iter types.Iterator) (int, types.ClusterStats) {
	i, val := iter.Value()

	var b *Bucket
	switch item := val.(type) {
	case []*Bucket:
		b = item[i]
	case *Bucket:
		b = item
	}

	return i, b
}

// lambdastore.InstanceManager implementation
func (mw *MovingWindow) Instance(id uint64) *lambdastore.Instance {
	return pool.Instance(id)
}

func (mw *MovingWindow) Recycle(ins types.LambdaDeployment) error {
	pool.Recycle(ins)
	return nil
}

// lambdastore.Relocator implementation
func (mw *MovingWindow) Relocate(meta interface{}, chunkId int, cmd types.Command) (*lambdastore.Instance, error) {
	ins, _, err := mw.placer.Place(meta.(*metastore.Meta), chunkId, cmd)
	return ins, err
}

func (mw *MovingWindow) TryRelocate(meta interface{}, chunkId int, cmd types.Command) (*lambdastore.Instance, bool, error) {
	// Get bucket from group info.
	if gins, ok := pool.InstanceIndex(meta.(*metastore.Meta).Placement[chunkId]); ok {
		bucketId := gins.idx.(*BucketIndex).BucketId
		diffBuckets := mw.GetCurrentBucket().id - bucketId // Call "GetCurrentBucket" once only for better concurrency.
		// Relocate if the chunk has not been touched within active window,
		// or opportunisitcally has not been touched for a while.
		if diffBuckets < config.NumActiveBuckets &&
			(diffBuckets < config.NumActiveBuckets/config.ActiveReplica || mw.rand() != 1) {
			return nil, false, nil
		}
	} // In case !ok, instance can be expired, force relocate.

	// Relocate
	ins, err := mw.Relocate(meta, chunkId, cmd)
	if err != nil {
		return nil, true, err
	}

	return ins, true, nil
}

// metastore.InstanceManger implementation
func (mw *MovingWindow) GetActiveInstances(num int) []*lambdastore.Instance {
	instances := mw.GetCurrentBucket().activeInstances(num)
	if len(instances) >= num {
		return instances
	} else {
		prm := promise.NewPromise()
		mw.Trigger(
			metastore.EventInsufficientStorage,
			&types.ScaleEvent{
				BaseInstance: instances[len(instances)-1],
				ScaleTarget:  num - len(instances),
				Scaled:       prm,
				Reason:       "on demand",
			})
		if err := prm.Error(); err != nil {
			mw.log.Warn("Failed to get at least %d active instances: %v", num, err)
			return instances
		} else {
			// Retry
			return mw.GetActiveInstances(num)
		}
	}
}

func (mw *MovingWindow) GetSlice(size int) metastore.Slice {
	return nil
}

func (mw *MovingWindow) Trigger(event int, args ...interface{}) {
	if event == metastore.EventInsufficientStorage {
		if len(args) == 0 {
			mw.log.Warn("Insufficient parameters for EventInsufficientStorage, 1 expected")
			return
		}
		evt, ok := args[0].(*types.ScaleEvent)
		if !ok {
			mw.log.Warn("Invalid parameters for EventInsufficientStorage, (*lambdastore.Instance) expected")
			return
		}
		// Try not block here: make sure the scaler buffer is large enough
		mw.scaler <- evt
	}
}

func (mw *MovingWindow) Daemon() {
	timer := time.NewTimer(time.Duration(config.BucketDuration) * time.Minute)
	statTimer := time.NewTimer(1 * time.Minute) // I tried 1 second and it failed to respond to scaling. 1 minute is ok.
	for {
		select {
		case <-mw.done:
			if !timer.Stop() {
				<-timer.C
			}
			if !statTimer.Stop() {
				<-statTimer.C
			}
			for len(mw.scaler) > 0 {
				evt := <-mw.scaler
				evt.SetError(ErrClusterClosed)
			}
			return
		// scaling out in bucket
		case evt := <-mw.scaler:
			mw.doScale(evt)
		// for bucket rolling
		case ts := <-timer.C:
			// Rotate. No degradation or expiration if no new bucket is allocated.
			old, inherited, err := mw.Rotate()
			if err != nil {
				mw.log.Error("Failed to initiate new bucket on rotating: %v", err)
				continue
			} else {
				mw.log.Info("Succeeded to rotate the cluster. The latest bucket is %d", mw.getCurrentBucketLocked().id)
			}

			// Degrade instances beyond active window.
			degraded := mw.DegradeCheck()

			// Expire old buckets first to free functions
			expired := mw.ExpireCheck()

			collector.Collect(collector.LogBucketRotate,
				collector.LogTypeBucketRotate, ts.UnixNano(),
				inherited, old.InstanceLen()-inherited, degraded, expired)

			// reset ticker
			timer.Reset(time.Duration(config.BucketDuration) * time.Minute)
		case ts := <-statTimer.C:
			total := pool.NumActives()
			// Log: type, time, total, actives, degraded, expired
			collector.Collect(collector.LogCluster,
				collector.LogTypeCluster, ts.UnixNano(),
				total, mw.numActives, total-mw.numActives, mw.getCurrentBucketLocked().end.Idx()-total)

			// reset ticker
			statTimer.Reset(1 * time.Minute)
		}
	}
}

func (mw *MovingWindow) GetCurrentBucket() *Bucket {
	mw.mu.RLock()
	defer mw.mu.RUnlock()

	return mw.getCurrentBucketLocked()
}

func (mw *MovingWindow) getCurrentBucketLocked() *Bucket {
	return mw.cursor
}

// Bucket degrading
func (mw *MovingWindow) getDegradingInstance() *Bucket {
	if len(mw.buckets) <= config.NumActiveBuckets {
		return nil
	} else {
		return mw.buckets[len(mw.buckets)-config.NumActiveBuckets-1]
	}
}

func (mw *MovingWindow) Rotate() (old *Bucket, inherited int, err error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	// Start new bucket to fill active window.
	old = mw.getCurrentBucketLocked()
	bucket, inherited, err := old.createNextBucket(mw.numFuncSteps)
	if err != nil {
		return old, 0, err
	}

	// update cursor, point to active bucket
	mw.cursor = bucket

	// append to bucket list & append current bucket group to proxy group
	mw.buckets = append(mw.buckets, bucket)
	added := bucket.len() - inherited
	mw.numActives += added
	if added > 0 {
		mw.assignBackupLocked(mw.group.SubGroup(DefaultGroupIndex(bucket.end.Idx()-added), bucket.end))
	}

	return old, inherited, nil
}

func (mw *MovingWindow) degrade(bucket *Bucket) int {
	instances := bucket.getInstances()
	for _, ins := range instances {
		ins.Degrade()
	}
	mw.numActives -= len(instances)
	bucket.state = BUCKET_COLD
	bucket.log.Info("Degraded")

	return len(instances)
}

func (mw *MovingWindow) DegradeCheck() int {
	degradeBucket := mw.getDegradingInstance()
	if degradeBucket != nil {
		return mw.degrade(degradeBucket)
	}
	return 0
}

func (mw *MovingWindow) expire(bucket *Bucket) int {
	// Expire instances
	instances := bucket.getInstances()
	for _, ins := range instances {
		ins.Expire()
	}
	return len(instances)
}

func (mw *MovingWindow) ExpireCheck() int {
	if len(mw.buckets) <= config.NumAvailableBuckets {
		return 0
	}

	expiringBuckets := mw.buckets[:len(mw.buckets)-config.NumAvailableBuckets]
	numExpiredInstances := 0
	for _, bucket := range expiringBuckets {
		if bucket.state == BUCKET_EXPIRE {
			continue
		}
		expired := mw.expire(bucket)
		bucket.state = BUCKET_EXPIRE
		numExpiredInstances += expired
		bucket.log.Info("Expired %d instances", expired)
	}
	mw.log.Debug("%d in total to expire", numExpiredInstances)

	// Update buckets: leave one expired bucket
	if len(mw.buckets) > config.NumAvailableBuckets+1 {
		copy(mw.buff[:config.NumAvailableBuckets+1], mw.buckets[len(mw.buckets)-config.NumAvailableBuckets-1:])
		mw.buckets, mw.buff = mw.buff[:config.NumAvailableBuckets+1], mw.buckets
	}
	// Notify the group
	mw.group.Expire(numExpiredInstances)
	return numExpiredInstances
}

// func (mw *MovingWindow) getActiveInstanceForChunk(meta *metastore.Meta, chunkId int) *lambdastore.Instance {
// 	instances := mw.GetActiveInstances(meta.NumChunks)
// 	return instances[chunkId%len(instances)]
// }

func (mw *MovingWindow) rand() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) // [0,2) random
}

// only assign backup for new node in bucket
func (mw *MovingWindow) assignBackupLocked(gall []*GroupInstance) {
	// When current bucket leaves active window, there backups should not have been expired.
	bucketRange := config.NumAvailableBuckets
	if bucketRange > len(mw.buckets) {
		bucketRange = len(mw.buckets)
	}
	startBucket := mw.buckets[len(mw.buckets)-bucketRange]

	all := mw.group.SubGroup(startBucket.start, mw.buckets[len(mw.buckets)-1].end)
	for _, gins := range gall {
		num, candidates := mw.getBackupsForNode(all, gins.Idx()-startBucket.start.Idx())
		node := gins.Instance()
		node.AssignBackups(num, candidates)
	}
}

func (mw *MovingWindow) doScale(evt *types.ScaleEvent) {
	ins := evt.BaseInstance.(*lambdastore.Instance)
	// Test
	// It is safe to call getCurrentBucketLocked() because the only place that may change buckets are in Daemon,
	// which is the same place that can call doScale() and is exclusive.
	gins, ok := pool.InstanceIndex(ins.Id())
	if !ok {
		evt.SetError(ErrInvalidInstance)
		return
	} else if !mw.testScaledLocked(gins, mw.getCurrentBucketLocked(), evt.Retire) {
		evt.SetScaled()
		return
	}

	mw.mu.Lock()
	defer mw.mu.Unlock()

	// Test again
	bucket := mw.getCurrentBucketLocked()
	if !mw.testScaledLocked(gins, bucket, evt.Retire) {
		evt.SetScaled()
		return
	}

	mw.log.Debug("Scaleing...")

	// Scale
	num := mw.numFuncSteps
	if evt.ScaleTarget > mw.numFuncSteps {
		num = ((evt.ScaleTarget-1)/mw.numFuncSteps + 1) * mw.numFuncSteps
	}
	lastLen := bucket.len()
	newGins, err := bucket.scale(num)
	if err != nil {
		mw.log.Error("Failed to scale bucket %d by %d(%d) : %v", bucket.id, num, lastLen, err)
		evt.SetError(err)
		return
	}
	mw.numActives += num

	mw.assignBackupLocked(newGins)
	mw.log.Info("Scaled bucket %d by %d out of %d, trigger: %d, reason: %s", bucket.id, num, lastLen, ins.Id(), evt.Reason)

	// Flag inactive
	if evt.Retire {
		bucket.flagInactive(gins)
	}
	evt.SetScaled()
}

func (mw *MovingWindow) testScaledLocked(gins *GroupInstance, bucket *Bucket, retire bool) bool {
	if gins.idx.(*BucketIndex).BucketId != bucket.id {
		// Bucket is rotated
		return false
	} else if !bucket.shouldScale(gins, mw.numFuncSteps) {
		// Already scaled, flag inactive
		if retire {
			bucket.flagInactive(gins)
		}
		return false
	}

	return true
}

func (mw *MovingWindow) getBackupsForNode(gall []*GroupInstance, i int) (int, []*lambdastore.Instance) {
	numBaks := config.BackupsPerInstance
	available := len(gall)

	numTotal := numBaks * 2
	distance := available / (numTotal + 1) // main + double backup candidates
	if distance == 0 {
		// In case 2 * total >= g.Len()
		distance = 1
		numBaks = util.Ifelse(numBaks >= available, available-1, numBaks).(int) // Use all
		numTotal = util.Ifelse(numTotal >= available, available-1, numTotal).(int)
	}
	candidates := make([]*lambdastore.Instance, numTotal)
	for j := 0; j < numTotal; j++ {
		candidates[j] = gall[(i+j*distance+rand.Int()%distance+1)%available].Instance() // Random to avoid the same backup set.
	}
	// Because only first numBaks backups will be used initially, shuffle to avoid the same backup set.
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return numBaks, candidates
}
