package cluster

import (
	"math/rand"
	"sync"
	"time"

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
	NumInitialClusters = config.NumLambdaClusters * 2
)

func init() {
	if NumInitialClusters > config.LambdaMaxDeployments {
		NumInitialClusters = config.LambdaMaxDeployments
	}
}

// reuse window and interval should be MINUTES
type MovingWindow struct {
	log    logger.ILogger
	placer metastore.Placer
	group  *Group

	window   int
	interval int
	num      int // number of hot bucket 1 hour time window = 6 num * 10 min
	buckets  []*Bucket
	buff     []*Bucket

	cursor    *Bucket
	startTime time.Time

	scaler chan *lambdastore.Instance
	// scaleCounter int32

	mu   sync.RWMutex
	done chan struct{}

	numActives int
}

func NewMovingWindow(window int, interval int) *MovingWindow {
	cluster := &MovingWindow{
		log:       global.GetLogger("MovingWindow: "),
		group:     NewGroup(0),
		num:       config.NumActiveBuckets,
		window:    window,
		interval:  interval,
		buckets:   make([]*Bucket, 0, config.NumAvailableBuckets+2), // Reserve space for new bucket and last expired bucket
		buff:      make([]*Bucket, 0, config.NumAvailableBuckets+2),
		startTime: time.Now(),

		// for scaling out
		scaler: make(chan *lambdastore.Instance, config.NumLambdaClusters),
		// scaleCounter: 0,

		done: make(chan struct{}),
	}
	cluster.placer = metastore.NewDefaultPlacer(metastore.New(), cluster)
	return cluster
}

func (mw *MovingWindow) Start() error {
	// init bucket
	bucket, err := newBucket(0, mw.group, NumInitialClusters)
	if err != nil {
		return err
	}

	// append to bucket list & append current bucket group to proxy group
	mw.buckets = append(mw.buckets, bucket)
	mw.numActives += len(bucket.instances)

	// assign backup node for all nodes of this bucket
	mw.assignBackupLocked(mw.group.SubGroup(bucket.start, bucket.end), bucket)

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

func (mw *MovingWindow) Len() int {
	return len(mw.buckets)
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
func (mw *MovingWindow) Instance(id uint64) (*lambdastore.Instance, bool) {
	return pool.Instance(id)
}

func (mw *MovingWindow) Relocate(meta interface{}, chunkId int) *lambdastore.Instance {
	return mw.getActiveInstanceForChunk(meta.(*metastore.Meta), chunkId)
}

func (mw *MovingWindow) TryRelocate(meta interface{}, chunkId int) (*lambdastore.Instance, bool) {
	gins, ok := pool.InstanceIndex(uint64(meta.(*metastore.Meta).Placement[chunkId]))
	if !ok {
		// instance can be expired
		return mw.Relocate(meta, chunkId), true
	}

	bucketId := gins.idx.(*BucketIndex).BucketId
	diffBuckets := mw.GetCurrentBucket().id - bucketId // Call "GetCurrentBucket" once only for better concurrency.
	// Relocate if the chunk has not been touched within active window,
	// or opportunisitcally has not been touched for a while.
	if diffBuckets >= config.NumActiveBuckets {
		return mw.Relocate(meta, chunkId), true
	} else if mw.rand() == 1 && diffBuckets >= config.NumActiveBuckets/config.ActiveReplica {
		return mw.Relocate(meta, chunkId), true
	}

	return nil, false
}

func (mw *MovingWindow) Recycle(ins types.LambdaDeployment) error {
	pool.Recycle(ins)
	return nil
}

// metastore.InstanceManger implementation
func (mw *MovingWindow) GetActiveInstances(num int) []*lambdastore.Instance {
	return mw.GetCurrentBucket().activeInstances(num)
}

func (mw *MovingWindow) Trigger(event int, args ...interface{}) {
	if event == metastore.EventInsufficientStorage {
		if len(args) == 0 {
			mw.log.Warn("Insufficient parameters for EventInsufficientStorage, 1 expected")
			return
		}
		ins, ok := args[0].(*lambdastore.Instance)
		if !ok {
			mw.log.Warn("Invalid parameters for EventInsufficientStorage, (*lambdastore.Instance) expected")
			return
		}
		// Will not block here
		select {
		case mw.scaler <- ins:
		default:
		}
	}
}

func (mw *MovingWindow) Daemon() {
	idx := 1
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
			return
		// scaling out in bucket
		case ins := <-mw.scaler:
			mw.doScale(ins)
		// for bucket rolling
		case <-timer.C:
			// TODO: Try migrate active instance to new bucket
			//currentBucket := mw.GetCurrentBucket()
			//if mw.avgSize(currentBucket) < 1000 {
			//	break
			//}
			mw.mu.Lock()

			// Start new bucket to fill active window.
			bucket, err := newBucket(idx, mw.group, config.NumLambdaClusters)
			if err != nil {
				mw.log.Error("Failed to initate new bucket: %v", err)
				continue // No degradation or expiration if no new bucket is allocated.
			} else {
				// append to bucket list & append current bucket group to proxy group
				mw.buckets = append(mw.buckets, bucket)
				mw.numActives += len(bucket.instances)
				mw.assignBackupLocked(mw.group.SubGroup(bucket.start, bucket.end), bucket)

				// update cursor, point to active bucket
				mw.cursor = bucket
				mw.log.Debug("Rotation finished, latest bucket is %d", bucket.id)
				idx++
			}

			// Degrade instances beyond active window.
			mw.DegradeCheck()

			// Expire old buckets first to free functions
			mw.ExpireCheck()

			mw.mu.Unlock()

			// reset ticker
			timer.Reset(time.Duration(config.BucketDuration) * time.Minute)
		case ts := <-statTimer.C:
			total := pool.NumActives()
			collector.Collect(collector.LogCluster, "cluster", ts.UnixNano(), total, mw.numActives, total-mw.numActives)

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
	return mw.buckets[len(mw.buckets)-1]
}

// Bucket degrading
func (mw *MovingWindow) getDegradingInstanceLocked() *Bucket {
	if len(mw.buckets) <= config.NumActiveBuckets {
		return nil
	} else {
		return mw.buckets[len(mw.buckets)-config.NumActiveBuckets-1]
	}
}

func (mw *MovingWindow) degrade(bucket *Bucket) {
	for _, ins := range bucket.instances {
		ins.Degrade()
	}
	mw.numActives -= len(bucket.instances)
	bucket.state = BUCKET_COLD
	bucket.log.Debug("Degraded")
}

func (mw *MovingWindow) DegradeCheck() {
	degradeBucket := mw.getDegradingInstanceLocked()
	if degradeBucket != nil {
		mw.degrade(degradeBucket)
	}
}

func (mw *MovingWindow) expire(bucket *Bucket) {
	// Expire instances
	for _, ins := range bucket.instances {
		ins.Expire()
	}
}

func (mw *MovingWindow) ExpireCheck() {
	if len(mw.buckets) <= config.NumAvailableBuckets {
		return
	}

	expiringBuckets := mw.buckets[:len(mw.buckets)-config.NumAvailableBuckets]
	numExpiringInstances := 0
	for _, bucket := range expiringBuckets {
		if bucket.state == BUCKET_EXPIRE {
			continue
		}
		mw.expire(bucket)
		bucket.state = BUCKET_EXPIRE
		numExpiringInstances += len(bucket.instances)
		bucket.log.Debug("Expired %d instances", len(bucket.instances))
		mw.log.Debug("%d in total to expire", numExpiringInstances)
	}

	// Update buckets: leave one expired bucket
	if len(mw.buckets) > config.NumAvailableBuckets+1 {
		copy(mw.buff[:config.NumAvailableBuckets+1], mw.buckets[len(mw.buckets)-config.NumAvailableBuckets-1:])
		mw.buckets, mw.buff = mw.buff[:config.NumAvailableBuckets+1], mw.buckets
	}
	// Notify the group
	mw.group.Expire(numExpiringInstances)
}

func (mw *MovingWindow) getActiveInstanceForChunk(meta *metastore.Meta, chunkId int) *lambdastore.Instance {
	instances := mw.GetActiveInstances(meta.NumChunks)
	return instances[chunkId%len(instances)]
}

func (mw *MovingWindow) rand() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) // [0,2) random
}

// only assign backup for new node in bucket
func (mw *MovingWindow) assignBackupLocked(gall []*GroupInstance, current *Bucket) {
	// When current bucket leaves active window, there backups should not have been expired.
	bucketRange := config.NumAvailableBuckets
	if bucketRange > len(mw.buckets) {
		bucketRange = len(mw.buckets)
	}
	startBucket := mw.buckets[len(mw.buckets)-bucketRange]

	all := mw.group.SubGroup(startBucket.start, current.end)
	for _, gins := range gall {
		num, candidates := mw.getBackupsForNode(all, gins.Idx()-startBucket.start.Idx())
		node := gins.Instance()
		node.AssignBackups(num, candidates)
	}
}

func (mw *MovingWindow) InstanceSum(stats int) int {
	sum := 0
	for _, bucket := range mw.buckets {
		if bucket.state == stats {
			sum += len(bucket.instances)
		}
	}
	return sum
}

// func (mw *MovingWindow) Touch(meta *metastore.Meta) {
// 	mw.log.Debug("in touch %v", meta.Placement)
// 	// brand new meta(-1) or already existed
// 	if meta.placerMeta.bucketIdx == -1 {
// 		mw.cursor.m.Set(meta.Key, meta)
// 	} else {
// 		// remove meta from previous bucket
// 		oldBucket := meta.placerMeta.bucketIdx
// 		if mw.cursor == mw.buckets[oldBucket] {
// 			return
// 		} else {
// 			mw.buckets[oldBucket].m.Del(meta.Key)
// 			mw.cursor.m.Set(meta.Key, meta)
// 		}
// 	}
//
// 	meta.placerMeta.bucketIdx = mw.cursor.id
// }

func (mw *MovingWindow) doScale(ins *lambdastore.Instance) {
	// Test
	// It is safe to call getCurrentBucketLocked() because the only place that may change buckets are in Daemon,
	// which is the same place that can call doScale() and is exclusive.
	gins, ok := pool.InstanceIndex(ins.Id())
	if !ok || !mw.testScaledLocked(gins, mw.getCurrentBucketLocked()) {
		return
	}

	mw.mu.Lock()
	defer mw.mu.Unlock()

	// Test again
	bucket := mw.getCurrentBucketLocked()
	if !mw.testScaledLocked(gins, bucket) {
		return
	}

	mw.log.Debug("Scaleing...")

	// Scale
	newGins, err := bucket.scale(config.NumLambdaClusters)
	if err != nil {
		mw.log.Error("Failed to scale: %v", err)
		return
	}
	mw.numActives += config.NumLambdaClusters

	mw.assignBackupLocked(newGins, bucket)
	mw.log.Debug("Scaled")

	// Flag inactive
	bucket.flagInactive(gins)
}

func (mw *MovingWindow) testScaledLocked(gins *GroupInstance, bucket *Bucket) bool {
	if gins.idx.(*BucketIndex).BucketId != bucket.id {
		// Bucket is rotated
		return false
	} else if !bucket.shouldScale(gins, config.NumLambdaClusters) {
		// Already scaled, flag inactive
		bucket.flagInactive(gins)
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
	return numBaks, candidates
}
