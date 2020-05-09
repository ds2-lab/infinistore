package server

import (
	"math/rand"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	ActiveDuration     = 3 // min
	ExpireDuration     = 6 //min
	ActiveBucketsNum   = ActiveDuration / config.BucketDuration
	ExpireBucketsNum   = ExpireDuration / config.BucketDuration
	NumBackupInstances = config.BackupsPerInstance * config.NumLambdaClusters
)

// reuse window and interval should be MINUTES
type MovingWindow struct {
	log    logger.ILogger
	placer *Placer
	group  *Group

	window   int
	interval int
	num      int // number of hot bucket 1 hour time window = 6 num * 10 min
	buckets  []*Bucket

	cursor    *Bucket
	startTime time.Time

	scaler       chan struct{}
	scaleCounter int32

	mu sync.Mutex
}

func (mw *MovingWindow) Instance(id uint64) (*lambdastore.Instance, bool) {
	got, exists := scheduler.actives.Get(id)
	if !exists {
		return nil, exists
	}

	ins := got.(*GroupInstance)
	validated := ins.group.Validate(ins)
	if validated != ins {
		// Switch keys
		scheduler.actives.Set(validated.Id(), validated)
		scheduler.actives.Set(ins.Id(), ins)
		// Recycle ins
		scheduler.Recycle(ins.LambdaDeployment)
	}
	return validated.LambdaDeployment.(*lambdastore.Instance), exists
}

func (mw *MovingWindow) Reroute(obj interface{}, chunkId int) *lambdastore.Instance {
	return mw.getActiveInstanceForChunk(obj.(*Meta), chunkId)
}

func NewMovingWindow(window int, interval int) *MovingWindow {
	group := NewGroup(config.NumLambdaClusters)
	return &MovingWindow{
		log: &logger.ColorLogger{
			Prefix: "Moving window ",
			Level:  global.Log.GetLevel(),
			Color:  !global.Options.NoColor,
		},
		group:     group,
		num:       ActiveBucketsNum,
		window:    window,
		interval:  interval,
		buckets:   make([]*Bucket, 0, 500),
		startTime: time.Now(),

		// for scaling out
		scaler:       make(chan struct{}, 1),
		scaleCounter: 0,
	}
}

func (mw *MovingWindow) waitReady() {
	mw.getCurrentBucket().waitReady()
}

func (mw *MovingWindow) Len() int {
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

func (mw *MovingWindow) findBucket(expireCount int) *Bucket {
	old := mw.getCurrentBucket().id - expireCount
	if old < 0 {
		return mw.buckets[0]
	}
	return mw.buckets[old]

}

func (mw *MovingWindow) start() {
	// init bucket
	bucket, _ := newBucket(0, mw.group, config.NumLambdaClusters, "start")
	for i := 0; i < len(mw.group.All); i++ {
		mw.log.Debug("instance is %v", mw.group.All[i].Name())
	}

	// append to bucket list & append current bucket group to proxy group
	mw.buckets = append(mw.buckets, bucket)

	// assign backup node for all nodes of this bucket
	mw.assignBackup(bucket.activeInstances(config.NumLambdaClusters))

	mw.placer.instances = mw.activeInstances(config.NumLambdaClusters)

}

func (mw *MovingWindow) Daemon() {
	idx := 1
	ticker := time.NewTicker(time.Duration(config.BucketDuration) * time.Minute)
	for {
		select {
		// scaling out in bucket
		case <-mw.scaler:

			bucket := mw.getCurrentBucket()
			bucket.scale(config.NumLambdaClusters)
			mw.assignBackup(bucket.activeInstances(config.NumLambdaClusters))

			//scale out phase finished
			mw.placer.scaling = false

			mw.placer.mu.Lock()
			mw.placer.instances = mw.activeInstances(config.NumLambdaClusters)
			mw.placer.mu.Unlock()

			mw.log.Debug("scale out finish")

		// for bucket rolling
		case <-ticker.C:
			//TODO: generate new fake bucket. use the same pointer as last bucket
			//currentBucket := mw.getCurrentBucket()
			//if mw.avgSize(currentBucket) < 1000 {
			//	break
			//}

			bucket, _ := newBucket(idx, mw.group, config.NumLambdaClusters)

			// append to bucket list & append current bucket group to proxy group
			mw.buckets = append(mw.buckets, bucket)
			mw.assignBackup(bucket.activeInstances(config.NumLambdaClusters))

			mw.placer.mu.Lock()
			mw.placer.instances = mw.activeInstances(config.NumLambdaClusters)
			mw.placer.mu.Unlock()

			//check degrade and expire
			mw.DegradeCheck()
			mw.ExpireCheck()

			// update cursor, point to active bucket
			mw.cursor = bucket
			mw.log.Debug("rolling finished, bucket is %v", bucket.id)
			idx += 1

			// reset ticker
			ticker = time.NewTicker(time.Duration(config.BucketDuration) * time.Minute)
		}
	}
}

func (mw *MovingWindow) getAllBuckets() []*Bucket {
	return mw.buckets
}

func (mw *MovingWindow) getCurrentBucket() *Bucket {
	return mw.buckets[len(mw.buckets)-1]
}

func (mw *MovingWindow) getInstanceId(id int, from int) int {
	//idx := mw.getCurrentBucket().from + id
	idx := id + from
	return idx
}

func (mw *MovingWindow) activeInstances(num int) []*GroupInstance {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	return mw.getCurrentBucket().activeInstances(num)
}

func (mw *MovingWindow) avgSize(bucket *Bucket) int {
	sum := 0
	start := bucket.start
	end := bucket.end

	for i := start; i < end; i++ {
		sum += int(mw.group.Instance(int(i)).Meta.Size())
	}

	return sum / (end - start + 1)
}

// Bucket degrading
func (mw *MovingWindow) getDegradingInstanceLocked() *Bucket {
	if len(mw.buckets) <= ActiveBucketsNum {
		return nil
	} else {
		return mw.buckets[len(mw.buckets)-ActiveBucketsNum-1]
	}
}

func (mw *MovingWindow) degrade(bucket *Bucket) {
	for _, ins := range bucket.instances {
		ins.LambdaDeployment.(*lambdastore.Instance).Degrade()
	}
}

func (mw *MovingWindow) DegradeCheck() {
	degradeBucket := mw.getDegradingInstanceLocked()
	if degradeBucket != nil {
		mw.degrade(degradeBucket)
		degradeBucket.state = BUCKET_COLD
	}
}

// Bucket expiration
func (mw *MovingWindow) getExpiringInstanceLocked() *Bucket {
	if len(mw.buckets) <= ExpireBucketsNum {
		return nil
	} else {
		return mw.buckets[len(mw.buckets)-ExpireBucketsNum-1]
	}
}

func (mw *MovingWindow) expire(bucket *Bucket) {
	for _, ins := range bucket.instances {
		ins.LambdaDeployment.(*lambdastore.Instance).Expire()
	}
}

func (mw *MovingWindow) ExpireCheck() {
	expireBucket := mw.getExpiringInstanceLocked()
	if expireBucket != nil {
		mw.expire(expireBucket)
		expireBucket.state = BUCKET_EXPIRE
	}
}

func (mw *MovingWindow) getActiveInstanceForChunk(obj *Meta, chunkId int) *lambdastore.Instance {
	instances := mw.activeInstances(obj.NumChunks)
	return instances[chunkId].LambdaDeployment.(*lambdastore.Instance)
}

func (mw *MovingWindow) Refresh(obj *Meta, chunkId int) (*lambdastore.Instance, bool) {
	oldBucketId := obj.placerMeta.bucketIdx
	if mw.getCurrentBucket().id-oldBucketId >= ActiveBucketsNum {
		instance := mw.getActiveInstanceForChunk(obj, chunkId)
		return instance, true
	} else if mw.rand() == 1 && mw.getCurrentBucket().id-oldBucketId >= (ActiveBucketsNum/config.ActiveReplica) {
		instance := mw.getActiveInstanceForChunk(obj, chunkId)
		return instance, true
	}

	return nil, false
}

func (mw *MovingWindow) rand() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(2) // [0,2) random
}

func (mw *MovingWindow) findBackupInstanceStart() int {
	if len(mw.group.All) < NumBackupInstances {
		return 0
	} else {
		return len(mw.group.All) - NumBackupInstances
	}

}

// only assign backup for new node in bucket
func (mw *MovingWindow) assignBackup(instances []*GroupInstance) {
	start := mw.findBackupInstanceStart()
	for i := 0; i < len(instances); i++ {
		num, candidates := scheduler.getBackupsForNode(mw.group.All[start:], i)
		node := instances[i].LambdaDeployment.(*lambdastore.Instance)
		mw.log.Debug("in assign backup, instance is %v", node.Name())
		node.AssignBackups(num, candidates)
	}
}

func (mw *MovingWindow) touch(meta *Meta) {
	//mw.log.Debug("in touch %v", meta.Placement)
	//// brand new meta(-1) or already existed
	//if meta.placerMeta.bucketIdx == -1 {
	//	mw.cursor.m.Set(meta.Key, meta)
	//} else {
	//	// remove meta from previous bucket
	//	oldBucket := meta.placerMeta.bucketIdx
	//	if mw.cursor == mw.buckets[oldBucket] {
	//		return
	//	} else {
	//		mw.buckets[oldBucket].m.Del(meta.Key)
	//		mw.cursor.m.Set(meta.Key, meta)
	//	}
	//}
	//
	//meta.placerMeta.bucketIdx = mw.cursor.id
}
