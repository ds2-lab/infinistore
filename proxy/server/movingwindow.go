package server

import (
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

var (
	bucketPerWindow  = 6
	activeHour       = 2
	activeNumBuckets = bucketPerWindow * activeHour
	NumBackupBuckets = 3 * 6
	expireHour       = 3
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
			Color:  true,
		},
		group:     group,
		num:       activeNumBuckets,
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

// only assign backup for new node in bucket
func (mw *MovingWindow) assignBackup(instances []*GroupInstance) {
	// get 3 hour buckets
	start := mw.findBucket(NumBackupBuckets).start
	for i := 0; i < len(instances); i++ {
		num, candidates := scheduler.getBackupsForNode(mw.group.All[start:], i)
		//node := mw.group.Instance(i)
		node := instances[i].LambdaDeployment.(*lambdastore.Instance)
		mw.log.Debug("instance is %v", node.Name())
		node.AssignBackups(num, candidates)
	}
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

}

func (mw *MovingWindow) Daemon() {
	idx := 1
	for {
		//ticker := time.NewTicker(time.Duration(mw.interval) * time.Minute)
		ticker := time.NewTicker(30 * time.Second)
		select {
		// scaling out in bucket
		case <-mw.scaler:

			bucket := mw.getCurrentBucket()
			bucket.scale(config.NumLambdaClusters)
			mw.assignBackup(bucket.activeInstances(config.NumLambdaClusters))

			//scale out phase finished
			mw.placer.scaling = false
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

			degrade := mw.getDegradingInstanceLocked()
			if degrade != nil {
				mw.degrade(degrade)
			}

			// update cursor, point to active bucket
			mw.cursor = bucket

		}
		idx += 1
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

func (mw *MovingWindow) getDegradingInstanceLocked() *Bucket {
	if len(mw.buckets) <= activeNumBuckets {
		return nil
	} else {
		return mw.buckets[len(mw.buckets)-activeNumBuckets-1]
	}
}

func (mw *MovingWindow) degrade(bucket *Bucket) {
	for _, ins := range bucket.instances {
		ins.LambdaDeployment.(*lambdastore.Instance).Degrade()
	}
}

func (mw *MovingWindow) getActiveInstanceForChunk(obj *Meta, chunkId int) *lambdastore.Instance {
	instances := mw.activeInstances(obj.NumChunks)
	return instances[chunkId].LambdaDeployment.(*lambdastore.Instance)
}

func (mw *MovingWindow) CanRefresh(obj *Meta, chunkId int) bool {
	return true
}
