package server

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
	"github.com/wangaoone/LambdaObjectstore/proxy/lambdastore"
)

const (
	INIT_CAPACITY = 10000
)

type PriorityQueue []*lambdastore.Instance

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Size() < pq[j].Size()
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Block = i
	pq[j].Block = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	lambda := x.(*lambdastore.Instance)
	lambda.Block = n
	*pq = append(*pq, lambda)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	lambda := old[n-1]
	old[n-1] = nil // avoid memory leak
	lambda.Block = -1
	*pq = old[0 : n-1]
	return lambda
}

type LruPlacerMeta struct {
	// Object management properties
	visited   bool
	visitedAt int64
	//confirmed    []bool
	//numConfirmed int

	//evicts *Meta // meta has already evicted
	evicts []*Meta // meta has already evicted
	once   *sync.Once
	action MetaDoPostProcess // proxy call back function

	confirmed bool
}

func newLruPlacerMeta(numChunks int) *LruPlacerMeta {
	return &LruPlacerMeta{
		visited:   false,
		visitedAt: 0,
		confirmed: false,
	}

}

func (pm *LruPlacerMeta) postProcess(action MetaDoPostProcess) {
	if pm.once == nil {
		return
	}
	pm.action = action
	pm.once.Do(pm.doPostProcess)
}

func (pm *LruPlacerMeta) doPostProcess() {
	pm.action(pm.evicts)
	pm.evicts = nil
}

type LruPlacer struct {
	log       logger.ILogger
	cache     *lru.Cache
	Queue     PriorityQueue
	metaStore *MetaStore
	group     *Group
	mu        sync.RWMutex
}

func NewLruPlacer(store *MetaStore, group *Group) *LruPlacer {
	cache, _ := lru.New(INIT_CAPACITY)
	lruPlacer := &LruPlacer{
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		cache:     cache,
		metaStore: store,
		group:     group,
	}
	return lruPlacer
}

func (l *LruPlacer) Init() {
	l.Queue = make(PriorityQueue, l.group.size, LambdaMaxDeployments)
	for i := 0; i < l.group.size; i++ {
		l.Queue[i] = l.group.Instance(i)
		l.Queue[i].Block = i
	}
	l.log.Debug("initial queue:")
	l.dump()
}

func (l *LruPlacer) Append(ins *lambdastore.Instance) {
	l.Queue = append(l.Queue, ins)
}

func (l *LruPlacer) AvgSize() int {
	sum := 0
	for i := 0; i < l.group.Len(); i++ {
		sum += int(l.group.Instance(i).Meta.Size())
	}
	return sum / l.group.Len()
}

func (l *LruPlacer) reBalance(meta *Meta, chunk int) *lambdastore.Instance {
	// counter to control whether get balanced
	if int(atomic.LoadInt32(&meta.Balanced)) == meta.NumChunks {
		l.log.Debug("already balanced (existed already)")
		return l.group.Instance(meta.Placement[chunk])
	} else {
		l.log.Debug("in balancer")
		// find best loc in PQ
		bestLoc := l.Queue[0]
		//meta.Placement[chunk] = int(bestLoc.Id())
		//l.group.Instance(int(bestLoc.Id())).Meta.IncreaseSize(meta.ChunkSize)
		atomic.AddInt32(&meta.Balanced, 1)
		// fix the order in PQ
		return bestLoc
	}
}

func (l *LruPlacer) Adapt(lambdaId int) {
	heap.Fix(&l.Queue, l.group.Instance(lambdaId).Block)
	l.dump()
}

func (l *LruPlacer) dump() {
	msg := "[%v:%d]%s"
	for _, lambda := range l.Queue {
		msg = fmt.Sprintf(msg, lambda.Deployment.Name(), lambda.Size(), ",[%v:%d]%s")
	}
	l.log.Debug(msg, 0, 0, "\n")
}

func (l *LruPlacer) NewMeta(key string, sliceSize int, numChunks int, chunkId int, lambdaId int, chunkSize int64) *Meta {
	l.log.Debug("key and chunkId is %v,%v", key, chunkId)
	meta := NewMeta(key, numChunks, chunkSize)
	l.group.InitMeta(meta, sliceSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	//l.TouchObject(meta)
	return meta
}

func (l *LruPlacer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool, MetaPostProcess) {
	if l.AvgSize() > InstanceCapacity*Threshold {
		//TODO:  lambda instances scale out

	}

	chunk := newMeta.lastChunk

	// lambdaId from client
	//lambdaId := newMeta.Placement[chunk]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	//defer meta.mu.Unlock()

	if meta.Deleted {
		meta.placerMeta = nil
		meta.Deleted = false
	}

	if meta.placerMeta == nil {
		meta.placerMeta = newLruPlacerMeta(len(meta.Placement))
	}

	// Check availability
	l.mu.Lock()
	//defer l.mu.Unlock()

	// get re-balanced assigned lambda instance
	instance := l.reBalance(meta, chunk)
	l.log.Debug("selected instance is %v", instance.Name())

	// load balancer, not trigger evict
	if instance.Meta.Size()+uint64(meta.ChunkSize) < instance.Meta.Capacity {
		meta.Placement[chunk] = int(instance.Id())
		l.cache.Add(meta, nil)
		instance.Meta.IncreaseSize(meta.ChunkSize)
		l.Adapt(meta.Placement[chunk])

		//l.log.Debug("return at balancer, Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
		//instance, size, instance.Meta.Capacity, chunk, key, meta.ChunkSize)

		l.mu.Unlock()
		meta.mu.Unlock()
		return meta, got, nil
	}

	l.mu.Unlock()
	meta.mu.Unlock()

	l.log.Debug("need to evict")
	// need to evict oldest object

	l.mu.Lock()
	defer l.mu.Unlock()

	// find next available place & update instance size
	l.NextAvailable(meta)

	// adapt priority queue
	l.Adapt(meta.Placement[chunk])
	l.log.Debug("finish evict obj")
	//instance := l.reBalance(meta, chunk)
	//meta.Placement[chunk] = int(instance.Id())

	meta.placerMeta.once = &sync.Once{}
	return meta, got, meta.placerMeta.postProcess
}

func (l *LruPlacer) TouchObject(meta *Meta) {
	meta.placerMeta.visited = true
	meta.placerMeta.visitedAt = time.Now().Unix()
}

func (l *LruPlacer) NextAvailable(meta *Meta) {
	//if atomic.LoadInt32(&meta.placerMeta.confirmed) == 1 {
	//	l.log.Debug("other goroutine already evicted")
	//	// return existed meta value to proxy
	//	// TODO
	//	return
	//}
	//atomic.AddInt32(&meta.placerMeta.confirmed, 1)

	l.log.Debug("in NextAvailable %v", meta.placerMeta.confirmed)
	// confirm == true, other goroutine already update the placement
	if meta.placerMeta.confirmed == true {
		l.log.Debug("already updated %v", meta.Placement)
		return
	}

	evictList := make([]*Meta, 0, 100)

	for {
		// get oldest meta
		evict, _, _ := l.cache.RemoveOldest()
		l.log.Debug("evict object is %v", evict.(*Meta).Key)
		evictList = append(evictList, evict.(*Meta))

		if meta.ChunkSize <= evict.(*Meta).ChunkSize {
			// meta size smaller than evict size, enough
			l.log.Debug("meta smaller or equal than evict")
			break
		}
		//for i := 0; i < len(evict.(*Meta).Placement); i++ {
		//	delta := meta.ChunkSize - evictSize
		//	if l.group.Instance(evict.(*Meta).Placement[i]).IncreaseSize(delta) < InstanceCapacity*Threshold {
		//		meta.placerMeta.numConfirmed += 1
		//	} else {
		//
		//	}
		//}
		//if meta.placerMeta.numConfirmed == meta.NumChunks {
		//	enough = true
		//}
	}
	l.log.Debug("after infinite loop")
	//l.log.Debug("evict list is %v", l.dumpList(evictList))

	// copy last evict obj placement to meta
	meta.Placement = evictList[len(evictList)-1].Placement
	l.log.Debug("meta placement updated %v", meta.Placement)
	// update instance size
	l.cache.Add(meta, nil)
	l.updateInstanceSize(evictList, meta)

	meta.placerMeta.confirmed = true

	meta.placerMeta.evicts = evictList
}

func (l *LruPlacer) Get(key string, chunk int) (*Meta, bool) {
	meta, ok := l.metaStore.Get(key)
	if !ok {
		return nil, ok
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	if meta.Deleted {
		return meta, ok
	}
	// Ensure availability
	l.mu.Lock()
	defer l.mu.Unlock()

	// Object may be evicted just before locking.
	if meta.Deleted {
		return meta, ok
	}
	l.TouchObject(meta)
	return meta, ok
}

func (l *LruPlacer) dumpList(list []*Meta) string {
	res := ""
	for _, i2 := range list {
		res = fmt.Sprintf("%s,%s", res, i2.Key)
	}
	return res
}

func (l *LruPlacer) updateInstanceSize(evictList []*Meta, m *Meta) {
	last := evictList[len(evictList)-1]
	l.log.Debug("last in evict list is %v", last.Key)
	for i := 0; i < len(last.Placement); i++ {
		l.group.Instance(last.Placement[i]).Meta.IncreaseSize(m.ChunkSize - last.ChunkSize)
	}
	l.log.Debug("len of evict list is", len(evictList))
	if len(evictList) > 1 {
		l.log.Debug("evict list length larger than 1")
		for j := 0; j < len(evictList)-1; j++ {
			e := evictList[j]
			//delta := m.ChunkSize - e.ChunkSize
			for i := 0; i < len(e.Placement); i++ {
				l.group.Instance(e.Placement[i]).Meta.DecreaseSize(e.ChunkSize)
			}
		}
		return
	}

	return
}
