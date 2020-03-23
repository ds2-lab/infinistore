package server

import (
	"container/heap"
	"fmt"
	"sync"
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
	numConfirmed int

	evicts *Meta // meta has already evicted
	once   *sync.Once
	action MetaDoPostProcess // proxy call back function
}

func newLruPlacerMeta(numChunks int) *LruPlacerMeta {
	return &LruPlacerMeta{
		//confirmed: make([]bool, numChunks),
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
	l.Queue = make(PriorityQueue, l.group.size)
	for i := 0; i < l.group.size; i++ {
		l.Queue[i] = l.group.Instance(i)
		l.Queue[i].Block = i
	}
	l.log.Debug("initial queue:")
	l.dump()
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
	if meta.Balanced == meta.NumChunks {
		l.log.Debug("already balanced (existed already)")
		return l.group.Instance(meta.Placement[chunk])
	} else {
		// find best loc in PQ
		bestLoc := l.Queue[0]
		//meta.Placement[chunk] = int(bestLoc.Id())
		//l.group.Instance(int(bestLoc.Id())).Meta.IncreaseSize(meta.ChunkSize)
		meta.Balanced += 1
		// fix the order in PQ
		l.Adapt(meta.Placement[chunk])
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
	meta := NewMeta(key, numChunks, chunkSize)
	l.group.InitMeta(meta, sliceSize)
	meta.Placement[chunkId] = lambdaId
	meta.lastChunk = chunkId
	l.TouchObject(meta)
	return meta
}

func (l *LruPlacer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool, MetaPostProcess) {
	chunk := newMeta.lastChunk

	// lambdaId from client
	//lambdaId := newMeta.Placement[chunk]

	meta, got, _ := l.metaStore.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}

	meta.mu.Lock()
	defer meta.mu.Unlock()

	if meta.Deleted {
		meta.placerMeta = nil
		meta.Deleted = false
	}

	if meta.placerMeta == nil {
		meta.placerMeta = newLruPlacerMeta(len(meta.Placement))
	}

	//if l.AvgSize() < InstanceCapacity*Threshold {

	// get re-balanced assigned lambda instance
	instance := l.reBalance(meta, chunk)

	if instance.Meta.Size()+uint64(meta.ChunkSize) < instance.Meta.Capacity {
		meta.Placement[chunk] = int(instance.Id())
		l.cache.Add(meta, nil)
		size := instance.Meta.IncreaseSize(meta.ChunkSize)
		l.log.Debug("return at balancer, Lambda %d size updated: %d of %d (key:%d@%s, Δ:%d).",
			instance, size, instance.Meta.Capacity, chunk, key, meta.ChunkSize)
		return meta, got, nil
	}

	//}

	// need to evict oldest object
	//evictList := l.NextAvailable(meta)
	//
	//instance := l.reBalance(meta, chunk)
	//meta.Placement[chunk] = int(instance.Id())
	//return meta, got, meta.placerMeta.postProcess
	return meta, got, nil
}

func (l *LruPlacer) TouchObject(meta *Meta) {
	meta.placerMeta.visited = true
	meta.placerMeta.visitedAt = time.Now().Unix()
}

func (l *LruPlacer) NextAvailable(meta *Meta) []*Meta {
	enough := false
	evictList := make([]*Meta, 1, 100)

	for !enough {

		// get oldest meta
		evict, _, _ := l.cache.RemoveOldest()
		evictList = append(evictList, evict.(*Meta))
		evictSize := evict.(*Meta).ChunkSize

		for i := 0; i < len(evict.(*Meta).Placement); i++ {
			delta := meta.ChunkSize - evictSize
			if l.group.Instance(evict.(*Meta).Placement[i]).IncreaseSize(delta) > l.group.Instance(evict.(*Meta).Placement[i]).Capacity {
				//
			} else {
				meta.placerMeta.numConfirmed += 1
			}
		}
		if meta.placerMeta.numConfirmed == meta.NumChunks {
			enough = true
		}
	}
	return evictList
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
