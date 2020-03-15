package server

import (
	"container/heap"
	"fmt"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
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

type Balancer struct {
	log    logger.ILogger
	Placer *Placer
	Queue  PriorityQueue
}

func NewBalancer(store *MetaStore, group *Group) *Balancer {
	balancer := &Balancer{
		log: &logger.ColorLogger{
			Prefix: "Balancer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		Placer: NewPlacer(store, group),
	}
	return balancer
}

func (b *Balancer) Init() {
	b.Queue = make(PriorityQueue, b.Placer.group.size)
	for i := 0; i < b.Placer.group.size; i++ {
		b.Queue[i] = b.Placer.group.Instance(i)
		b.Queue[i].Block = i
	}
}

func (b *Balancer) GetOrInsert(key string, newMeta *Meta) *Meta {
	chunk := newMeta.lastChunk
	lambdaId := newMeta.Placement[chunk]

	meta, got, _ := b.Placer.store.GetOrInsert(key, newMeta)
	if got {
		newMeta.close()
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()

	b.Placer.mu.Lock()
	defer b.Placer.mu.Unlock()

	if b.Queue[chunk].Size() < b.Queue[lambdaId].Size() {
		meta.Placement[chunk] = int(b.Queue[chunk].Id())
	}

	b.log.Debug("placement is %v", meta.Placement)
	return meta
}

func (b *Balancer) Adapt(lambdaId int) {
	heap.Fix(&b.Queue, b.Placer.group.Instance(lambdaId).Block)
	// b.dump()
}

func (b *Balancer) dump() {
	msg := "[%d:%d]%s"
	for _, lambda := range b.Queue {
		msg = fmt.Sprintf(msg, lambda.Id, lambda.Size(), ",[%d:%d]%s")
	}
	b.log.Debug(msg, 0, 0, "\n")
}
