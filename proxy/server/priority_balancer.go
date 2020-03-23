package server

import (
	"container/heap"
	"fmt"

	"github.com/wangaoone/LambdaObjectstore/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

type Balancer struct {
	log          logger.ILogger
	balanceStore *MetaStore
	Queue        PriorityQueue
	Placer       *Placer
}

func NewBalancer(store *MetaStore, group *Group) *Balancer {
	balancer := &Balancer{
		log: &logger.ColorLogger{
			Prefix: "Balancer ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		balanceStore: store,
		Placer:       NewPlacer(NewMataStore(), group),
	}
	return balancer
}

func (b *Balancer) Init() {
	b.Queue = make(PriorityQueue, b.Placer.group.size)
	for i := 0; i < b.Placer.group.size; i++ {
		b.Queue[i] = b.Placer.group.Instance(i)
		b.Queue[i].Block = i
	}
	b.log.Debug("initial queue:")
	b.dump()
}

func (b *Balancer) GetOrInsert(key string, newMeta *Meta) (*Meta, bool, MetaPostProcess) {
	chunk := newMeta.lastChunk
	//lambdaId := newMeta.Placement[chunk]
	chunkKey := fmt.Sprintf("%d@%s", chunk, key)

	meta, got, _ := b.balanceStore.GetOrInsert(chunkKey, newMeta)
	if got {
		newMeta.close()
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()

	//if b.Queue[chunk].Size() < b.Queue[lambdaId].Size() {
	//	b.log.Debug("find better place, better is %v, before is %v",
	//		b.Queue[chunk].Deployment.Name(), b.Queue[lambdaId].Deployment.Name())
	//
	//	meta.Placement[chunk] = int(b.Queue[chunk].Id())
	//} else {
	//	b.log.Debug("keep original placement")
	//	meta.Placement[chunk] = newMeta.Placement[chunk]
	//}

	if !meta.Balanced {
		meta.Balanced = true
		// get first lambda instance in pq
		reDirect := b.Queue[0]
		meta.Placement[chunk] = int(reDirect.Id())
		b.Placer.group.Instance(int(reDirect.Id())).IncreaseSize(meta.ChunkSize)

		// fix the pq order
		b.Adapt(meta.Placement[chunk])

		return meta, got, nil
	} else {
		b.log.Debug("object already existed")
		return meta, got, nil
	}
}

func (b *Balancer) Adapt(lambdaId int) {
	heap.Fix(&b.Queue, b.Placer.group.Instance(lambdaId).Block)
	b.dump()
}

func (b *Balancer) dump() {
	msg := "[%v:%d]%s"
	for _, lambda := range b.Queue {
		msg = fmt.Sprintf(msg, lambda.Deployment.Name(), lambda.Size(), ",[%v:%d]%s")
	}
	b.log.Debug(msg, 0, 0, "\n")
}

func (b *Balancer) Get(key string, chunk int) (*Meta, bool) {
	chunkKey := fmt.Sprintf("%d@%s", chunk, key)
	b.log.Debug("chunk key is %v", chunkKey)
	meta, ok := b.balanceStore.Get(chunkKey)
	if !ok {
		b.log.Debug("get error is %v", ok)
		return nil, ok
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()

	b.log.Debug("meta is %v, %v", meta.Placement, ok)

	return meta, ok
}

//func (b *Balancer) isFull(chunk int) bool {
//	isFull := True
//	for i := 0; i < b.Placer.group.Len(); i++ {
//		if b.Placer.group.Instance(i).Size() < InstanceCapacity*Threshold {
//
//		}
//	}
//}
