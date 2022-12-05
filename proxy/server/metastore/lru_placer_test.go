package metastore

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils/mapreduce"
	"github.com/mason-leap-lab/infinicache/common/logger"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var container []*Meta

func init() {
	global.Log = &logger.ColorLogger{
		Level: logger.LOG_LEVEL_ALL,
	}
}

type TestInstanceManager struct {
	all []*lambdastore.Instance
}

func (im *TestInstanceManager) Instance(id uint64) *lambdastore.Instance {
	return im.all[id]
}

func (im *TestInstanceManager) Recycle(dp types.LambdaDeployment) error {
	return nil
}

func (im *TestInstanceManager) GetBackupCandidates() mapreduce.Iterator {
	return nil
}

func (im *TestInstanceManager) GetDelegates() []*lambdastore.Instance {
	return nil
}

func (im *TestInstanceManager) GetActiveInstances(int) lambdastore.InstanceEnumerator {
	return lambdastore.NewInstanceEnumerator(im.all)
}

func (im *TestInstanceManager) GetSlice(int) Slice {
	return nil
}

func (im *TestInstanceManager) Trigger(int, ...interface{}) {}

func (im *TestInstanceManager) GetServePort(uint64) int {
	return 0
}

func newTestMeta(i int) *Meta {
	return &Meta{
		key:        strconv.Itoa(i),
		ChunkSize:  1,
		Placement:  []uint64{uint64(i)},
		placerMeta: &LRUPlacerMeta{},
	}
}

func initPlacer(caseCode int) *LRUPlacer {
	container = make([]*Meta, 0, 15)
	placer := NewLRUPlacer(nil, nil)

	for i := 0; i < 10; i++ {
		container = append(container, newTestMeta(i))
		placer.AddObject(container[i])
	}
	switch caseCode {
	case 1:
		placer.NextAvailableObject(container[0], nil)
		for i := 0; i < 4; i++ {
			placer.TouchObject(container[i])
		}
		for i := 5; i < 10; i++ {
			placer.TouchObject(container[i])
		}
	}
	return placer
}

func initGroupPlacer(numCluster int, capacity int) *LRUPlacer {
	im := &TestInstanceManager{all: make([]*lambdastore.Instance, numCluster)}
	for i := 0; i < numCluster; i++ {
		ins := lambdastore.NewInstance("TestInstance", uint64(i))
		ins.Meta.ResetCapacity(uint64(capacity), uint64(0))
		im.all[i] = ins
	}
	return NewLRUPlacer(New(), im)
}

func dumpPlacer(p *LRUPlacer, args ...bool) string {
	if len(args) > 0 && args[0] {
		return dump(p.objects[p.secondary])
	} else {
		return dump(p.objects[p.primary])
	}
}

func dump(metas []*Meta) string {
	if metas == nil || len(metas) < 1 {
		return ""
	}

	elem := make([]string, len(metas)-1)
	for i, meta := range metas[1:] {
		if meta == nil {
			elem[i] = "nil"
			continue
		}

		visited := 0
		if meta.placerMeta.(*LRUPlacerMeta).visited {
			visited = 1
		}
		elem[i] = fmt.Sprintf("%s-%d", meta.Key(), visited)
	}
	return strings.Join(elem, ",")
}

func proxySimulator(incomes chan interface{}, p *LRUPlacer, done *sync.WaitGroup) {
	for income := range incomes {
		switch m := income.(type) {
		case *Meta:
			chunk := m.lastChunk
			fmt.Printf("Processing %d@%s\n", m.lastChunk, m.Key())
			meta, _, _ := p.Insert(m.Key(), m)
			fmt.Printf("Set %d@%s: %v\n", chunk, meta.Key(), meta.Placement)
		case func():
			m()
		}

	}
	done.Done()
}

var _ = Describe("Placer", func() {
	It("should visited be initialized with true", func() {
		placer := initPlacer(0)
		Expect(dumpPlacer(placer)).To(Equal("0-1,1-1,2-1,3-1,4-1,5-1,6-1,7-1,8-1,9-1"))
		Expect(dumpPlacer(placer, true)).To(Equal(""))
	})

	It("should replace the unvisited object", func() {
		placer := initPlacer(1)
		idx := len(container)
		container = append(container, newTestMeta(idx))

		_, found := placer.NextAvailableObject(container[idx], nil)
		Expect(found).To(Equal(true))
		Expect(dumpPlacer(placer)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1,5-1,6-1,7-1,8-1,9-1", idx)))
		Expect(dumpPlacer(placer, true)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1", idx)))
		Expect(container[idx].placerMeta.(*LRUPlacerMeta).pos).To(Equal([2]int{5, 5}))
		Expect(container[idx].placerMeta.(*LRUPlacerMeta).swapMap).To(Equal(container[4].Placement))
	})

	It("should replace the unvisited object even the newer has been appended to the list", func() {
		placer := initPlacer(1)
		idx := len(container)
		container = append(container, newTestMeta(idx))

		placer.AddObject(container[idx])
		Expect(dumpPlacer(placer)).To(Equal(fmt.Sprintf("0-1,1-1,2-1,3-1,4-0,5-1,6-1,7-1,8-1,9-1,%d-1", idx)))
		Expect(container[idx].placerMeta.(*LRUPlacerMeta).pos).To(Equal([2]int{0, idx + 1}))

		_, found := placer.NextAvailableObject(container[idx], nil)
		Expect(found).To(Equal(true))
		Expect(dumpPlacer(placer)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1,5-1,6-1,7-1,8-1,9-1,nil", idx)))
		Expect(dumpPlacer(placer, true)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1", idx)))
		Expect(container[idx].placerMeta.(*LRUPlacerMeta).pos).To(Equal([2]int{5, 5}))
		Expect(container[idx].placerMeta.(*LRUPlacerMeta).swapMap).To(Equal(container[4].Placement))
	})

	It("should a second call and compact works if the first call failed", func() {
		placer := initPlacer(1)
		idx := len(container)
		container = append(container, newTestMeta(idx), newTestMeta(idx+1))

		placer.AddObject(container[idx])
		placer.NextAvailableObject(container[idx], nil)

		_, found := placer.NextAvailableObject(container[idx+1], nil)
		Expect(found).To(Equal(false))
		Expect(dumpPlacer(placer)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1,5-0,6-0,7-0,8-0,9-0", idx)))
		Expect(dumpPlacer(placer, true)).To(Equal(fmt.Sprintf("0-0,1-0,2-0,3-0,%d-1,5-0,6-0,7-0,8-0,9-0,nil", idx)))

		_, found = placer.NextAvailableObject(container[idx+1], nil)
		Expect(found).To(Equal(true))
		Expect(dumpPlacer(placer)).To(Equal(fmt.Sprintf("%d-1,1-0,2-0,3-0,%d-1,5-0,6-0,7-0,8-0,9-0", idx+1, idx)))
		Expect(dumpPlacer(placer, true)).To(Equal(fmt.Sprintf("%d-1", idx+1)))
		Expect(container[idx+1].placerMeta.(*LRUPlacerMeta).pos).To(Equal([2]int{1, 1}))
		Expect(container[idx+1].placerMeta.(*LRUPlacerMeta).swapMap).To(Equal(container[0].Placement))
	})

	It("should post process callback works", func() {
		var called string
		cb := func(meta *Meta) {
			called = meta.Key()
		}

		meta := newTestMeta(1)
		meta.placerMeta.(*LRUPlacerMeta).evicts = newTestMeta(2)
		meta.placerMeta.(*LRUPlacerMeta).once = &sync.Once{}
		meta.placerMeta.(*LRUPlacerMeta).postProcess(cb)

		Expect(called).To(Equal("2"))
	})

	It("should basic LRU works", func() {
		numCluster := 10
		capacity := 1000
		reqId := uuid.New().String()

		n := 50
		shards := 6
		chunkSize := int64(400)

		placer := initGroupPlacer(numCluster, capacity)
		queues := make([]chan interface{}, numCluster)
		var done sync.WaitGroup
		for i := 0; i < numCluster; i++ {
			queues[i] = make(chan interface{})
			done.Add(1)
			go proxySimulator(queues[i], placer, &done)
		}

		sess := 0
		for i := 0; i < n; i++ {
			for j := 0; j < shards; j++ {
				lambdaId := sess % numCluster
				queues[lambdaId] <- placer.NewMeta(reqId, strconv.Itoa(i), chunkSize*int64(shards), 4, 2, j, int64(chunkSize), uint64(lambdaId), 0)
				sess++
			}
		}

		for i := 0; i < numCluster; i++ {
			close(queues[i])
		}
		done.Wait()

		Expect(true).To(Equal(true))
	})

	It("should GET request return same placement", func() {
		numCluster := 10
		capacity := 1000
		reqId := uuid.New().String()

		shards := 6
		chunkSize := int64(400)

		placer := initGroupPlacer(numCluster*2, capacity)
		queues := make([]chan interface{}, numCluster)
		var simulators sync.WaitGroup
		for i := 0; i < numCluster; i++ {
			queues[i] = make(chan interface{})
			simulators.Add(1)
			go proxySimulator(queues[i], placer, &simulators)
		}

		var conns sync.WaitGroup
		sess := 0
		for i := 0; i < 2; i++ {
			for j := 0; j < shards; j++ {
				conns.Add(1)
				lambdaId := sess % numCluster
				queues[lambdaId] <- func(m *Meta) func() {
					return func() {
						meta, _, _ := placer.Insert(m.Key(), m)
						fmt.Printf("Set %d@%s: %v\n", m.lastChunk, meta.Key(), meta.Placement)
						conns.Done()
					}
				}(placer.NewMeta(reqId, strconv.Itoa(i), chunkSize*int64(shards), 4, 2, j, int64(chunkSize), uint64(lambdaId), 0))
				sess++
			}
		}

		for i := 0; i < numCluster; i++ {
			close(queues[i])
		}
		simulators.Wait()

		conns.Wait()
		meta, ok := placer.Get("1", 0)
		Expect(ok).To(Equal(true))
		Expect(meta.Key).To(Equal("1"))
		Expect(meta.placerMeta.(*LRUPlacerMeta).confirmed).To(Equal([]bool{true, true, true, true, true, true}))
		Expect(meta.Placement).To(Equal(Placement{6, 7, 8, 9, 0, 1}))
	})
})
