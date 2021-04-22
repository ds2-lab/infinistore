package cluster

import (
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/infinicache/common/util/cache"

	"github.com/mason-leap-lab/infinicache/proxy/collector"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type SliceInitializer func(int) (int, int)

type Slice struct {
	once *sync.Once
	init SliceInitializer
	size int
	base int
	cap  int
}

func NewSlice(size int, initializer SliceInitializer) *Slice {
	return &Slice{once: &sync.Once{}, init: initializer, size: size}
}

func (s *Slice) Size() int {
	return s.size
}

func (s *Slice) Reset(size int) {
	s.size = size
	s.once = &sync.Once{}
}

func (s *Slice) GetIndex(idx uint64) uint64 {
	s.once.Do(s.get)
	return uint64((s.base + int(idx)) % s.cap)
}

func (s *Slice) get() {
	s.base, s.cap = s.init(s.size)
}

type StaticCluster struct {
	log       logger.ILogger
	group     *Group
	placer    *metastore.LRUPlacer
	ready     sync.WaitGroup
	cap       int
	instances cache.InlineCache
	sliceBase uint64
}

// initial lambda group
func NewStaticCluster(size int) *StaticCluster {
	initPool(size)

	extra := 0
	if global.Options.Evaluation && global.Options.NumBackups > 0 {
		extra = global.Options.NumBackups
	}
	c := &StaticCluster{
		log:   global.GetLogger("StaticCluster: "),
		cap:   size + extra,
		group: NewGroup(size + extra),
	}
	c.placer = metastore.NewLRUPlacer(metastore.New(), c)
	c.instances.Producer = cache.InlineProducer0(func() interface{} {
		return c.group.All()
	})

	// Initialize instances
	for i := c.group.StartIndex(); i < c.group.EndIndex(); i = i.Next() {
		ins := pool.GetForGroup(c.group, i)
		c.log.Info("[Lambda store %s Registered]", ins.Name())
	}
	// Something can only be done after all nodes initialized.
	all := c.instances.Value().([]*lambdastore.Instance)
	for i, node := range all {
		node.AssignBackups(c.getBackupsForNode(all, i))

		// Initialize instance, this is not neccessary if the start time of the instance is acceptable.
		// c.ready.Add(1)
		// go func() {
		// 	node.WarmUp()
		// 	c.ready.Done()
		// }()

		// Begin handle requests
		go node.HandleRequests()
	}

	return c
}

func (c *StaticCluster) Start() error {
	// Do nothing.
	return nil
}

func (c *StaticCluster) WaitReady() {
	c.ready.Wait()
}

func (c *StaticCluster) GetPlacer() metastore.Placer {
	return c.placer
}

func (c *StaticCluster) CollectData() {
	for _, gins := range c.group.all {
		// send data command
		gins.Instance().CollectData()
	}
	c.log.Info("Waiting data from Lambda")
	global.DataCollected.Wait()
	if err := collector.Flush(); err != nil {
		c.log.Error("Failed to save data from lambdas: %v", err)
	} else {
		c.log.Info("Data collected.")
	}
}

func (c *StaticCluster) Close() {
	for i, gins := range c.group.all {
		gins.Instance().Close()
		c.group.all[i] = nil
	}
	pool.Clear(c.group)
}

// ClusterStatus implementation
func (c *StaticCluster) InstanceLen() int {
	return c.group.Len()
}

func (c *StaticCluster) InstanceStats(idx int) types.InstanceStats {
	return c.group.all[idx].Instance()
}

func (c *StaticCluster) AllInstancesStats() types.Iterator {
	all := c.group.all
	return types.NewStatsIterator(all, len(all))
}

func (c *StaticCluster) InstanceStatsFromIterator(iter types.Iterator) (int, types.InstanceStats) {
	i, val := iter.Value()

	var ins *GroupInstance
	switch item := val.(type) {
	case []*GroupInstance:
		ins = item[i]
	case *GroupInstance:
		ins = item
	}

	if ins == nil {
		return i, nil
	} else {
		return i, ins.LambdaDeployment.(*lambdastore.Instance)
	}
}

// lambdastore.InstanceManager implementation
func (c *StaticCluster) Instance(id uint64) *lambdastore.Instance {
	return pool.Instance(id)
}

func (c *StaticCluster) Recycle(ins types.LambdaDeployment) error {
	return ErrUnsupported
}

// lambdastore.Relocator implementation
func (c *StaticCluster) Relocate(obj interface{}, chunk int, cmd types.Command) (*lambdastore.Instance, error) {
	// Not support.
	return nil, ErrUnsupported
}

func (c *StaticCluster) TryRelocate(o interface{}, chunkId int, cmd types.Command) (*lambdastore.Instance, bool, error) {
	return nil, false, ErrUnsupported
}

// metastore.InstanceManger implementation
func (c *StaticCluster) GetActiveInstances(num int) []*lambdastore.Instance {
	return c.instances.Value().([]*lambdastore.Instance)
}

func (c *StaticCluster) GetSlice(size int) metastore.Slice {
	return NewSlice(size, c.nextSlice)
}

func (c *StaticCluster) Trigger(event int, args ...interface{}) {
	// No event
}

func (c *StaticCluster) getBackupsForNode(all []*lambdastore.Instance, i int) (int, []*lambdastore.Instance) {
	numBaks := config.BackupsPerInstance
	available := len(all)
	selectFrom := 0 // Select backups from this index.
	unipool := 1    // If i can be selected as a backup.
	if global.Options.Evaluation && global.Options.NumBackups > 0 {
		// In evaluation mode, main nodes and backup nodes are seqarated
		numBaks = global.Options.NumBackups
		available = global.Options.NumBackups
		selectFrom = len(all) - available
		unipool = 0
	}

	numTotal := numBaks * 2
	distance := available / (numTotal + unipool) // main + double backup candidates
	if distance == 0 {
		// In case 2 * total >= g.Len()
		distance = 1
		numBaks = util.Ifelse(numBaks >= available, available-unipool, numBaks).(int) // Use all
		numTotal = util.Ifelse(numTotal >= available, available-unipool, numTotal).(int)
	}
	candidates := make([]*lambdastore.Instance, numTotal)
	for j := 0; j < numTotal; j++ {
		candidates[j] = all[selectFrom+(i+j*distance+rand.Int()%distance+1)%available] // Random to avoid the same backup set.
	}
	return numBaks, candidates
}

func (c *StaticCluster) nextSlice(sliceSize int) (int, int) {
	return int((atomic.AddUint64(&c.sliceBase, uint64(sliceSize)) - uint64(sliceSize)) % uint64(c.cap)), c.cap
}
