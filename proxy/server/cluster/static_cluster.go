package cluster

import (
	"math/rand"
	"sync"

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

type StaticCluster struct {
	log       logger.ILogger
	group     *Group
	placer    *metastore.LRUPlacer
	ready     sync.WaitGroup
	instances cache.InlineCache
}

// initial lambda group
func NewStaticCluster(size int) *StaticCluster {
	extra := 0
	if global.Options.Evaluation && global.Options.NumBackups > 0 {
		extra = global.Options.NumBackups
	}
	c := &StaticCluster{
		log:   global.GetLogger("StaticCluster: "),
		group: NewGroup(size + extra),
	}
	c.placer = metastore.NewLRUPlacer(metastore.New(), c)
	c.instances.Producer = cache.InlineProducer0(func() interface{} {
		return c.group.All()
	})

	// Initialize instances
	for i := c.group.StartIndex(); i < c.group.EndIndex(); i = i.Next() {
		c.log.Info("[Registering lambda store %s%d]", config.LambdaPrefix, i)
		pool.GetForGroup(c.group, i)
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
func (c *StaticCluster) Len() int {
	return c.group.Len()
}

func (c *StaticCluster) Instance(id uint64) (*lambdastore.Instance, bool) {
	return pool.Instance(id)
}

func (c *StaticCluster) Relocate(obj interface{}, chunk int) *lambdastore.Instance {
	// Not support.
	return nil
}

func (c *StaticCluster) TryRelocate(o interface{}, chunkId int) (*lambdastore.Instance, bool) {
	return nil, false
}

func (c *StaticCluster) Recycle(ins types.LambdaDeployment) error {
	return ErrUnsupported
}

// metastore.InstanceManger implementation
func (c *StaticCluster) GetActiveInstances(num int) []*lambdastore.Instance {
	return c.instances.Value().([]*lambdastore.Instance)
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
