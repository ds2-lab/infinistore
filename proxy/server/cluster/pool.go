package cluster

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
	"github.com/mason-leap-lab/infinicache/migrator"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const DEP_STATUS_POOLED = 0
const DEP_STATUS_ACTIVE = 1
const DEP_STATUS_ACTIVATING = 2
const IN_DEPLOYMENT_MIGRATION = true

var (
	pool *Pool

	ErrInsufficientDeployments = errors.New("insufficient lambda deployments")
)

type Pool struct {
	backend  chan *lambdastore.Deployment
	actives  hashmap.HashMap
	recycled int32
}

// numCluster = small number, numDeployment = large number
func NewPool(numCluster int, numDeployment int) *Pool {
	s := &Pool{
		backend: make(chan *lambdastore.Deployment, numDeployment+1), // Allocate extra 1 buffer to avoid blocking
		actives: hashmap.NewMap(numCluster),
	}
	for i := 0; i < numDeployment; i++ {
		s.backend <- lambdastore.NewDeployment(global.Options.GetLambdaPrefix(), uint64(i))
	}
	return s
}

func (s *Pool) NumAvailable() int {
	return len(s.backend)
}

func (s *Pool) NumActives() int {
	return s.actives.Len() - int(atomic.LoadInt32(&s.recycled))
}

// Get a instance at ith position for the group.
// There is no border check for the index, which means the group should solely responsible
// for the validity of the index, and the index can be a virtual one.
// This operation will be blocked if no more deployment available
func (s *Pool) GetForGroup(g *Group, idx GroupIndex) *GroupInstance {
	gins := g.Reserve(idx, lambdastore.NewInstanceFromDeployment(<-s.backend, uint64(idx.Idx())))
	s.actives.Store(gins.Id(), gins)
	g.Set(gins)
	return gins
}

// Reserve a deployment at ith position in the group.
// The group may choose to instancize it later or not.
// This operation will return err if no more deployment available
func (s *Pool) ReserveForGroup(g *Group, idx GroupIndex) (*GroupInstance, error) {
	select {
	case item := <-s.backend:
		gins := g.Reserve(idx, item)
		s.actives.Store(gins.Id(), gins)
		return gins, nil
	default:
		return nil, types.ErrNoSpareDeployment
	}
}

// Reserve a deployment to replace specified instance.
// Can be a different deployment other than the instance's for different mode.
func (s *Pool) ReserveForInstance(insId uint64) (*GroupInstance, error) {
	gins, exists := s.getActive(insId)
	if !exists {
		return nil, fmt.Errorf("instance %d not found", insId)
	}

	if IN_DEPLOYMENT_MIGRATION {
		return gins, nil
	} else {
		return s.ReserveForGroup(gins.group, gins.idx)
	}
}

// Helper function for two phase deletion
func (s *Pool) getActive(key interface{}) (*GroupInstance, bool) {
	active, exists := s.actives.Load(key)
	if !exists {
		return nil, false
	} else {
		ins := active.(*GroupInstance)
		return ins, !ins.IsRetired()
	}
}

// Recycle lambda deployment for later use
// Instead of removing instance from actives, placehold a legacy instance to keep track delegate information.
// TODO: Relocate meta to reflect delegated placements.
func (s *Pool) Recycle(dp types.LambdaDeployment) {
	if active, ok := s.getActive(dp.Id()); !ok {
		// Not exist or retired
		return
	} else if !active.Retire() {
		// Retired
		return
	}

	// TODO: Uncomment if metas were relocated.
	// s.actives.Del(dp.Id())
	atomic.AddInt32(&s.recycled, 1)
	switch backend := dp.(type) {
	case *lambdastore.Deployment:
		s.backend <- backend
	case *lambdastore.Instance:
		s.backend <- backend.Deployment
	}
}

func (s *Pool) Deployment(id uint64) (types.LambdaDeployment, bool) {
	gins, exists := s.getActive(id)
	if exists {
		return gins.LambdaDeployment, exists
	} else {
		return nil, exists
	}
}

func (s *Pool) Instance(id uint64) *lambdastore.Instance {
	gins, _ := s.getActive(id)
	// Legacy may be returned.
	if gins == nil {
		return nil
	}
	return gins.LambdaDeployment.(*lambdastore.Instance)
}

func (s *Pool) InstanceIndex(id uint64) (*GroupInstance, bool) {
	gins, exists := s.getActive(id)
	// Legacy will be filtered to avoid error.
	if !exists {
		return nil, exists
	}

	return gins, exists
}

func (s *Pool) Clear(g *Group) {
	s.actives.Range(func(key, value interface{}) bool {
		if gins, ok := value.(*GroupInstance); ok && !gins.IsRetired() && gins.group == g {
			gins.LambdaDeployment.(*lambdastore.Instance).Close()
		}
		return true
	})
}

func (s *Pool) ClearAll() {
	s.actives.Range(func(key, value interface{}) bool {
		if gins, ok := value.(*GroupInstance); ok && !gins.IsRetired() {
			gins.LambdaDeployment.(*lambdastore.Instance).Close()
		}
		return true
	})
}

// MigrationScheduler implementations
func (s *Pool) StartMigrator(lambdaId uint64) (string, error) {
	m := migrator.New(global.BaseMigratorPort+int(lambdaId), true)
	err := m.Listen()
	if err != nil {
		return "", err
	}

	go m.Serve()

	return m.Addr, nil
}

func (s *Pool) GetDestination(lambdaId uint64) (types.LambdaDeployment, error) {
	gins, err := pool.ReserveForInstance(lambdaId)
	if err == nil {
		return gins.LambdaDeployment, err
	} else {
		return nil, err
	}
}

func initPool(size int) {
	if pool == nil {
		pool = NewPool(size, config.LambdaMaxDeployments)
		global.Migrator = pool
	}
}

func CleanUpPool() {
	pool.ClearAll()
	pool = nil

	migrator.CleanUp()
}
