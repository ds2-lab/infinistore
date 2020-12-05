package cluster

import (
	"errors"
	"fmt"

	"github.com/mason-leap-lab/infinicache/migrator"
	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/zhangjyr/hashmap"
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
	backend chan *lambdastore.Deployment
	actives *hashmap.HashMap
}

// numCluster = small number, numDeployment = large number
func NewPool(numCluster int, numDeployment int) *Pool {
	s := &Pool{
		backend: make(chan *lambdastore.Deployment, numDeployment+1), // Allocate extra 1 buffer to avoid blocking
		actives: hashmap.New(uintptr(numCluster)),
	}
	for i := 0; i < numDeployment; i++ {
		s.backend <- lambdastore.NewDeployment(config.LambdaPrefix, uint64(i))
	}
	return s
}

func newPool() *Pool {
	return NewPool(config.NumLambdaClusters, config.LambdaMaxDeployments)
}

func (s *Pool) NumAvailable() int {
	return len(s.backend)
}

func (s *Pool) NumActives() int {
	return s.actives.Len()
}

// Get a instance at ith position for the group.
// There is no border check for the index, which means the group should solely responsible
// for the validity of the index, and the index can be a virtual one.
// This operation will be blocked if no more deployment available
func (s *Pool) GetForGroup(g *Group, idx GroupIndex) *lambdastore.Instance {
	ins := g.Reserve(idx, lambdastore.NewInstanceFromDeployment(<-s.backend, uint64(idx.Idx())))
	s.actives.Set(ins.Id(), ins)
	g.Set(ins)
	return ins.LambdaDeployment.(*lambdastore.Instance)
}

// Reserve a deployment at ith position in the group.
// The group may choose to instancize it later or not.
// This operation will return err if no more deployment available
func (s *Pool) ReserveForGroup(g *Group, idx GroupIndex) (types.LambdaDeployment, error) {
	select {
	case item := <-s.backend:
		ins := g.Reserve(idx, item)
		s.actives.Set(ins.Id(), ins)
		return ins.LambdaDeployment, nil
	default:
		return nil, types.ErrNoSpareDeployment
	}
}

// Reserve a deployment to replace specified instance.
// Can be a different deployment other than the instance's for different mode.
func (s *Pool) ReserveForInstance(insId uint64) (types.LambdaDeployment, error) {
	got, exists := s.getActive(insId)
	if !exists {
		return nil, fmt.Errorf("instance %d not found", insId)
	}

	ins := got.(*GroupInstance)
	if IN_DEPLOYMENT_MIGRATION {
		return ins.LambdaDeployment, nil
	} else {
		return s.ReserveForGroup(ins.group, ins.idx)
	}
}

// Helper function for two phase deletion
func (s *Pool) getActive(key interface{}) (interface{}, bool) {
	active, exists := s.actives.Get(key)
	if exists && active != nil {
		return active, true
	} else {
		return nil, false
	}
}

func (s *Pool) Recycle(dp types.LambdaDeployment) {
	// There is no atomic delete that can detect the existence of the key. Using two phase deletion here.
	// We need this to ensure a active being recycle once.
	if active, ok := s.getActive(dp.Id()); !ok {
		return
	} else if !s.actives.Cas(dp.Id(), active, nil) {
		return
	}

	s.actives.Del(dp.Id())
	switch backend := dp.(type) {
	case *lambdastore.Deployment:
		s.backend <- backend
	case *lambdastore.Instance:
		s.backend <- backend.Deployment
	}
}

func (s *Pool) Deployment(id uint64) (types.LambdaDeployment, bool) {
	ins, exists := s.getActive(id)
	if exists {
		return ins.(*GroupInstance).LambdaDeployment, exists
	} else {
		return nil, exists
	}
}

func (s *Pool) Instance(id uint64) *lambdastore.Instance {
	got, exists := s.getActive(id)
	if !exists {
		return nil
	}
	return got.(*GroupInstance).LambdaDeployment.(*lambdastore.Instance)
}

func (s *Pool) InstanceIndex(id uint64) (*GroupInstance, bool) {
	gins, exists := s.getActive(id)
	if !exists {
		return nil, exists
	}

	return gins.(*GroupInstance), exists
}

func (s *Pool) Clear(g *Group) {
	for item := range s.actives.Iter() {
		ins := item.Value.(*GroupInstance)
		if ins.group == g {
			ins.LambdaDeployment.(*lambdastore.Instance).Close()
		}
	}
}

func (s *Pool) ClearAll() {
	for item := range s.actives.Iter() {
		item.Value.(*GroupInstance).LambdaDeployment.(*lambdastore.Instance).Close()
	}
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
	return pool.ReserveForInstance(lambdaId)
}

func initPool() {
	if pool == nil {
		pool = newPool()
		global.Migrator = pool
	}
}

func CleanUpPool() {
	pool.ClearAll()
	pool = nil

	migrator.CleanUp()
}
