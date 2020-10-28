package cluster

import (
	"fmt"

	"github.com/cornelk/hashmap"
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
		s.backend <- lambdastore.NewDeployment(config.LambdaPrefix, uint64(i), false)
	}
	return s
}

func newPool() *Pool {
	return NewPool(config.NumLambdaClusters, config.LambdaMaxDeployments)
}

// Get a instance at ith position for the group.
// There is no border check for the index, which means the group should solely responsible
// for the validity of the index, and the index can be a virtual one.
// This operation will be blocked if no more deployment available
func (s *Pool) GetForGroup(g *Group, idx GroupIndex) *lambdastore.Instance {
	ins := g.Reserve(idx, lambdastore.NewInstanceFromDeployment(<-s.backend))
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
	got, exists := s.actives.Get(insId)
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

func (s *Pool) Recycle(dp types.LambdaDeployment) {
	s.actives.Del(dp.Id())
	switch backend := dp.(type) {
	case *lambdastore.Deployment:
		s.backend <- backend
	case *lambdastore.Instance:
		backend.Close()
		s.backend <- backend.Deployment
	}
}

func (s *Pool) Deployment(id uint64) (types.LambdaDeployment, bool) {
	ins, exists := s.actives.Get(id)
	if exists {
		return ins.(*GroupInstance).LambdaDeployment, exists
	} else {
		return nil, exists
	}
}

func (s *Pool) Instance(id uint64) (*lambdastore.Instance, bool) {
	got, exists := s.actives.Get(id)
	if !exists {
		return nil, exists
	}

	ins := got.(*GroupInstance)
	validated := ins.group.Validate(ins)
	if validated != ins {
		// Update actives
		s.actives.Set(validated.Id(), validated)
		// Update so ins can be recycled
		s.actives.Set(ins.Id(), ins)
		s.Recycle(ins.LambdaDeployment)
	}
	return validated.LambdaDeployment.(*lambdastore.Instance), exists
}

func (s *Pool) InstanceIndex(id uint64) (*GroupInstance, bool) {
	gins, exists := s.actives.Get(id)
	if !exists {
		return nil, exists
	}

	return gins.(*GroupInstance), exists
}

func (s *Pool) Clear(g *Group) {
	for item := range s.actives.Iter() {
		ins := item.Value.(*GroupInstance)
		if ins.group == g {
			s.Recycle(ins.LambdaDeployment)
		}
	}
}

func (s *Pool) ClearAll() {
	for item := range s.actives.Iter() {
		s.Recycle(item.Value.(*GroupInstance).LambdaDeployment)
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

func init() {
	pool = newPool()

	global.Migrator = pool

}

func CleanUpPool() {
	pool.ClearAll()
	pool = nil

	migrator.CleanUp()
}
