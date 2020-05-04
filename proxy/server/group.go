package server

import (
	"errors"
	"sync"

	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var emptyTemplate = make([]*GroupInstance, config.LambdaMaxDeployments)

type Group struct {
	All []*GroupInstance

	size      int
	sliceBase uint64

	base int

	mu sync.RWMutex
}

type GroupInstance struct {
	types.LambdaDeployment
	group *Group
	idx   int
}

func NewGroup(num int) *Group {
	return &Group{
		All: make([]*GroupInstance, num, config.LambdaMaxDeployments),
	}
}

func (g *Group) Len() int {
	return g.size
}

func (g *Group) Expand(n int) (int, error) {
	if cap(g.All) < g.Len()+n {
		return g.size, errors.New("insufficient lambda deployments")
	}
	// bin packing
	if cap(g.All) < len(g.All)+n {
		g.mu.Lock()
		copy(g.All[:len(g.All)-g.base], g.All[g.base:len(g.All)])
		copy(g.All[len(g.All):cap(g.All)], emptyTemplate[:cap(g.All)-len(g.All)])
		g.base = 0
		g.mu.Unlock()
	}
	g.All = g.All[0 : len(g.All)+n]
	g.size = len(g.All) - g.base
	return g.size, nil
}

func (g *Group) Base(offset int) int {
	g.mu.RLock()
	offset += g.base
	g.mu.RUnlock()
	return offset
}

func (g *Group) SubGroup(start int, end int) []*GroupInstance {
	g.mu.RLock()
	subGroup := g.All[start+g.base : end+g.base]
	g.mu.RUnlock()
	return subGroup
}

func (g *Group) IsBoundary(end int) bool {
	return g.size == end
}

func (g *Group) Reserve(idx int, d types.LambdaDeployment) *GroupInstance {
	return &GroupInstance{d, g, idx}
}

func (g *Group) Set(ins *GroupInstance) {
	switch ins.LambdaDeployment.(type) {
	case *lambdastore.Deployment:
		ins.LambdaDeployment = lambdastore.NewInstanceFromDeployment(ins.LambdaDeployment.(*lambdastore.Deployment))
	}
	g.All[ins.idx] = ins
}

func (g *Group) Append(ins *GroupInstance) {
	switch ins.LambdaDeployment.(type) {
	case *lambdastore.Deployment:
		ins.LambdaDeployment = lambdastore.NewInstanceFromDeployment(ins.LambdaDeployment.(*lambdastore.Deployment))
	}
	g.All = append(g.All, ins)
	//g.size += 1
}

func (g *Group) Validate(ins *GroupInstance) *GroupInstance {
	gins := g.All[ins.idx]
	if gins == nil {
		g.Set(ins)
	} else if gins != ins {
		gins.LambdaDeployment.(*lambdastore.Instance).Switch(ins)
	}

	return gins
}

func (g *Group) Instance(idx int) *lambdastore.Instance {
	return g.All[idx].LambdaDeployment.(*lambdastore.Instance)
}
