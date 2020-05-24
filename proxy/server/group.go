package server

import (
	"errors"
	"sync"

	"github.com/mason-leap-lab/infinicache/proxy/config"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	ErrOutOfBound = errors.New("instance is not active")
)

type Group struct {
	All       []*GroupInstance
	idxBase   int
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
	return len(g.All)
}

func (g *Group) Expand(n int) (int, error) {
	if cap(g.All) < len(g.All)+n {
		return g.idxBase + len(g.All), errors.New("insufficient lambda deployments")
	}

	g.mu.RLock()
	g.All = g.All[0 : len(g.All)+n]
	end := g.idxBase + len(g.All)
	g.mu.RUnlock()

	return end, nil
}

func (g *Group) Expire(n int) error {
	if n > len(g.All) {
		return errors.New("not enough instance to be expired")
	}

	// Force bin packing
	all := make([]*GroupInstance, config.LambdaMaxDeployments)
	g.mu.Lock()
	all = all[:len(g.All) - n]
	copy(all, g.All[n:len(g.All)])
	g.idxBase += n
	g.All = all
	g.mu.Unlock()

	return nil
}

func (g *Group) Base(offset int) int {
	g.mu.RLock()
	offset -= g.idxBase
	g.mu.RUnlock()
	return offset
}

func (g *Group) SubGroup(startOffset int, endOffset int) []*GroupInstance {
	g.mu.RLock()
	subGroup := g.All[startOffset-g.idxBase:endOffset-g.idxBase]
	g.mu.RUnlock()
	return subGroup
}

func (g *Group) IsBoundary(end int) bool {
	g.mu.RLock()
	ret := len(g.All) + g.idxBase == end
	g.mu.RUnlock()
	return ret
}

func (g *Group) Reserve(idx int, d types.LambdaDeployment) *GroupInstance {
	return &GroupInstance{d, g, idx}
}

func (g *Group) Set(ins *GroupInstance) {
	g.setWithTest(ins, false)
}

func (g *Group) Validate(ins *GroupInstance) *GroupInstance {
	ret, err := g.setWithTest(ins, true)
	if err != nil {
		return nil
	} else if ret != ins {
		ret.LambdaDeployment.(*lambdastore.Instance).Switch(ins)
	}
	return ret
}

func (g *Group) Instance(idx int) *lambdastore.Instance {
	g.mu.RLock()
	ret := g.All[idx-g.idxBase]
	g.mu.RUnlock()
	return ret.LambdaDeployment.(*lambdastore.Instance)
}

func (g *Group) InstanceStats(idx int) types.InstanceStats {
	return g.Instance(idx)
}

func (g *Group) AllInstancesStats() types.Iterator {
	all := g.All
	return types.NewStatsIterator(all, len(all))
}

func (g *Group) InstanceStatsFromIterator(iter types.Iterator) (int, types.InstanceStats) {
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

func (g *Group) setWithTest(ins *GroupInstance, test bool) (ret *GroupInstance, err error) {
	g.mu.RLock()
	if !test {
		g.All[ins.idx-g.idxBase] = ins
		ret = ins
	} else if ins.idx < g.idxBase || ins.idx >= g.idxBase + len(g.All) {
		err = ErrOutOfBound
	} else if ret = g.All[ins.idx-g.idxBase]; ret == nil {
		g.All[ins.idx-g.idxBase] = ins
	}
	g.mu.RUnlock()

	if ret != nil {
		switch deployment := ret.LambdaDeployment.(type) {
		case *lambdastore.Deployment:
			ret.LambdaDeployment = lambdastore.NewInstanceFromDeployment(deployment)
		}
	}
	return
}
