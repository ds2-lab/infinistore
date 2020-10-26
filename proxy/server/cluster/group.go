package cluster

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
	all       []*GroupInstance
	idxBase   int
	mu        sync.RWMutex
}

type GroupInstance struct {
	types.LambdaDeployment
	group    *Group
	idx      GroupIndex
}

func (gins *GroupInstance) Idx() int {
	return gins.idx.Idx()
}

func (gins *GroupInstance) Instance() *lambdastore.Instance {
	ins, _ := gins.LambdaDeployment.(*lambdastore.Instance)
	return ins
}

type GroupIndex interface {
	Idx() int
}

type DefaultGroupIndex int

func (i DefaultGroupIndex) Idx() int {
	return int(i)
}

func (i *DefaultGroupIndex) Next() DefaultGroupIndex {
	return *i + 1
}

func (i *DefaultGroupIndex) NextN(n int) DefaultGroupIndex {
	return *i + DefaultGroupIndex(n)
}

func NewGroup(num int) *Group {
	return &Group{
		all: make([]*GroupInstance, num, config.LambdaMaxDeployments),
	}
}

func (g *Group) Len() int {
	return len(g.all)
}

func (g *Group) StartIndex() DefaultGroupIndex {
	return DefaultGroupIndex(g.idxBase)
}

func (g *Group) EndIndex() DefaultGroupIndex {
	g.mu.RLock()
	end := g.idxBase + len(g.all)
	g.mu.RUnlock()
	return DefaultGroupIndex(end)
}

func (g *Group) Expand(n int) (DefaultGroupIndex, error) {
	if cap(g.all) < len(g.all)+n {
		return DefaultGroupIndex(g.idxBase + len(g.all)), errors.New("insufficient lambda deployments")
	}

	g.mu.RLock()
	g.all = g.all[0 : len(g.all)+n]
	end := g.idxBase + len(g.all)
	g.mu.RUnlock()

	return DefaultGroupIndex(end), nil
}

func (g *Group) Expire(n int) error {
	if n > len(g.all) {
		return errors.New("not enough instance to be expired")
	}

	// Force bin packing
	all := make([]*GroupInstance, config.LambdaMaxDeployments)
	g.mu.Lock()
	all = all[:len(g.all) - n]
	copy(all, g.all[n:len(g.all)])
	g.idxBase += n
	g.all = all
	g.mu.Unlock()

	return nil
}

func (g *Group) All() []*lambdastore.Instance {
	g.mu.RLock()
	all := make([]*lambdastore.Instance, len(g.all))
	for i := 0; i < len(all); i++ {
		all[i] = g.all[i].Instance()
	}
	g.mu.RUnlock()

	return all
}

func (g *Group) SubGroup(start GroupIndex, end GroupIndex) []*GroupInstance {
	g.mu.RLock()
	sub := make([]*GroupInstance, end.Idx() - start.Idx())
	for i := 0; i < len(sub); i++ {
		sub[i] = g.all[start.Idx()-g.idxBase+i]
	}
	g.mu.RUnlock()
	return sub
}

func (g *Group) Reserve(idx GroupIndex, d types.LambdaDeployment) *GroupInstance {
	return &GroupInstance{d, g, idx}
}

func (g *Group) Set(ins *GroupInstance) {
	g.setWithTest(ins, false)
}

// Checks if specified instance is valid at corresponding index.
// If not, switch functionality to the new instance.
func (g *Group) Validate(ins *GroupInstance) *GroupInstance {
	ret, err := g.setWithTest(ins, true)
	if err != nil {
		return nil
	} else if ret != ins {
		ret.LambdaDeployment.(*lambdastore.Instance).Switch(ins)
	}
	return ret
}

func (g *Group) Instance(idx GroupIndex) *lambdastore.Instance {
	g.mu.RLock()
	ret := g.all[idx.Idx()-g.idxBase]
	g.mu.RUnlock()
	return ret.LambdaDeployment.(*lambdastore.Instance)
}

// setWithTest ensures specfied instance being instancized at corresponding index.
// If test is set, a different instance may be returned if the instance has been replaced.
func (g *Group) setWithTest(ins *GroupInstance, test bool) (ret *GroupInstance, err error) {
	g.mu.RLock()
	if !test {
		g.all[ins.Idx()-g.idxBase] = ins
		ret = ins
	} else if ins.Idx() < g.idxBase || ins.Idx() >= g.idxBase + len(g.all) {
		err = ErrOutOfBound
	} else if ret = g.all[ins.Idx()-g.idxBase]; ret == nil {
		g.all[ins.Idx()-g.idxBase] = ins
		ret = ins
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
