package cluster

import (
	"github.com/mason-leap-lab/go-utils/mapreduce"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

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

type GroupInstance struct {
	types.LambdaDeployment
	group    *Group
	idx      GroupIndex
	disabled bool
}

func (gins *GroupInstance) Idx() int {
	return gins.idx.Idx()
}

func (gins *GroupInstance) Instance() *lambdastore.Instance {
	ins, _ := gins.LambdaDeployment.(*lambdastore.Instance)
	return ins
}

type GroupInstanceEnumerator struct {
	mapreduce.Enumerator
}

func NewGroupInstanceEnumerator(ginstances []*GroupInstance) *GroupInstanceEnumerator {
	enumerator, _ := mapreduce.NewEnumerator(ginstances)
	return &GroupInstanceEnumerator{Enumerator: enumerator}
}

func (enum *GroupInstanceEnumerator) Instance(i int) *lambdastore.Instance {
	return enum.Item(i).(*GroupInstance).Instance()
}
