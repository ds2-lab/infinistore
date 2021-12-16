package lambdastore

import (
	"github.com/mason-leap-lab/go-utils/mapreduce"
)

type InstanceEnumerator interface {
	mapreduce.Enumerator
	Instance(i int) *Instance
}

type DefaultInstanceEnumerator struct {
	mapreduce.Enumerator
}

func NewInstanceEnumerator(instances []*Instance) *DefaultInstanceEnumerator {
	enumerator, _ := mapreduce.NewEnumerator(instances)
	return &DefaultInstanceEnumerator{Enumerator: enumerator}
}

func (enum *DefaultInstanceEnumerator) Instance(i int) *Instance {
	return enum.Item(i).(*Instance)
}
