package types

import (
	"errors"
	"strconv"
)

var ErrNoSpareDeployment = errors.New("No spare deployment")

type Id struct {
	ConnId   int
	ReqId    string
	ChunkId  string
	oldChunk *string
	chunk    int
}

func (id *Id) Chunk() int {
	if id.oldChunk != &id.ChunkId {
		id.chunk, _ = strconv.Atoi(id.ChunkId)
		id.oldChunk = &id.ChunkId
	}
	return id.chunk
}

type Command interface {
	String() string
	GetRequest() *Request
	Retriable() bool
	Flush() error
}

type LambdaDeployment interface {
	Name() string
	Id() uint64
	Reset(new LambdaDeployment, old LambdaDeployment)
}

type MigrationScheduler interface {
	StartMigrator(uint64) (string, error)
	GetDestination(uint64) (LambdaDeployment, error)
}

type Iterator interface {
	Len() int
	Next() bool
	Value() (int, interface{})
}

type ClusterStats interface {
	Len() int
	InstanceStats(int) InstanceStats
	AllInstancesStats() Iterator
	InstanceStatsFromIterator(Iterator) (int, InstanceStats)
}

type GroupedClusterStats interface {
	Len() int
	ClusterStats(int) ClusterStats
	AllClustersStats() Iterator
	ClusterStatsFromIterator(Iterator) (int, ClusterStats)
	InstanceSum(int) int
}

type InstanceStats interface {
	Status() uint64
}

type StatsIterator struct {
	arr  interface{}
	len  int
	i    int
}

func NewStatsIterator(arr interface{}, len int) *StatsIterator {
	return &StatsIterator{ arr: arr, len: len, i: -1 }
}

func (iter *StatsIterator) Len() int {
	return iter.len
}

func (iter *StatsIterator) Next() bool {
	iter.i++
	return iter.i < iter.len
}

func (iter *StatsIterator) Value() (int, interface{}) {
	return iter.i, iter.arr
}
