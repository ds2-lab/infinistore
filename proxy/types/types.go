package types

import (
	"errors"
)

var ErrNoSpareDeployment = errors.New("No spare deployment")

type Id struct {
	ConnId  int
	ReqId   string
	ChunkId string
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

type ClusterStatus interface {
	Len() int
	InstanceStatus(int) InstanceStatus
}

type InstanceStatus interface {
	Status() uint64
}
