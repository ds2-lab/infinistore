package types

import (
	"errors"
)

var ErrNoSpareDeployment = errors.New("No spare deployment")

type ClientReqCounter struct {
	Cmd          string
	DataShards   int64
	ParityShards int64
	Returned     int64       // Returned counter from lambda.
	Requests     []*Request
}

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
