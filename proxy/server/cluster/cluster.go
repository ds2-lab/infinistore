package cluster

import (
	"errors"

	"github.com/ds2-lab/infinistore/proxy/lambdastore"
	"github.com/ds2-lab/infinistore/proxy/server/metastore"
	"github.com/ds2-lab/infinistore/proxy/types"
)

var (
	ErrUnsupported   = errors.New("unsupported")
	ErrClusterClosed = errors.New("err cluster closed")
)

type Cluster interface {
	lambdastore.InstanceManager
	lambdastore.Relocator
	metastore.ClusterManager

	Start() error
	WaitReady()
	GetPlacer() metastore.Placer
	CollectData()
	Close()
}

type ServerProvider interface {
	GetServePort(uint64) int
	GetPersistCache() types.PersistCache
}
