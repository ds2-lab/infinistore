package cluster

import (
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
)

type Cluster interface {
	lambdastore.InstanceManager
	metastore.InstanceManager

	Start() error
	WaitReady()
	GetPlacer() metastore.Placer
	CollectData()
	Close()
}
