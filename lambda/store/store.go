package store

import (
	"github.com/ds2-lab/infinistore/common/logger"
	"github.com/ds2-lab/infinistore/lambda/lifetime"
	"github.com/ds2-lab/infinistore/lambda/types"
	"github.com/ds2-lab/infinistore/lambda/worker"
)

var (
	Server  *worker.Worker
	Store   types.Storage
	Persist types.PersistentStorage
	Lineage types.Lineage
	Log     = &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false}

	// Track how long the store has lived, migration is required before timing up.
	Lifetime *lifetime.Lifetime
)

func IsDebug() bool {
	return Log.Level == logger.LOG_LEVEL_ALL
}
