package store

import (
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/lambda/lifetime"
	"github.com/mason-leap-lab/infinicache/lambda/storage"
	"github.com/mason-leap-lab/infinicache/lambda/types"
	"github.com/mason-leap-lab/infinicache/lambda/worker"
)

var (
	Server  *worker.Worker
	Store   types.Storage = (*storage.Storage)(nil)
	Persist types.PersistentStorage
	Lineage types.Lineage
	Log     = &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false}

	// Track how long the store has lived, migration is required before timing up.
	Lifetime *lifetime.Lifetime
)

func IsDebug() bool {
	return Log.Level == logger.LOG_LEVEL_ALL
}
