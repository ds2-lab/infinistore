package global

import (
	"math/rand"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

var (
	// Clients        = make([]chan interface{}, 1024*1024)
	Options          CommandlineOptions
	DataCollected    sync.WaitGroup
	Log              logger.ILogger
	ReqCoordinator   = NewRequestCoordinator(1024)
	Migrator         types.MigrationScheduler
	BasePort         = 6378
	BaseMigratorPort = 6380
	ServerIp         string
	Flags            uint64
)

func init() {
	rand.Seed(time.Now().UnixNano()) // Reseed random.
	Log = logger.NilLogger

	if ServerIp == "" {
		ip, err := GetPrivateIp()
		if err != nil {
			panic(err)
		}
		ServerIp = ip
	}

	Flags = protocol.FLAG_ENABLE_WARMUP | protocol.FLAG_ENABLE_PERSISTENT
}

func IsWarmupWithFixedInterval() bool {
	return Flags&protocol.FLAG_FIXED_INTERVAL_WARMUP > 0
}

func GetLogger(prefix string) logger.ILogger {
	return &logger.ColorLogger{
		Prefix: prefix,
		Level:  Log.GetLevel(),
		Color:  !Options.NoColor,
	}
}

func SetLoggerLevel(level int) {
	if color, ok := Log.(*logger.ColorLogger); ok {
		color.Level = level
	}
}
