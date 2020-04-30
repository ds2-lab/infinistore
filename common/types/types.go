package types

// InputEvent Input for the Lambda
type InputEvent struct {
	Sid     string `json:"sid"`  // session id.
	Cmd     string `json:"cmd"`
	Id      uint64 `json:"id"`
	Proxy   string `json:"proxy"`
	Addr    string `json:"addr"`
	Prefix  string `json:"prefix"`
	Log     int    `json:"log"`
	Flags   uint64 `json:"flags"`
	Backups int    `json:"baks"`
	Status  Status `json:"status"`
}

func (i *InputEvent) IsReplicaEnabled() bool {
	return (i.Flags & FLAG_ENABLE_REPLICA) > 0
}

func (i *InputEvent) IsPersistentEnabled() bool {
	return (i.Flags & FLAG_ENABLE_PERSISTENT) > 0
}

func (i *InputEvent) IsBackingOnly() bool {
	return (i.Flags & FLAG_BACKING_ONLY) > 0
}

type Status []Meta

type Meta struct {
	// Lambda ID
	Id       uint64 `json:"id"`

	// Sequence of the last confirmed log. Logs store by sequence.
	Term     uint64 `json:"term"`

	// Total transmission size for restoring all confirmed logs.
	Updates uint64 `json:"updates"`

	// Rank for lambda to decide if a fast recovery is required.
	DiffRank float64 `json:"diffrank"`

	// Hash of the last confirmed log.
	Hash string    `json:"hash"`

	// Sequence of snapshot.
	SnapshotTerm uint64 `json:"snapshot"`

	// Total transmission size for restoring all confirmed logs from start to SnapShotSeq.
	SnapshotUpdates uint64 `json:"snapshotupdates"`

	// Total size of snapshot for transmission.
	SnapshotSize uint64 `json:"snapshotsize"`

	// Tips offer the lambda clues of how to recovery data. It is in http query format.
	Tip string `json:"tip"`
}

type OutputError struct {
	Message    string `json:"errorMessage"`
	Type       string `json:"errorType"`
}

func (e *OutputError) String() string {
	return e.Message
}

const (
	// FLAG_ENABLE_WARMUP Enable warmup.
	FLAG_ENABLE_WARMUP = 0x0001
	// FLAG_FIXED_INTERVAL_WARMUP Warming up with fixed interval regardless workload.
	FLAG_FIXED_INTERVAL_WARMUP = 0x0003
	// FLAG_ENABLE_REPLICA Enable replication.
	FLAG_ENABLE_REPLICA = 0x0010
	// FLAG_WARMUP_REPLICA Replication will be triggered on warming up.
	FLAG_WARMUP_REPLICA = 0x0030
	// FLAG_ENABLE_PERSISTENT Enable persist.
	FLAG_ENABLE_PERSISTENT = 0x0100
	// FLAG_EXPIRED Disable recovery for main repository
	FLAG_BACKING_ONLY = 0x1000

	// PONG_RECOVERY Pong with parallel recovery requested
	PONG_RECOVERY = 0x0001
	// PONG_RECLAIMED Pong with claiming the node has experienced reclaimation (backing mode only).
	PONG_RECLAIMED = 0x0002

	CMD_GET = "get"              // Redis and Lambda command
	CMD_GET_CHUNK = "get chunk"  // Client command
	CMD_SET = "set"              // Redis and Lambda command
	CMD_SET_CHUNK = "set chunk"  // Client command
	CMD_RECOVER = "recover"      // Control command
	CMD_DEL = "del"              // Control command
	CMD_WARMUP = "warmup"        // Control command
	CMD_PING = "ping"            // Control command
	CMD_POND = "pong"            // Control command
	CMD_RECOVERED = "recovered"  // Control command
	CMD_INITMIGRATE = "initMigrate" // Control command
	CMD_MIGRATE = "migrate"      // Control command
	CMD_MHELLO = "mhello"        // Control command
	CMD_DATA = "data"            // Control command
	CMD_BYE = "bye"              // Control command

	// TIP_BACKUP_KEY Backup ID.
	TIP_BACKUP_KEY = "bak"
	// TIP_BACKUP_TOTAL Total backups available.
	TIP_BACKUP_TOTAL = "baks"
	// TIP_SERVING_KEY Key should be recovered as the first one.
	TIP_SERVING_KEY = "key"
)
