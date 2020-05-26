package types

// InputEvent Input for the Lambda
type InputEvent struct {
	Sid     string `json:"sid"`     // Session id to ensure invoking once only.
	Cmd     string `json:"cmd"`     // Invocation type, can be "ping", "warmup".
	Id      uint64 `json:"id"`      // Node id
	Proxy   string `json:"proxy"`   // Address of proxy .
	Addr    string `json:"addr"`    // Address of P2P relay.
	Prefix  string `json:"prefix"`  // Experiment id reserved for evaluation.
	Log     int    `json:"log"`     // Log level - debug or not.
	Flags   uint64 `json:"flags"`   // Feature flags
	Backups int    `json:"baks"`    // Number of configured recovery nodes
	Status  Status `json:"status"`  // Lineage info
}

func (i *InputEvent) IsReplicaEnabled() bool {
	return (i.Flags & FLAG_ENABLE_REPLICA) > 0
}

func (i *InputEvent) IsPersistencyEnabled() bool {
	return (i.Flags & FLAG_ENABLE_PERSISTENT) > 0
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

	CMD_GET = "get"
	CMD_GET_CHUNK = "get chunk"
	CMD_SET = "set"
	CMD_SET_CHUNK = "set chunk"
	CMD_DEL = "del"
	CMD_WARMUP = "warmup"
	CMD_PING = "ping"
	CMD_POND = "pong"
	CMD_RECOVERED = "recovered"
	CMD_INITMIGRATE = "initMigrate"
	CMD_MIGRATE = "migrate"
	CMD_MHELLO = "mhello"
	CMD_DATA = "data"
	CMD_BYE = "bye"

	// TIP_BACKUP_KEY Backup ID.
	TIP_BACKUP_KEY = "bak"
	// TIP_BACKUP_TOTAL Total backups available.
	TIP_BACKUP_TOTAL = "baks"
	// TIP_SERVING_KEY Key should be recovered as the first one.
	TIP_SERVING_KEY = "key"
)
