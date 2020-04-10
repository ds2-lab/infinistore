package types

// Input
type InputEvent struct {
	Cmd     string `json:"cmd"`
	Id      uint64 `json:"id"`
	Proxy   string `json:"proxy"`
	Timeout int    `json:"timeout"`
	Addr    string `json:"addr"`
	Prefix  string `json:"prefix"`
	Log     int    `json:"log"`
	Flags   uint64 `json:"flags"`
	Meta    Meta   `json:"meta"`
}

func (i *InputEvent) IsReplicaEnabled() bool {
	return (i.Flags & FLAG_ENABLE_REPLICA) > 0
}

func (i *InputEvent) IsPersistentEnabled() bool {
	return (i.Flags & FLAG_ENABLE_PERSISTENT) > 0
}

type Meta struct {
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

const (
	// Enable warmup.
	FLAG_ENABLE_WARMUP = 0x0001
	// Warming up with fixed interval regardless workload.
	FLAG_FIXED_INTERVAL_WARMUP = 0x0003
	// Enable replication.
	FLAG_ENABLE_REPLICA = 0x0010
	// Replication will be triggered on warming up.
	FLAG_WARMUP_REPLICA = 0x0030
	// Enable persist.
	FLAG_ENABLE_PERSISTENT = 0x0100

	CMD_GET = "get"
	CMD_SET = "set"
	CMD_DEL = "del"
	CMD_WARMUP = "warmup"
	CMD_MIGRATE = "migrate"
	CMD_DATA = "data"

	// Backup ID.
	TIP_ID = "id"
	// Key should be recovered as the first one.
	TIP_SERVING_KEY = "key"
)
