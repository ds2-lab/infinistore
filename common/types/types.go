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
	FLAG_ENABLE_REPLICA = 0x0001
	FLAG_WARMUP_REPLICA = 0x0002
	FLAG_ENABLE_PERSISTENT = 0x0010

	TIP_ID = "id"
	TIP_SERVING_KEY = "key"
)
