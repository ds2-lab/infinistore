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

type Meta struct {
	// Sequence of the last confirmed log. Logs store by sequence.
	Term     uint64 `json:"seq"`

	// Total transmission size for restoring all confirmed logs.
	Updates uint64 `json:"updates"`

	// Hash of the last confirmed log.
	Hash string    `json:"hash"`

	// Sequence of snapshot.
	SnapShotTerm uint64 `json:"snapshot"`

	// Total transmission size for restoring all confirmed logs from start to SnapShotSeq.
	SnapshotUpdates uint64 `json:"snapshotupdates"`

	// Total size of snapshot for transmission.
	SnapshotSize uint64 `json:"snapshotsize"`
}

const (
	FLAG_WARMUP_FIX_INTERVAL = 0x0001
	FLAG_REPLICATE_ON_WARMUP = 0x0002
	FLAG_REPLICATE_ON_INVOCATE = 0x0004
	FLAG_BACKUP_ON_SET = 0x0008
	FLAG_BACKUP_S3 = 0x000F
)
