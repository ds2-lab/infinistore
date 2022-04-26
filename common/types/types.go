package types

import "net"

// InputEvent Input for the Lambda
type InputEvent struct {
	Sid       string   `json:"sid"`   // Session id to ensure invoking once only.
	Cmd       string   `json:"cmd"`   // Invocation type, can be "ping", "warmup".
	Id        uint64   `json:"id"`    // Node id
	Proxy     string   `json:"proxy"` // Address of proxy .
	ProxyAddr net.Addr // Address of proxy, for simulation.
	Addr      string   `json:"addr"`   // Address of P2P relay.
	Prefix    string   `json:"prefix"` // Experiment id reserved for evaluation.
	Log       int      `json:"log"`    // Log level - debug or not.
	Flags     uint64   `json:"flags"`  // Feature flags
	Backups   int      `json:"baks"`   // Number of configured recovery nodes
	Status    Status   `json:"status"` // Lineage info
}

func (i *InputEvent) IsReplicaEnabled() bool {
	return (i.Flags & FLAG_ENABLE_REPLICA) > 0
}

func (i *InputEvent) IsPersistencyEnabled() bool {
	return (i.Flags & FLAG_ENABLE_PERSISTENT) > 0
}

func (i *InputEvent) IsRecoveryEnabled() bool {
	return (i.Flags & (FLAG_ENABLE_PERSISTENT | FLAG_DISABLE_RECOVERY)) == FLAG_ENABLE_PERSISTENT
}

func (i *InputEvent) IsBackingOnly() bool {
	return (i.Flags & FLAG_BACKING_ONLY) > 0
}

func (i *InputEvent) IsWaitForCOSDisabled() bool {
	return (i.Flags & FLAG_DISABLE_WAIT_FOR_COS) > 0
}

type Status struct {
	Capacity  uint64 `json:"cap"`
	Mem       uint64 `json:"mem"`
	Effective uint64 `json:"effe"`
	Modified  uint64 `json:"modi"`
	Metas     []Meta `json:"metas"`
}

type Meta struct {
	// Lambda ID
	Id uint64 `json:"id"`

	// Sequence of the last confirmed log. Logs store by sequence.
	Term uint64 `json:"term"`

	// Total transmission size for restoring all confirmed logs.
	Updates uint64 `json:"updates"`

	// Rank for lambda to decide if a fast recovery is required.
	DiffRank float64 `json:"diffrank"`

	// Hash of the last confirmed log.
	Hash string `json:"hash"`

	// Sequence of snapshot.
	SnapshotTerm uint64 `json:"snapshot"`

	// Total transmission size for restoring all confirmed logs from start to SnapShotSeq.
	SnapshotUpdates uint64 `json:"snapshotupdates"`

	// Total size of snapshot for transmission.
	SnapshotSize uint64 `json:"snapshotsize"`

	// Tips offer the lambda clues of how to recovery data. It is in http query format.
	Tip string `json:"tip"`
}

type ShortMeta struct {
	// Lambda ID
	Id uint64 `json:"id"`

	// Sequence of the last confirmed log. Logs store by sequence.
	Term uint64 `json:"term"`

	// Total transmission size for restoring all confirmed logs.
	Updates uint64 `json:"updates"`

	// Rank for lambda to decide if a fast recovery is required.
	DiffRank float64 `json:"diffrank"`

	// Hash of the last confirmed log.
	Hash string `json:"hash"`
}

type OutputError struct {
	Message string `json:"errorMessage"`
	Type    string `json:"errorType"`
}

func (e *OutputError) String() string {
	return e.Message
}

const (
	// FLAG_ENABLE_WARMUP Enable warmup.
	FLAG_ENABLE_WARMUP = 0x0001
	// FLAG_FIXED_INTERVAL_WARMUP Warming up with fixed interval regardless workload.
	FLAG_FIXED_INTERVAL_WARMUP = 0x0002

	// FLAG_ENABLE_REPLICA Enable replication.
	FLAG_ENABLE_REPLICA = 0x0010
	// FLAG_WARMUP_REPLICA Replication will be triggered on warming up.
	FLAG_WARMUP_REPLICA = 0x0020

	// FLAG_ENABLE_PERSISTENT Enable persist.
	FLAG_ENABLE_PERSISTENT = 0x0100
	// FLAG_DISABLE_RECOVERY Disable recovery on reclaimation.
	FLAG_DISABLE_RECOVERY = 0x0200

	// FLAG_BACKING_ONLY Disable recovery for main repository
	FLAG_BACKING_ONLY = 0x1000
	// FLAG_DISABLE_WAIT_FOR_COS Disable waiting for COS on PUT chunks.
	FLAG_DISABLE_WAIT_FOR_COS = 0x2000

	// PONG_FOR_DATA Pong for data link
	PONG_FOR_DATA = int64(0x0000)
	// PONG_FOR_CTRL Pong for ctrl link
	PONG_FOR_CTRL = int64(0x0001)

	// PONG_ON_INVOKING Pong issued on invoking
	PONG_ON_INVOKING = int64(0x0010)
	// PONG_RECOVERY Pong with parallel recovery requested
	PONG_RECOVERY = int64(0x0020)
	// PONG_RECLAIMED Pong with claiming the node has experienced reclaimation (backing mode only).
	PONG_RECLAIMED = int64(0x0040)
	// PONG_WITH_PAYLOAD Pong with piggyback payload.
	PONG_WITH_PAYLOAD = int64(0x0080)

	// PONG_RECONCILE Pong with reconcile meta included.
	PONG_RECONCILE = int64(0x0100)

	CMD_TEST        = "test"
	CMD_ACK         = "ack"         // Control command
	CMD_GET         = "get"         // Redis and Lambda command
	CMD_GET_CHUNK   = "get chunk"   // Client command
	CMD_SET         = "set"         // Redis and Lambda command
	CMD_SET_CHUNK   = "set chunk"   // Client command
	CMD_RECOVER     = "recover"     // Control command
	CMD_DEL         = "del"         // Control command
	CMD_WARMUP      = "warmup"      // Control command
	CMD_PING        = "ping"        // Control command
	CMD_PONG        = "pong"        // Control command
	CMD_RECOVERED   = "recovered"   // Control command
	CMD_INITMIGRATE = "initMigrate" // Control command
	CMD_MIGRATE     = "migrate"     // Control command
	CMD_MHELLO      = "mhello"      // Control command
	CMD_DATA        = "data"        // Control command
	CMD_BYE         = "bye"         // Control command

	REQUEST_GET_OPTIONAL      = 0x0001 // Flag response is optional. There is a compete fallback will eventually fulfill the request.
	REQUEST_GET_OPTION_BUFFER = 0x0002 // Flag the chunk should be put in buffer area.

	// TIP_BACKUP_KEY Backup ID.
	TIP_BACKUP_KEY = "bak"
	// TIP_BACKUP_TOTAL Total backups available.
	TIP_BACKUP_TOTAL = "baks"
	// TIP_DELEGATE_KEY Delegate ID.
	TIP_DELEGATE_KEY = "dele"
	// TIP_DELEGATE_TOTAL Total delegates available.
	TIP_DELEGATE_TOTAL = "deles"
	// TIP_SERVING_KEY Key should be recovered as the first one.
	TIP_SERVING_KEY = "key"
	// TIP_MAX_CHUNK Max chunk size can be backed up.
	TIP_MAX_CHUNK = "max"
)
