package types

import (
	"net/url"
	"strconv"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type LineageMeta struct {
	*protocol.Meta
	Consistent  bool
	Backup      bool
	BackupId    int
	BackupTotal int
	Tips        url.Values
}

func LineageMetaFromProtocol(meta *protocol.Meta) (lm *LineageMeta, err error) {
	lm = &LineageMeta{Meta: meta, Consistent: true}

	// Parse tips
	lm.Tips, err = url.ParseQuery(meta.Tip)
	if err != nil {
		return
	}

	// Parse backup id
	if sId := lm.Tips.Get(protocol.TIP_BACKUP_KEY); sId != "" {
		lm.Backup = true
		lm.BackupId, err = strconv.Atoi(sId)
		if err != nil {
			return
		}
		lm.BackupTotal, err = strconv.Atoi(lm.Tips.Get(protocol.TIP_BACKUP_TOTAL))
		if err != nil {
			return
		}
	}

	return
}

func (meta *LineageMeta) ServingKey() string {
	return meta.Tips.Get(protocol.TIP_SERVING_KEY)
}

type Lineage interface {
	IsConsistent(*LineageMeta) (bool, error)
	ClearBackup()
	Commit() (*CommitOption, error)
	Recover(*LineageMeta) (bool, <-chan error)
	Status() LineageStatus
}

type LineageTerm struct {
	Size uint64      // Storage footprint of storing current term.
	Ops  []LineageOp // Operations of the term.

	// Fields below will be serialized.
	Term     uint64  // Term id, increase on write operation.
	Updates  uint64  // Storage footprint of storing all terms of lineage so far.
	RawOps   []byte  // Serialized "Ops"
	Hash     string  // Hash value for the term.
	DiffRank float64 // For snapshot only, this is supposed to be a moving value.
}

func LineageTermFromMeta(meta *LineageMeta) *LineageTerm {
	if meta == nil {
		return &LineageTerm{Term: 1}
	}
	return &LineageTerm{
		Term:    meta.Meta.Term,
		Updates: meta.Meta.Updates,
		Hash:    meta.Meta.Hash,
	}
}

type LineageOp struct {
	Op       uint32 // Operation, can be "OP_SET" or "OP_DEL"
	Key      string // Key of the object
	Id       string // Chunk id of the object
	Size     uint64 // Size of the object
	Accessed time.Time
	Bucket   string
}

type OpWrapper struct {
	LineageOp
	*OpRet
	Body      []byte // For safety of persistence of the SET operation in the case like DEL after SET.
	OpIdx     int
	Persisted bool // Indicate the operation has been persisted.
}

type CommitOption struct {
	Full          bool
	Snapshotted   bool
	BytesUploaded uint64
	Checked       bool
}

type LineageStatus []*protocol.Meta

func (s LineageStatus) ProtocolStatus() protocol.Status {
	// For efficiency, we hard code usual cases: len 1 and len 2.
	switch len(s) {
	case 0:
		return protocol.Status{}
	case 1:
		return protocol.Status{*s[0]}
	case 2:
		return protocol.Status{*s[0], *s[1]}
	default:
		status := make(protocol.Status, len(s))
		for i := 0; i < len(s); i++ {
			status[i] = *s[i]
		}
		return status
	}
}

type OpRet struct {
	error
	delayed chan struct{}
}

func OpError(err error) *OpRet {
	return &OpRet{err, nil}
}

func OpSuccess() *OpRet {
	return &OpRet{nil, nil}
}

func OpDelayedSuccess() *OpRet {
	return &OpRet{nil, make(chan struct{}, 1)}
}

func (ret *OpRet) Error() error {
	return ret.error
}

func (ret *OpRet) IsDelayed() bool {
	return ret.delayed != nil
}

// Conclude OpRet. Noted error is evaluated before notifing "delayed" channel, so no lock is required for IsDone or Wait.
func (ret *OpRet) Done(err ...error) {
	if ret.delayed == nil {
		return
	}

	if len(err) > 0 {
		ret.error = err[0]
	}
	close(ret.delayed)
}

func (ret *OpRet) IsDone() bool {
	if ret.delayed == nil {
		return true
	}

	select {
	case <-ret.delayed:
		return true
	default:
		return false
	}
}

// Behavior like the Promise in javascript.
// Allow blocking wait or return last result if delayed is closed.
func (ret *OpRet) Wait() error {
	if ret.delayed == nil {
		return nil
	}

	<-ret.delayed
	return ret.error
}
