package types

import (
	"net/url"
	"strconv"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type LineageMetaType int

const (
	LineageMetaTypeMain LineageMetaType = iota
	LineageMetaTypeBackup
	LineageMetaTypeDelegate
)

type LineageValidationResult int

const (
	LineageValidationInconsistent LineageValidationResult = iota
	LineageValidationConsistent
	LineageValidationConsistentWithHistoryTerm
)

func LineageValidationResultFromConsistent(consistent bool) LineageValidationResult {
	if consistent {
		return LineageValidationConsistent
	}
	return LineageValidationInconsistent
}

func (ret LineageValidationResult) IsConsistent() bool {
	return ret > LineageValidationInconsistent
}

func (t LineageMetaType) String() string {
	switch t {
	case LineageMetaTypeMain:
		return "main"
	case LineageMetaTypeBackup:
		return "backup"
	case LineageMetaTypeDelegate:
		return "delegate"
	default:
		return "unknown"
	}
}

type LineageMeta struct {
	*protocol.Meta
	Consistent   bool
	Type         LineageMetaType
	BackupId     int
	BackupTotal  int
	MaxChunkSize uint64
	Tips         url.Values
}

func LineageMetaFromProtocol(meta *protocol.Meta) (lm *LineageMeta, err error) {
	lm = &LineageMeta{Meta: meta, Consistent: true, Type: LineageMetaTypeMain}

	// Parse tips
	lm.Tips, err = url.ParseQuery(meta.Tip)
	if err != nil {
		return
	}

	// Parse backup id
	if sId := lm.Tips.Get(protocol.TIP_BACKUP_KEY); sId != "" {
		lm.Type = LineageMetaTypeBackup
		lm.BackupId, _ = strconv.Atoi(sId)
		lm.BackupTotal, _ = strconv.Atoi(lm.Tips.Get(protocol.TIP_BACKUP_TOTAL))
		lm.MaxChunkSize, _ = strconv.ParseUint(lm.Tips.Get(protocol.TIP_MAX_CHUNK), 10, 64)
	} else if sId := lm.Tips.Get(protocol.TIP_DELEGATE_KEY); sId != "" {
		lm.Type = LineageMetaTypeDelegate
		lm.BackupId, _ = strconv.Atoi(sId)
		lm.BackupTotal, _ = strconv.Atoi(lm.Tips.Get(protocol.TIP_DELEGATE_TOTAL))
		lm.MaxChunkSize, _ = strconv.ParseUint(lm.Tips.Get(protocol.TIP_MAX_CHUNK), 10, 64)
	}

	return
}

func (meta *LineageMeta) ServingKey() string {
	return meta.Tips.Get(protocol.TIP_SERVING_KEY)
}

type Lineage interface {
	// Validate validates the lineage, it will call the IsConsistent to check if the lineage is consistent.
	Validate(*LineageMeta) (LineageValidationResult, error)

	// IsConsistent checks if the lineage is consistent.
	IsConsistent(*LineageMeta) (bool, error)

	// ClearBackup clears the backup data.
	ClearBackup()

	// Commit commits the lineage to the COS.
	Commit() (*CommitOption, error)

	// Recover recovers data from the COS by the given lineage.
	Recover(*LineageMeta) (bool, <-chan error)

	// Status returns the status of the lineage.
	// Parameter short: returns simplified status if passes true.
	Status(bool) (confirmed uint64, status LineageStatus)
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
	Buffered int     // For snapshot only, number of buffered objects.
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
	BIdx     int // Index in bufferQueue
}

type OpWrapper struct {
	LineageOp
	*OpRet
	Body      []byte // For safety of persistence of the SET operation in the case like DEL after SET.
	Chunk     *Chunk
	OpIdx     int
	Persisted bool // Indicate the operation has been persisted.
	Accessed  bool // Indicate the access time should not be changed.
	Sized     bool // Indicate the size of storage has been updated.
}

type CommitOption struct {
	Full               bool
	Snapshotted        bool
	BytesUploaded      uint64
	Checked            bool
	StorageSignalFlags uint32
}

func (opts *CommitOption) Flags() uint32 {
	return opts.StorageSignalFlags
}

type LineageStatus []*protocol.Meta

func (s LineageStatus) ProtocolStatus() protocol.Status {
	// For efficiency, we hard code usual cases: len 1 and len 2.
	switch len(s) {
	case 0:
		return protocol.Status{}
	case 1:
		return protocol.Status{Metas: []protocol.Meta{*s[0]}}
	case 2:
		return protocol.Status{Metas: []protocol.Meta{*s[0], *s[1]}}
	default:
		metas := make([]protocol.Meta, len(s))
		for i := 0; i < len(s); i++ {
			metas[i] = *s[i]
		}
		return protocol.Status{Metas: metas}
	}
}

func (s LineageStatus) ShortStatus() *protocol.ShortMeta {
	// For efficiency, we hard code usual cases: len 1 and len 2.
	switch len(s) {
	case 0:
		return nil
	default:
		return &protocol.ShortMeta{
			Id:       s[0].Id,
			Term:     s[0].Term,
			Updates:  s[0].Updates,
			DiffRank: s[0].DiffRank,
			Hash:     s[0].Hash,
		}
	}
}

type OpRet struct {
	error
	delayed chan struct{}
	msg     string
}

func OpError(err error) *OpRet {
	return &OpRet{err, nil, ""}
}

func OpErrorWithMessage(err error, msg string) *OpRet {
	return &OpRet{err, nil, msg}
}

func OpSuccess() *OpRet {
	return &OpRet{nil, nil, ""}
}

func OpDelayedSuccess() *OpRet {
	return &OpRet{nil, make(chan struct{}, 1), ""}
}

func (ret *OpRet) Error() error {
	return ret.error
}

func (ret *OpRet) Message() string {
	return ret.msg
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
