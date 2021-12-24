package storage

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	// "strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/kelindar/binary"
	csync "github.com/mason-leap-lab/infinicache/common/sync"
	"github.com/zhangjyr/hashmap"

	mys3 "github.com/mason-leap-lab/infinicache/common/aws/s3"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	LINEAGE_KEY  = "%s%s/lineage-%d"
	SNAPSHOT_KEY = "%s%s/snapshot-%d.gz"

	RECOVERING_NONE   uint32 = 0x00
	RECOVERING_MAIN   uint32 = 0x01
	RECOVERING_BACKUP uint32 = 0x02

	LineageStorageOverhead    = StorageOverhead // Reuse StorageOverhead
	BackupStoreageReservation = 0.1             // 1/N * 2, N = 20, backups per lambda.
)

var (
	Backups = 10

	// Updated: 6/30/2010
	// Try download as few as lineage file in batch: try one snapshot each term
	SnapshotInterval = uint64(1)
	// To minimize lineage recovery latency, try download the same file with multiple contenders.
	LineageRecoveryContenders = 3

	// Errors
	ErrRecovering          = errors.New("already recovering")
	ErrRecovered           = errors.New("already recovered")
	ErrRecoveryInterrupted = errors.New("recovery interrupted")
	ErrBackupSetForbidden  = errors.New("forbidden to set backup objects")
)

// Storage with lineage
type LineageStorage struct {
	*PersistentStorage

	// Lineage
	lineage   *types.LineageTerm // The lineage of current/recent term. The lineage is updated to recent term while recovering.
	recovered *types.LineageTerm // Stores recovered lineage if it is not fully recovered, and will replace lineage on returning.
	snapshot  *types.LineageTerm // The latest snapshot of the lineage.
	diffrank  LineageDifferenceRank
	getSafe   csync.WaitGroup
	setSafe   csync.WaitGroup
	safenote  uint32       // Flag what's going on
	lineageMu sync.RWMutex // Mutex for lienage commit.
	commited  chan interface{}

	// backup
	backup                  *hashmap.HashMap   // Just a index, all will be available to repo
	backupLineage           *types.LineageMeta // Only one backup is effective at a time.
	backupLocator           protocol.BackupLocator
	backupRecoveryCanceller context.CancelFunc

	// buffer delegates
	bufferMeta  StorageMeta
	bufferQueue *ChunkQueue
}

func NewLineageStorage(id uint64, cap uint64) *LineageStorage {
	storage := &LineageStorage{
		PersistentStorage: NewPersistentStorage(id, cap),
		lineage: &types.LineageTerm{
			Term: 1,                             // Term start with 1 to avoid uninitialized term ambigulous.
			Ops:  make([]types.LineageOp, 0, 1), // We expect 1 "write" maximum for each term for sparse workload.
		},
		backup:   hashmap.New(1000), // Initilize early to buy time for fast backup recovery.
		diffrank: NewSimpleDifferenceRank(Backups),
	}
	storage.meta.Overhead = LineageStorageOverhead
	storage.meta.Rsrved = uint64(float64(storage.meta.Cap) * BackupStoreageReservation)
	storage.helper = storage
	storage.persistHelper = storage
	storage.meta.modifier = &storage.bufferMeta
	return storage
}

// Storage Implementation
func (s *LineageStorage) getWithOption(key string, opt *types.OpWrapper) (*types.Chunk, *types.OpRet) {
	// Before getting safely, try what is available so far.
	chunk, ok := s.helper.get(key)
	// !ok: Most likely, this is because an imcomplete lineage.
	// not safe: Corresponding lineage is recovering, and the chunk is not just set
	if !ok || !s.isSafeToGet(chunk) {
		s.getSafe.Wait()
	}

	// Ask to not update chunk.Accessed.
	if opt == nil {
		opt = &types.OpWrapper{}
	}
	opt.Accessed = true

	chunk, ret := s.PersistentStorage.getWithOption(key, opt)
	if ret.Error() != nil {
		return chunk, ret
	} else if chunk.IsBuffered(false) {
		// Increase the priority to be evicted, since the key will be migrated to hot node.
		chunk.Accessed = time.Time{}
		s.bufferFix(chunk)
	} else {
		// Make up for other objects.
		chunk.Access()
	}

	return chunk, ret
}

func (s *LineageStorage) setWithOption(key string, chunk *types.Chunk, opt *types.OpWrapper) *types.OpRet {
	if chunk.Backup {
		return types.OpError(ErrBackupSetForbidden)
	}

	s.setSafe.Wait()
	// s.log.Debug("ready to set key %v", key)
	// Lock lineage, ensure operation get processed in the term.
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()
	// s.log.Debug("in mutex of setting key %v", key)

	// Oversize check.
	if updatedOpt, ok := s.helper.validate(chunk, opt); ok {
		opt = updatedOpt
	} else {
		return types.OpError(ErrOOStorage)
	}

	return s.PersistentStorage.setWithOption(key, chunk, opt)
}

func (s *LineageStorage) newChunk(key string, chunkId string, size uint64, val []byte) *types.Chunk {
	chunk := types.NewChunk(key, chunkId, val)
	chunk.Size = size
	chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
	chunk.Bucket = s.getBucket(key)
	return chunk
}

func (s *LineageStorage) validate(test *types.Chunk, opt *types.OpWrapper) (*types.OpWrapper, bool) {
	if test.IsBuffered(true) {
		s.bufferInit(100)
		s.bufferAdd(test)
	}

	size := s.meta.IncreaseSize(test.Size)                      // Oversize test.
	for size >= s.meta.Effective() && s.bufferMeta.Size() > 0 { // No need to init buffer if test is not to be buffered, bufferMeta.Size() would be 0 in this case.
		evicted := heap.Pop(s.bufferQueue).(*types.Chunk)
		s.bufferMeta.DecreaseSize(evicted.Size)
		// If validate is called iteratively, make sure iterate test chunks using reversed heap.
		// RU of to be delegated is before LRU of delegated already. Stop.
		if evicted == test {
			break
		}

		// evicted must be objects previously in buffer.
		s.PersistentStorage.delWithOption(evicted, "eviction", nil)
		size = s.meta.Size()
	}
	if size < s.meta.Effective() {
		if opt == nil {
			opt = &types.OpWrapper{}
		}
		opt.Sized = true // Size update has been dealed for both backups and nonbackups.
		return opt, true
	} else {
		s.meta.DecreaseSize(test.Size) // Reset meta size.
		return opt, false
	}
}

func (s *LineageStorage) Del(key string, reason string) *types.OpRet {
	s.getSafe.Wait()

	chunk, ok := s.helper.get(key)
	if !ok {
		return types.OpError(types.ErrNotFound)
	}

	s.setSafe.Wait()

	// Lock lineage
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
	return s.helper.delWithOption(chunk, reason, nil)
}

func (s *LineageStorage) Len() int {
	s.getSafe.Wait()

	return s.repo.Len()
}

func (s *LineageStorage) Keys() <-chan string {
	s.getSafe.Wait()

	return s.PersistentStorage.Keys()
}

// Lineage Implementation
func (s *LineageStorage) ConfigS3(bucket string, prefix string) {
	s.PersistentStorage.ConfigS3(bucket, prefix)
	s.setSafe = csync.WaitGroup{}
	s.getSafe = csync.WaitGroup{}
	s.delaySet()
}

func (s *LineageStorage) IsConsistent(meta *types.LineageMeta) (bool, error) {
	lineage := s.lineage
	switch meta.Type {
	case types.LineageMetaTypeBackup:
		if s.backupLineage == nil || s.backupLineage.Id != meta.Id || s.backupLineage.BackupId != meta.BackupId || s.backupLineage.BackupTotal != meta.BackupTotal {
			meta.Consistent = false
			return meta.Consistent, nil
		}
		lineage = types.LineageTermFromMeta(s.backupLineage)
	case types.LineageMetaTypeDelegate:
		meta.Consistent = false
		return meta.Consistent, nil
	}

	// Assertion: lineage exists.
	if lineage.Term > meta.Term {
		// meta.Consistent = false
		// return meta.Consistent, fmt.Errorf("detected staled term of lambda %d, expected at least %d, have %d", meta.Id, lineage.Term, meta.Term)
		// TODO: ignore for now.
		meta.Consistent = true
		return meta.Consistent, nil
	}
	// Don't check hash if term is the start term(1).
	meta.Consistent = lineage.Term == meta.Term && (meta.Term == 1 || lineage.Hash == meta.Hash)
	return meta.Consistent, nil
}

func (s *LineageStorage) StartTracker() {
	if s.chanOps == nil {
		s.lineage.Ops = s.lineage.Ops[:0] // Reset metalogs
		s.resetSet()
		s.commited = make(chan interface{})
		s.PersistentStorage.StartTracker()
		return
	}

	s.log.Error("You should not have seen this error for here is unreachable.")
}

func (s *LineageStorage) onPersisted(persisted *types.OpWrapper) {
	s.lineage.Ops = append(s.lineage.Ops, persisted.LineageOp)

	// If lineage is not recovered (get unsafe), skip diffrank, it will be replay when lineage is recovered.
	if !s.getSafe.IsWaiting() {
		s.diffrank.AddOp(&persisted.LineageOp)
	} // else: Skip
}

func (s *LineageStorage) onSignalTracker(signal interface{}) bool {
	switch option := signal.(type) {
	case *types.CommitOption:
		return s.doCommit(option)
	default:
		return s.PersistentStorage.onSignalTracker(signal)
	}
}

func (s *LineageStorage) Commit() (*types.CommitOption, error) {
	if s.signalTracker == nil {
		return nil, ErrTrackerNotStarted
	}

	s.log.Debug("Commiting lineage...")

	// Initialize option for committing.
	option := &types.CommitOption{}

	// Are we goint to do the snapshot?
	var snapshotTerm uint64
	if s.snapshot != nil {
		snapshotTerm = s.snapshot.Term
	}
	// For incompleted lineage recovery (s.recovered != nil), do not snapshot.
	// On local laptop,
	// it takes 1509 bytes and 3ms to snapshot 500 chunks and 125 bytes to persist one term, both gzipped,
	// and it takes roughly same time to download a snapshot or up to 10 lineage terms.
	// So snapshot every 5 - 10 terms will be appropriate.
	option.Full = s.recovered == nil && s.lineage.Term-snapshotTerm >= SnapshotInterval-1

	// Signal and wait for committed.
	s.log.Debug("Signal tracker to commit")
	s.signalTracker <- option
	option = (<-s.commited).(*types.CommitOption)

	// Flag checked
	return option, nil
}

func (s *LineageStorage) StopTracker() {
	if s.signalTracker != nil {
		s.PersistentStorage.StopTracker()
		s.commited = nil
		if s.recovered != nil {
			// The recovery is not complete, discard current term and replaced with whatever recovered.
			// The node will try recovery in next invocation.
			s.lineage = s.recovered
		}
	}
}

func (s *LineageStorage) Status() types.LineageStatus {
	meta := &protocol.Meta{
		Id:       s.id,
		Term:     s.lineage.Term,
		Updates:  s.lineage.Updates,
		DiffRank: s.diffrank.Rank(),
		Hash:     s.lineage.Hash,
	}
	if s.snapshot != nil {
		meta.SnapshotTerm = s.snapshot.Term
		meta.SnapshotUpdates = s.snapshot.Updates
		meta.SnapshotSize = s.snapshot.Size
	}
	if s.backupLineage != nil {
		return types.LineageStatus{meta, s.backupLineage.Meta}
	} else {
		return types.LineageStatus{meta}
	}
}

// Recover based on the term of specified meta.
// We support partial recovery. Errors during recovery will be sent to returned channel.
// The recovery ends if returned channel is closed.
// If the first return value is false, no fast recovery is needed.
func (s *LineageStorage) Recover(meta *types.LineageMeta) (bool, <-chan error) {
	if meta.Consistent {
		return false, nil
	}

	// Accessing of delegated objects are optional and no safety guranteee.
	s.log.Info("Start recovery of node %d(type:%v).", meta.Meta.Id, meta.Type)
	chanErr := make(chan error, 1)
	recoverFlag := s.getRecoverFlag(meta)
	// Flag get as unsafe
	if !s.delayGet(recoverFlag) {
		chanErr <- ErrRecovering
		return false, chanErr
	}
	// Double check consistency
	consistent, err := s.IsConsistent(meta)
	if err != nil {
		s.resetGet(recoverFlag)
		chanErr <- err
		return false, chanErr
	} else if consistent {
		s.resetGet(recoverFlag)
		chanErr <- ErrRecovered
		return false, chanErr
	}

	// Copy lineage data for recovery, update term to the recent record, and we are ready for write operatoins.
	var old *types.LineageTerm
	if meta.Type == types.LineageMetaTypeMain {
		old = &types.LineageTerm{
			Term:    s.lineage.Term,
			Updates: s.lineage.Updates,
		}
		s.lineage.Term = meta.Term
		s.lineage.Updates = meta.Updates
		s.lineage.Hash = meta.Hash
		s.log.Info("During recovery, write operations enabled at term %d", s.lineage.Term+1)
	} else if meta.Type == types.LineageMetaTypeBackup {
		if s.backupLineage != nil &&
			s.backupLineage.Meta.Id == meta.Meta.Id &&
			s.backupLineage.BackupId == meta.BackupId &&
			s.backupLineage.BackupTotal == meta.BackupTotal {
			// Compare metas of backups for the same lambda
			old = types.LineageTermFromMeta(s.backupLineage)
		} else {
			// New backup lambda
			old = types.LineageTermFromMeta(nil)
			if s.backupLineage != nil && s.backupLineage.Meta.Id != meta.Meta.Id {
				s.log.Info("Backup data of node %d cleared to serve %d.", s.backupLineage.Meta.Id, meta.Meta.Id)
				// Clean obsolete backups
				if s.backupRecoveryCanceller != nil {
					s.backupRecoveryCanceller()
				}
				s.ClearBackup()
			}
		}
	} // No old lineage for delegate objects. They will be merged into main and evictable.

	ctx := context.Background()
	if meta.Type == types.LineageMetaTypeBackup {
		newCtx, cancel := context.WithCancel(ctx)
		ctx = newCtx
		s.backupRecoveryCanceller = func() {
			// before canceller take affect, resetGet is called at the end of replaying lineage.
			s.backupRecoveryCanceller = nil
			cancel()
		}
	}
	go s.doRecover(ctx, old, meta, chanErr)

	// Fast recovery if the node is not backup and significant enough.
	return meta.Type == types.LineageMetaTypeMain && s.diffrank.IsSignificant(meta.DiffRank), chanErr
}

func (s *LineageStorage) ClearBackup() {
	// Batch cleanup, no size need to be updated.
	for keyValue := range s.backup.Iter() {
		chunk := keyValue.Value.(*types.Chunk)
		chunk.Body = nil
		s.repo.Del(keyValue.Key)
		s.backup.Del(keyValue.Key)
	}
	s.backupLineage = nil
	s.meta.bakSize = 0
}

func (s *LineageStorage) doCommit(opt *types.CommitOption) bool {
	if len(s.lineage.Ops) > 0 {
		var termBytes, ssBytes int
		uploader := s3manager.NewUploader(types.AWSSession(), func(u *s3manager.Uploader) {
			u.Concurrency = 1
		})

		start := time.Now()
		lineage, term, err := s.doCommitTerm(s.lineage, uploader)
		s.lineage = lineage
		stop1 := time.Now()
		if err != nil {
			s.log.Warn("Failed to commit term %d: %v", term, err)
		} else {
			termBytes = int(s.lineage.Size)

			if !opt.Full || opt.Snapshotted {
				// pass
			} else if snapshot, err := s.doSnapshot(s.lineage, uploader); err != nil {
				s.log.Warn("Failed to snapshot up to term %d: %v", term, err)
			} else {
				ssBytes = int(snapshot.Size)
				s.snapshot = snapshot
				opt.Snapshotted = true
			}
		}
		end := time.Now()

		// Can be other operations during persisting, signal tracker again.
		// This time, ignore argument "full" if snapshotted.
		s.log.Info("Term %d commited, resignal to check possible new term during committing.", term)
		s.log.Trace("action,lineage,snapshot,elapsed,bytes")
		s.log.Trace("commit,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop1), end.Sub(start), termBytes+ssBytes)
		collector.AddCommit(
			start, types.OP_COMMIT, 0, s.id, int(term),
			stop1.Sub(start), end.Sub(stop1), end.Sub(start), termBytes, ssBytes)
		opt.BytesUploaded += uint64(termBytes + ssBytes)
		opt.Checked = true
		s.signalTracker <- opt
	} else {
		// No operation since last signal.This will be quick and we are ready to exit lambda.
		// DO NOT close "committed", since there will be a double check on stoping the tracker.
		if opt.Checked {
			s.log.Info("Double checked: no more term to commit, signal committed.")
		} else {
			s.log.Info("Checked: no term to commit, signal committed.")
		}
		s.commited <- opt
	}
	return false
}

func (s *LineageStorage) doCommitTerm(lineage *types.LineageTerm, uploader *s3manager.Uploader) (*types.LineageTerm, uint64, error) {
	// Lock local lineage
	s.lineageMu.Lock()

	// Marshal ops first, so it can be reused largely in calculating hash and to be uploaded.
	raw, err := binary.Marshal(lineage.Ops)
	if err != nil {
		s.lineageMu.Unlock()
		return lineage, lineage.Term + 1, err
	}

	// Construct the term.
	term := &types.LineageTerm{
		Term:    lineage.Term + 1,
		Updates: lineage.Updates, // Stores "Updates" of the last term, don't forget to fix this on recovery.
		RawOps:  raw,
	}
	hash := new(bytes.Buffer)
	hashBinder := binary.NewEncoder(hash)
	hashBinder.WriteUint64(term.Term)
	hashBinder.WriteUint64(term.Updates)
	hashBinder.Write(term.RawOps)
	hashBinder.Write([]byte(lineage.Hash))
	term.Hash = fmt.Sprintf("%x", sha256.Sum256(hash.Bytes()))

	// Zip and marshal the term.
	buf := new(bytes.Buffer)
	zipWriter := gzip.NewWriter(buf)
	if err := binary.MarshalTo(term, zipWriter); err != nil {
		s.lineageMu.Unlock()
		return lineage, term.Term, err
	}
	if err := zipWriter.Close(); err != nil {
		s.lineageMu.Unlock()
		return lineage, term.Term, err
	}

	// Update local lineage. Size must be updated before used for uploading.
	lineage.Size = uint64(buf.Len())
	lineage.Ops = lineage.Ops[:0]
	lineage.Term = term.Term
	lineage.Updates += lineage.Size // Fix local "Updates"
	lineage.Hash = term.Hash
	lineage.DiffRank = s.diffrank.Rank() // Store for snapshot use.
	// Unlock lineage, the storage can server next term while uploading
	s.lineageMu.Unlock()

	// Upload
	params := &s3manager.UploadInput{
		Bucket: aws.String(s.s3bucketDefault),
		Key:    aws.String(fmt.Sprintf(LINEAGE_KEY, s.s3prefix, s.functionName(s.id), term.Term)),
		Body:   buf,
	}
	_, err = uploader.Upload(params)
	// if err != nil {
	// 	// TODO: Pending and retry at a later time.
	// }
	return lineage, term.Term, err
}

func (s *LineageStorage) doSnapshot(lineage *types.LineageTerm, uploader *s3manager.Uploader) (*types.LineageTerm, error) {
	start := time.Now()
	// Construct object list.
	allOps := make([]types.LineageOp, 0, s.repo.Len())
	lenBuffer := 0
	for keyChunk := range s.repo.Iter() {
		chunk := keyChunk.Value.(*types.Chunk)
		buffered := chunk.IsBuffered(false) && !chunk.IsDeleted()
		if buffered {
			lenBuffer++
		}
		if !chunk.Backup && (!chunk.IsBuffered(true) || buffered) && chunk.Term <= lineage.Term {
			allOps = append(allOps, types.LineageOp{
				Op:       chunk.Op(),
				Key:      chunk.Key,
				Id:       chunk.Id,
				Size:     chunk.Size,
				Accessed: chunk.Accessed,
				BIdx:     chunk.BuffIdx,
			})
		}
	}

	// Construct the snapshot.
	ss := &types.LineageTerm{
		Term:     lineage.Term,
		Updates:  lineage.Updates,
		Ops:      allOps,
		Hash:     lineage.Hash,
		DiffRank: lineage.DiffRank,
		Buffered: lenBuffer,
	}

	// Zip and marshal the snapshot.
	buf := new(bytes.Buffer)
	zipWriter := gzip.NewWriter(buf)
	if err := binary.MarshalTo(ss, zipWriter); err != nil {
		return nil, err
	}
	if err := zipWriter.Close(); err != nil {
		return nil, err
	}
	// Release "Ops" and update size. Size must be updated before used for uploading.
	ss.Ops = nil
	ss.Size = uint64(buf.Len())

	s.log.Trace("It took %v to snapshot %d chunks.", time.Since(start), len(allOps))

	// Persists.
	params := &s3manager.UploadInput{
		Bucket: aws.String(s.s3bucketDefault),
		Key:    aws.String(fmt.Sprintf(SNAPSHOT_KEY, s.s3prefix, s.functionName(s.id), ss.Term)),
		Body:   buf,
	}
	if _, err := uploader.Upload(params); err != nil {
		// Simply abandon.
		return nil, err
	}

	return ss, nil
}

func (s *LineageStorage) getRecoverFlag(meta *types.LineageMeta) uint32 {
	switch meta.Type {
	case types.LineageMetaTypeMain:
		return RECOVERING_MAIN
	case types.LineageMetaTypeBackup:
		return RECOVERING_BACKUP
	default:
		return RECOVERING_NONE
	}
}

func (s *LineageStorage) doRecover(ctx context.Context, lineage *types.LineageTerm, meta *types.LineageMeta, chanErr chan error) {
	// Initialize s3 api
	downloader := s.getS3Downloader()
	recoverFlag := s.getRecoverFlag(meta)

	// Recover lineage
	start := time.Now()
	lineageBytes, terms, numOps, err := s.doRecoverLineage(lineage, meta.Meta, downloader)
	stop1 := time.Since(start)
	if err != nil {
		chanErr <- err
	}
	s.log.Debug("Lineage recovered %d.", meta.Meta.Id)

	if len(terms) == 0 {
		// No term recovered
		if meta.Type == types.LineageMetaTypeMain {
			s.recovered = lineage // Flag for incomplete recovery
		}
		s.log.Error("No term is recovered for node %d.", meta.Meta.Id)

		close(chanErr)
		s.resetGet(recoverFlag)
		return
	}

	// Replay lineage
	var tbds []*types.Chunk
	if meta.Type == types.LineageMetaTypeDelegate {
		tbds = s.doDelegateLineage(meta, terms, numOps)
	} else {
		tbds = s.doReplayLineage(meta, terms, numOps)
	}
	// Now get is safe
	s.resetGet(recoverFlag)
	s.log.Debug("Lineage replayed %d.", meta.Meta.Id)

	// s.log.Debug("tbds %d: %v", meta.Meta.Id, tbds)
	// for i, t := range tbds {
	// 	s.log.Debug("%d: %v", i, *t)
	// }

	start2 := time.Now()
	objectBytes := 0
	if len(tbds) > 0 {
		objectBytes, err = s.doRecoverObjects(ctx, tbds, downloader)
	}
	stop2 := time.Since(start2)
	if err != nil {
		chanErr <- err
	}

	end := time.Since(start)
	s.log.Info("End recovery of node %d.", meta.Meta.Id)
	s.log.Trace("action,lineage,objects,elapsed,bytes")
	s.log.Trace("recover,%d,%d,%d,%d", stop1, stop2, end, objectBytes)
	collector.AddRecovery(
		start, types.OP_RECOVERY, int(meta.Type), meta.Meta.Id, meta.BackupId,
		stop1, stop2, end, lineageBytes, objectBytes, len(tbds))
	close(chanErr)
}

func (s *LineageStorage) doRecoverLineage(lineage *types.LineageTerm, meta *protocol.Meta, downloader *mys3.Downloader) (int, []*types.LineageTerm, int, error) {
	// If hash not match, invalidate lineage.
	if lineage == nil {
		lineage = &types.LineageTerm{Term: 1}
	} else if meta.Term == lineage.Term && meta.Hash != lineage.Hash {
		// Since snapshots are small, always load snapshot only if dismatch.
		lineage.Term = 1
		lineage.Updates = 0
		lineage.Hash = ""
	}

	// meta.Updates - meta.SnapshotUpdates + meta.SnapshotSize < meta.Updates - lineage.Updates
	baseTerm := lineage.Term
	snapshot := false
	if meta.SnapshotUpdates-meta.SnapshotSize > lineage.Updates {
		// Recover lineage from snapshot
		baseTerm = meta.SnapshotTerm - 1
		snapshot = true
		s.log.Info("Recovering node %d from snapshot of term %d to term %d", meta.Id, meta.SnapshotTerm, meta.Term)
	} else {
		s.log.Info("Recovering node %d from term %d to term %d", meta.Id, lineage.Term, meta.Term)
	}

	// Setup receivers
	terms := meta.Term - baseTerm
	inputs := make(chan *mys3.BatchDownloadObject, IntMin(int(terms)*LineageRecoveryContenders, downloader.Concurrency))
	receivedFlags := make([]bool, terms)
	receivedTerms := make([]*types.LineageTerm, 0, terms)
	chanNotify := make(chan interface{}, len(inputs))
	chanError := make(chan error)
	from := 0
	received := 0
	receivedBytes := 0
	receivedOps := 0

	// Setup input for snapshot downloading.
	if snapshot {
		i := 0
		input := &mys3.BatchDownloadObject{}
		input.Object = &s3.GetObjectInput{
			Bucket: aws.String(s.s3bucketDefault),
			Key:    aws.String(fmt.Sprintf(SNAPSHOT_KEY, s.s3prefix, s.functionName(meta.Id), baseTerm+1)), // meta.SnapshotTerm
		}
		input.Writer = aws.NewWriteAtBuffer(make([]byte, 0, meta.SnapshotSize))
		input.After = s.getReadyNotifier(input, chanNotify)
		input.Meta = &i
		for j := 0; j < LineageRecoveryContenders; j++ {
			inputs <- input
		}
		// skip 0
		from++
	}
	// Setup inputs for terms downloading.
	go func(from int) {
		for from < int(terms) {
			i := from
			input := &mys3.BatchDownloadObject{}
			input.Object = &s3.GetObjectInput{
				Bucket: aws.String(s.s3bucketDefault),
				Key:    aws.String(fmt.Sprintf(LINEAGE_KEY, s.s3prefix, s.functionName(meta.Id), baseTerm+uint64(from)+1)),
			}
			input.Writer = new(aws.WriteAtBuffer)
			input.After = s.getReadyNotifier(input, chanNotify)
			input.Meta = &i
			for j := 0; j < LineageRecoveryContenders; j++ {
				inputs <- input
			}
			from++
		}
		close(inputs)
	}(from)

	// Start downloading.
	go func() {
		// iter := &s3manager.DownloadObjectsIterator{ Objects: inputs }
		ctx := aws.BackgroundContext()
		ctx = context.WithValue(ctx, &ContextKeyLog, s.log)
		if err := downloader.DownloadWithIterator(ctx, inputs); err != nil {
			chanError <- err
		}
	}()

	// Wait for snapshot and lineage terms.

	for received < int(terms) {
		select {
		case err := <-chanError:
			return receivedBytes, receivedTerms, receivedOps, err
		case input := <-chanNotify:
			i := *(input.(*mys3.BatchDownloadObject).Meta.(*int))
			// Because of contenders, only the first one is recorded.
			if input.(*mys3.BatchDownloadObject).Error != nil || receivedFlags[i] {
				break
			}

			receivedFlags[i] = true
			for received < int(terms) && receivedFlags[received] {
				raw := input.(*mys3.BatchDownloadObject).Bytes()
				if len(raw) == 0 {
					// Something wrong, reset receivedFlags and wait for error
					receivedFlags[received] = false
					break
				}
				receivedBytes += len(raw)

				// Unzip
				zipReader, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					return receivedBytes, receivedTerms, receivedOps, err
				}

				rawTerm := new(bytes.Buffer)
				if _, err := io.Copy(rawTerm, zipReader); err != nil {
					return receivedBytes, receivedTerms, receivedOps, err
				}
				if err := zipReader.Close(); err != nil {
					return receivedBytes, receivedTerms, receivedOps, err
				}

				// Unmarshal
				var term types.LineageTerm
				if err := binary.Unmarshal(rawTerm.Bytes(), &term); err != nil {
					return receivedBytes, receivedTerms, receivedOps, err
				}

				// Fix values
				term.Size = uint64(len(raw))
				if term.RawOps != nil {
					// Lineage terms have field "rawOps"
					term.Ops = make([]types.LineageOp, 1)
					if err := binary.Unmarshal(term.RawOps, &term.Ops); err != nil {
						return receivedBytes, receivedTerms, receivedOps, err
					}
					// Fix "Updates"
					term.RawOps = nil // Release
					term.Updates += term.Size
				}

				// Collect terms
				receivedTerms = append(receivedTerms, &term)

				received++
				receivedOps += len(term.Ops)
			}
		}
	}
	return receivedBytes, receivedTerms, receivedOps, nil
}

func (s *LineageStorage) doReplayLineage(meta *types.LineageMeta, terms []*types.LineageTerm, numOps int) []*types.Chunk {
	var fromSnapshot uint64
	if meta.Type == types.LineageMetaTypeMain && terms[0].DiffRank > 0 {
		// Recover start with a snapshot
		fromSnapshot = terms[0].Term
		numOps -= s.repo.Len() + s.backup.Len() // Because snapshot includes what have been in repo, exclued them as estimation.
		if numOps < 10 {
			numOps = 10 // Arbitary 10 minimum.
		}
	}
	tbds := make([]*types.Chunk, 0, numOps) // To be downloaded. Initial capacity is estimated by the # of ops.

	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	// Init buffer according terms[0], usually it should be a snapshot, or the buffer has been initialized.
	s.bufferInit(terms[0].Buffered)

	// Deal with the key that is currently serving.
	servingKey := meta.ServingKey()
	if servingKey != "" {
		// If serving_key exists, we are done. Deteled serving_key is unlikely and will be verified later.
		chunk, existed := s.Storage.get(servingKey)
		if !existed {
			chunk = &types.Chunk{
				Key:       servingKey,
				Body:      nil,
				Size:      0, // To be filled later.
				Status:    types.CHUNK_RECOVERING,
				Available: 0,
				Backup:    meta.Type == types.LineageMetaTypeBackup,
			}
			// Occupy the repository, details will be filled later.
			s.Storage.set(servingKey, chunk) // No size update (don't know how) by calling "repo.Set". Just a placeholder.
		}

		// Add to head so it will be load first, no duplication is possible because it has been inserted to the repository.
		// Regardless new or existed, we add chunk to download list for possible RESET (unlikely).
		tbds = append(tbds, chunk)
	}

	// Replay operations
	if meta.Type == types.LineageMetaTypeMain && fromSnapshot > 0 {
		// Diffrank is supposed to be a moving value, we should replay it as long as possible.
		s.diffrank.Reset(terms[0].DiffRank) // Reset diffrank if recover from the snapshot
	}
	// allKeys := make([]string, 0, numOps)
	for _, term := range terms {
		for i := 0; i < len(term.Ops); i++ {
			op := &term.Ops[i]
			// if meta.Backup {
			// 	allKeys = append(allKeys, op.Key)
			// }

			// Replay diffrank, skip ops in snapshot.
			// Condition: !fromSnapshot || term.Term > meta.SnapshotTerm
			if meta.Type == types.LineageMetaTypeMain && term.Term > fromSnapshot { // Simplified.
				s.diffrank.AddOp(op)
			}

			chunk, existed := s.Storage.get(op.Key)
			if existed {
				// Exclude out of date operations, possibilities are:
				// 1. New incoming write operations during recovery: chunk.Term > s.lineage.Term >= term.Term
				// 2. Overlap operations (included in the snapshot): chunk.Term >= term.Term
				if term.Term <= chunk.Term {
					// Skip
					continue
				}

				if op.Op == types.OP_DEL {
					if !chunk.IsDeleted() {
						chunk.Term = term.Term
						chunk.Accessed = op.Accessed
						if chunk.Backup {
							s.Storage.del(chunk, "sync lineage") // logic deletion.
							s.meta.bakSize -= chunk.Size         // In case servingKey, placeholder chunk with servingKey has Size 0
						} else {
							s.Storage.delWithOption(chunk, "sync lineage", &types.OpWrapper{Accessed: true}) // Decreasing size in storage only by calling "Storage.delWithOption" (placeholder servingKey has Size 0).
							if chunk.IsBuffered(false) {
								s.bufferRemove(chunk) // "bufferRemove" will remove chunk from buffer and update buffer size.
							}
						}
					}
					// Deletion after set will be dealed on recovering objects.
					continue
				} else if op.Key == servingKey && chunk.Id == "" {
					// Updaet data of servingKey, overwrite is OK.
					chunk.Id = op.Id
					chunk.Size = op.Size
					chunk.Term = term.Term
					chunk.Accessed = op.Accessed
					chunk.Bucket = op.Bucket
				} else {
					// overlap
					chunk.Accessed = op.Accessed
					continue
				}
			}

			// New or reset chunk
			if chunk == nil {
				if s.isRecoverable(op, meta, false) {
					// Main repository or backup repository if backup ID matches.
					chunk = &types.Chunk{
						Key:       op.Key,
						Id:        op.Id,
						Body:      nil,
						Size:      op.Size,
						Term:      term.Term,
						Status:    types.CHUNK_RECOVERING,
						Available: 0,
						Accessed:  op.Accessed,
						Bucket:    op.Bucket,
						Backup:    meta.Type == types.LineageMetaTypeBackup,
					}
					if op.Op == types.OP_DEL {
						chunk.Status = types.CHUNK_DELETED
						chunk.Note = "restore lineage"
					}
				} else {
					// Skip non-recoverable objects.
					continue
				}
			}

			if chunk.Backup {
				s.Storage.set(chunk.Key, chunk)
				s.backup.Set(chunk.Key, chunk)
				if !chunk.IsDeleted() {
					s.meta.bakSize += chunk.Size
				}
			} else {
				// For types.LineageMetaTypeMain only, types.LineageMetaTypeDelegate will not call "doReplayLineage".
				chunk.BuffIdx = op.BIdx
				if !chunk.IsDeleted() {
					s.Storage.setWithOption(op.Key, chunk, nil) // Update size by calling "Storage.setWithOption".
					if chunk.IsBuffered(false) {
						// Chunk was indicated to be buffered, rebuff it.
						chunk.BuffIdx = types.CHUNK_TOBEBUFFERED
						// No size update by calling "bufferAdd". Update the size after object downloaded.
						s.bufferAdd(chunk) // No eviction will be considered since we are restoring existed state.
					}
				} else if !chunk.IsBuffered(false) {
					s.Storage.set(chunk.Key, chunk) // Simply set to indicate the object is deleted
				}
				// Deleted buffered chunk = evicted chunk, and will not be restore.
			}

			// Append to tbd list.
			if op.Key != servingKey {
				tbds = append(tbds, chunk)
			}
		}
		// Remove servingKey from the download list if it is not new and no reset is required.
		if servingKey != "" && tbds[0].IsAvailable() {
			tbds = tbds[1:]
		}

		// Passing by, update local snapshot
		if meta.Type == types.LineageMetaTypeMain && term.Term == meta.SnapshotTerm {
			if s.snapshot == nil {
				s.snapshot = &types.LineageTerm{}
			}
			s.snapshot.Term = term.Term
			s.snapshot.Updates = term.Updates // Assert: fixed in doRecoverLineage
			s.snapshot.Hash = term.Hash
			if fromSnapshot > 0 {
				s.snapshot.Size = term.Size // Use real value
			} else {
				s.snapshot.Size = meta.SnapshotSize // We didn't start from a snapshot, copy passed from the proxy.
			}
			s.snapshot.DiffRank = s.diffrank.Rank()
		}
	}

	// Now tbds are settled, initiate notifiers
	for _, tbd := range tbds {
		if !tbd.IsDeleted() {
			tbd.StartRecover()
		}
	}

	// Update local lineage.
	lastTerm := terms[len(terms)-1]
	if meta.Type == types.LineageMetaTypeMain {
		if lastTerm.Term < meta.Term {
			// Incomplete recovery, store result to s.recovered.
			s.recovered = lastTerm
			s.log.Error("Incomplete recovery up to term %d: %v", lastTerm.Term, terms)
		} else {
			// Complete. Term has been updated on calling "Recover".
			s.recovered = nil
			s.lineage.Updates = lastTerm.Updates
			s.lineage.Hash = lastTerm.Hash
			s.lineage.Size = lastTerm.Size
		}
		// Replay ops in current term
		for i := 0; i < len(s.lineage.Ops); i++ {
			s.diffrank.AddOp(&s.lineage.Ops[i])
		}
	} else {
		s.backupLineage = meta
		// var allchunks strings.Builder
		// for _, tbd := range tbds {
		// 	allchunks.WriteString(" ")
		// 	allchunks.WriteString(tbd.Key)
		// }
		// s.log.Debug("Keys to checked(%d of %d): %s", meta.BackupId, meta.BackupTotal, strings.Join(allKeys, " "))
		// s.log.Debug("Keys to recover(%d of %d): %s", meta.BackupId, meta.BackupTotal, allchunks.String())
	}

	return tbds
}

func (s *LineageStorage) doDelegateLineage(meta *types.LineageMeta, terms []*types.LineageTerm, numOps int) []*types.Chunk {
	tbds := make([]*types.Chunk, 0, numOps) // To be downloaded. Initial capacity is estimated by the # of ops.

	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	// Merge operations in terms
	// All delegated ops will using term: s.lineage.Term + 1
	for _, term := range terms {
		for i := 0; i < len(term.Ops); i++ {
			op := &term.Ops[i]
			chunk, existed := s.Storage.get(op.Key)
			if existed && op.Accessed.Before(chunk.Accessed) {
				// Skip duplicated
				continue
			} else if existed {
				chunk.Accessed = op.Accessed
				if op.Op == types.OP_DEL && !chunk.IsDeleted() {
					chunk.Status = types.CHUNK_DELETED
					chunk.Note = "delegation preflight"
					// Status changed, track.
					if chunk.Term < s.lineage.Term+1 {
						chunk.Term = s.lineage.Term + 1
						tbds = append(tbds, chunk)
					}
				} else {
					// Skip
					// In case op.Op == types.OP_SET && chunk.Status == types.CHUNK_DELETED, unlikely and nothing we should do.
					continue
				}
			}
			// Added delegate chunks
			if chunk == nil && op.Op != types.OP_DEL && s.isRecoverable(op, meta, false) {
				// Main repository or backup repository if backup ID matches.
				chunk := &types.Chunk{
					Key:       op.Key,
					Id:        op.Id,
					Body:      nil,
					Size:      op.Size,
					Term:      s.lineage.Term + 1,
					Status:    types.CHUNK_RECOVERING,
					Available: 0,
					Accessed:  op.Accessed,
					Bucket:    op.Bucket,
					Backup:    false,
					BuffIdx:   types.CHUNK_TOBEBUFFERED, // Temporary, original op.BIdx is discarded.
				}

				tbds = append(tbds, chunk)
				// Placeholder for deduplication purpoose.
				s.repo.Set(op.Key, chunk)
			}
		}
	}

	s.bufferInit(len(tbds))

	// Now tbds are settled, add delegates to buffer and evict least accessed if necessary.
	filtered := tbds[:]
	delegated := 0
	ru := &ChunkQueue{queue: tbds, lru: false}
	heap.Init(ru)
	breaked := false
	// Iterate from ru to lru, no object added here will be evicted again.
	for ru.Len() > 0 {
		tbd := heap.Pop(ru).(*types.Chunk)
		if !tbd.IsBuffered(true) && tbd.IsDeleted() {
			// Changes to main repo, append oplog to lineage.
			s.PersistentStorage.delWithOption(tbd, "delegation", nil)
			continue
		}

		if tbd.IsDeleted() {
			// Skip, remove if already in buffer
			if tbd.IsBuffered(false) {
				s.bufferRemove(tbd)
				s.PersistentStorage.delWithOption(tbd, "delegation", nil)
			} else {
				// Remove the placeholder.
				s.repo.Del(tbd.Key)
			}
			continue
		}

		// Remove the placeholder. Objects buffered but undeleted will not show here in the first place.
		s.repo.Del(tbd.Key)
		if breaked {
			continue
		}

		// Size check and add to buffer
		if opt, ok := s.helper.validate(tbd, nil); ok {
			opt.Persisted = true
			s.PersistentStorage.setWithOption(tbd.Key, tbd, opt)
			tbd.StartRecover()
			delegated++
			filtered[len(filtered)-delegated] = tbd
		} else {
			// No space? we are done
			breaked = true
		}
	}
	// Updated tbds
	tbds = filtered[len(filtered)-delegated:]

	return tbds
}

func (s *LineageStorage) doRecoverObjects(ctx context.Context, tbds []*types.Chunk, downloader *mys3.Downloader) (int, error) {
	// Setup receivers
	inputs := make(chan *mys3.BatchDownloadObject, downloader.Concurrency)
	chanNotify := make(chan interface{}, downloader.Concurrency)
	chanError := make(chan error, 1)
	var succeed uint32
	var requested uint32
	var received uint32
	var receivedBytes int

	// Setup inputs for terms downloading.
	more := true
	ctxDone := ctx.Done()
	cancelled := false
	go func() {
		defer close(inputs)

		for i := 0; i < len(tbds); i++ {
			// Check for terminate signal
			if ctxDone != nil {
				select {
				case <-ctxDone:
					more = false
					cancelled = true // Flag terminated, wait for download consumes all scheduled. Then, the interrupted err will be returned.
					return
				default:
				}
			}

			if tbds[i].IsDeleted() {
				// Skip deleted objects
				atomic.AddUint32(&succeed, 1)
				continue
			}

			bucket := s.bucket(&tbds[i].Bucket)
			key := aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, tbds[i].Key))
			tbds[i].Body = make([]byte, tbds[i].Size) // Pre-allocate fixed sized buffer.

			if num, err := downloader.Schedule(inputs, func(input *mys3.BatchDownloadObject) {
				// Update notifier if request is splitted.
				if input.Object.Key != nil {
					input.After = s.getReadyNotifier(input, chanNotify)
					return
				}
				input.Object.Bucket = bucket
				input.Object.Key = key
				input.Object.Range = nil
				input.Size = tbds[i].Size
				input.Writer = aws.NewWriteAtBuffer(tbds[i].Body) // Don't use new(aws.WriteAtBuffer), will OOM.
				input.After = s.getReadyNotifier(input, chanNotify)
				input.Meta = tbds[i]
			}); err != nil {
				s.log.Warn("%v", err)
			} else {
				atomic.AddUint32(&requested, uint32(num))
				// s.log.Debug("Scheduled %s(%d)", *key, num)
			}
		}
		more = false
		// s.log.Info("Inputs closed")
	}()

	// Start downloading.
	go func() {
		// iter := &s3manager.DownloadObjectsIterator{ Objects: inputs }
		ctx := aws.BackgroundContext()
		ctx = context.WithValue(ctx, &ContextKeyLog, s.log)
		err := downloader.DownloadWithIterator(ctx, inputs)
		if cancelled {
			chanError <- ErrRecoveryInterrupted
		} else if err != nil {
			s.log.Error("error on download objects: %v", err)
			chanError <- err
		}
	}()

	// Wait for objects, if all is not successful, wait for err.
	// Safe if objects are in buffer and has been evicted on downloaded.
	for more || received < atomic.LoadUint32(&requested) {
		select {
		case err := <-chanError:
			return receivedBytes, err
		case input := <-chanNotify:
			received++
			dobj := input.(*mys3.BatchDownloadObject)
			tbd := dobj.Meta.(*types.Chunk)
			receivedBytes += int(dobj.Downloaded)
			// objectRange := ""
			// if dobj.Object.Range != nil {
			// 	objectRange = *dobj.Object.Range
			// }
			// s.log.Debug("Update object availability %s(%s): %d of %d", tbd.Key, objectRange, available, tbd.Size)
			tbd.AddRecovered(uint64(dobj.Downloaded))
			downloader.Done(dobj)
			// s.log.Info("Object: %s, size: %d, available: %d", *dobj.Object.Key, tbd.Size, available)
			// s.log.Info("Requested: %d, Received: %d, Succeed: %d", requested, received, succeed)
		}
	}
	// Set any recovering chunk to CHUNK_INCOMPLETE
	for _, tbd := range tbds {
		tbd.EndRecover(types.CHUNK_INCOMPLETE)
	}
	return receivedBytes, nil
}

func (s *LineageStorage) isRecoverable(op *types.LineageOp, meta *types.LineageMeta, verify bool) bool {
	if meta.Type == types.LineageMetaTypeMain {
		return true
	}
	s.backupLocator.Reset(int(meta.BackupTotal))
	target, _, _ := s.backupLocator.Locate(op.Key)
	if target == meta.BackupId && (meta.MaxChunkSize == 0 || op.Size <= meta.MaxChunkSize) {
		return true
	} else if verify {
		s.log.Warn("Detected backup reroute error, expected %d, actual %d, key %s", meta.BackupId, target, op.Key)
	}
	return false
}

func (s *LineageStorage) getReadyNotifier(i interface{}, chanNotify chan interface{}) func() error {
	return func() error {
		chanNotify <- i
		return nil
	}
}

func (s *LineageStorage) delaySet() {
	s.setSafe.Add(1)
}

func (s *LineageStorage) resetSet() {
	s.setSafe.Done()
}

func (s *LineageStorage) delayGet(flag uint32) bool {
	old := atomic.LoadUint32(&s.safenote)
	if old&flag > 0 {
		// Already delayed
		return false
	}
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old|flag) {
		old = atomic.LoadUint32(&s.safenote)
		if old&flag > 0 {
			// Already delayed
			return false
		}
	}
	s.getSafe.Add(1)
	return true
}

func (s *LineageStorage) resetGet(flag uint32) {
	s.getSafe.Done()
	old := atomic.LoadUint32(&s.safenote)
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old&^flag) {
		old = atomic.LoadUint32(&s.safenote)
	}
}

func (s *LineageStorage) isSafeToGet(chunk *types.Chunk) bool {
	note := atomic.LoadUint32(&s.safenote)
	if chunk.Backup {
		return note&RECOVERING_BACKUP == 0
	} else {
		return (note&RECOVERING_MAIN) == 0 || chunk.Term > s.lineage.Term // Objects of Term beyond lineage.Term are newly set.
	}
}

func (s *LineageStorage) bufferInit(size int) {
	if s.bufferQueue == nil {
		s.bufferQueue = &ChunkQueue{queue: make([]*types.Chunk, 0, size), lru: true}
	}
}

func (s *LineageStorage) bufferAdd(chunk *types.Chunk) {
	heap.Push(s.bufferQueue, chunk)
	s.bufferMeta.IncreaseSize(chunk.Size)
}

func (s *LineageStorage) bufferRemove(chunk *types.Chunk) {
	if validation := s.bufferQueue.Chunk(chunk.BuffIdx - 1); validation == nil || validation.Key != chunk.Key {
		return
	}

	heap.Remove(s.bufferQueue, chunk.BuffIdx-1)
	s.bufferMeta.DecreaseSize(chunk.Size)
}

func (s *LineageStorage) bufferFix(chunk *types.Chunk) {
	if validation := s.bufferQueue.Chunk(chunk.BuffIdx - 1); validation == nil || validation.Key != chunk.Key {
		return
	}

	heap.Fix(s.bufferQueue, chunk.BuffIdx-1)
}

func (S *LineageStorage) functionName(id uint64) string {
	return fmt.Sprintf("%s%d", FunctionPrefix, id)
}

func IntMin(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func IntMax(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func Uint64Min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func Uint64Max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}
