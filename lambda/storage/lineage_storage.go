package storage

import (
	"bytes"
	"compress/gzip"
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
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/zhangjyr/hashmap"

	mys3 "github.com/mason-leap-lab/infinicache/common/aws/s3"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	LINEAGE_KEY  = "%s%s/lineage-%d"
	SNAPSHOT_KEY = "%s%s/snapshot-%d.gz"

	RECOVERING_MAIN   uint32 = 0x01
	RECOVERING_BACKUP uint32 = 0x02

	LineageStorageOverhead    = 150000000
	BackupStoreageReservation = 0.1 // 1/N * 2, N = 20, backups per lambda.
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

	// backup
	backup                  *hashmap.HashMap   // Just a index, all will be available to repo
	backupMeta              *types.LineageMeta // Only one backup is effective at a time.
	backupLocator           protocol.BackupLocator
	backupRecoveryCanceller context.CancelFunc

	// padding objects
	size        int64
	backupSize  int64
	paddingPool []*types.Chunk
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
	return storage
}

// Storage Implementation
func (s *LineageStorage) getWithOption(key string, opt *types.OpWrapper) (string, []byte, *types.OpRet) {
	// Before getting safely, try what is available so far.
	chunk, ok := s.helper.get(key)
	// !ok: Most likely, this is because an imcomplete lineage.
	// not safe: Corresponding lineage is recovering, and the chunk is not just set
	if !ok || !s.isSafeToGet(chunk) {
		s.getSafe.Wait()
	}

	// Now we're safe to proceed.
	return s.PersistentStorage.getWithOption(key, opt)
}

func (s *LineageStorage) set(key string, chunk *types.Chunk) {
	s.repo.Set(key, chunk)
	if chunk.Backup {
		s.backup.Set(key, chunk)
	}
}

func (s *LineageStorage) setWithOption(key string, chunkId string, val []byte, opt *types.OpWrapper) *types.OpRet {
	s.setSafe.Wait()
	// s.log.Debug("ready to set key %v", key)
	// Lock lineage, ensure operation get processed in the term.
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()
	// s.log.Debug("in mutex of setting key %v", key)

	return s.PersistentStorage.setWithOption(key, chunkId, val, opt)
}

func (s *LineageStorage) newChunk(key string, chunkId string, size uint64, val []byte) *types.Chunk {
	chunk := types.NewChunk(key, chunkId, val)
	chunk.Size = size
	chunk.Term = 1
	if s.lineage != nil {
		chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
		chunk.Bucket = s.getBucket(key)
	}
	return chunk
}

func (s *LineageStorage) Del(key string, chunkId string) *types.OpRet {
	s.getSafe.Wait()

	chunk, ok := s.helper.get(key)
	if !ok {
		return types.OpError(types.ErrNotFound)
	}

	s.setSafe.Wait()

	// Lock lineage
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	chunk.Term = util.Ifelse(s.lineage != nil, s.lineage.Term+1, 1).(uint64) // Add one to reflect real term.
	chunk.Access()
	chunk.Delete()

	if s.chanOps != nil {
		op := &types.OpWrapper{
			LineageOp: types.LineageOp{
				Op:       types.OP_DEL,
				Key:      key,
				Id:       chunkId,
				Size:     chunk.Size,
				Accessed: chunk.Accessed,
				// Ret: make(chan error, 1),
				Bucket: chunk.Bucket,
			},
			OpRet: types.OpDelayedSuccess(),
		}
		s.chanOps <- op
		return op.OpRet
	} else {
		return types.OpSuccess()
	}
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
	if meta.Backup {
		if s.backupMeta == nil || s.backupMeta.Id != meta.Id || s.backupMeta.BackupId != meta.BackupId || s.backupMeta.BackupTotal != meta.BackupTotal {
			meta.Consistent = false
			return meta.Consistent, nil
		}
		lineage = types.LineageTermFromMeta(s.backupMeta)
	}
	if lineage.Term > meta.Term {
		meta.Consistent = false
		return meta.Consistent, fmt.Errorf("detected staled term of lambda %d, expected at least %d, have %d", meta.Id, lineage.Term, meta.Term)
	}
	// Don't check hash if term is the start term(1).
	meta.Consistent = lineage.Term == meta.Term && (meta.Term == 1 || lineage.Hash == meta.Hash)
	return meta.Consistent, nil
}

func (s *LineageStorage) StartTracker() {
	if s.chanOps == nil {
		s.lineage.Ops = s.lineage.Ops[:0] // Reset metalogs
		s.resetSet()
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

func (s *LineageStorage) onStopTracker(signal interface{}) bool {
	return s.doCommit(signal.(*types.CommitOption))
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
	option = (<-s.trackerStopped).(*types.CommitOption)

	// Flag checked
	return option, nil
}

func (s *LineageStorage) StopTracker(signal interface{}) {
	if s.signalTracker != nil {
		option := signal.(*types.CommitOption)
		// Reset bytes uploaded
		option.BytesUploaded = 0

		s.PersistentStorage.StopTracker(option)

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
	if s.backupMeta != nil {
		return types.LineageStatus{meta, s.backupMeta.Meta}
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

	s.log.Debug("Start recovery of node %d(bak:%v).", meta.Meta.Id, meta.Backup)
	chanErr := make(chan error, 1)
	// Flag get as unsafe
	if !s.delayGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32)) {
		chanErr <- ErrRecovering
		return false, chanErr
	}
	// Double check consistency
	consistent, err := s.IsConsistent(meta)
	if err != nil {
		s.resetGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
		chanErr <- err
		return false, chanErr
	} else if consistent {
		s.resetGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
		chanErr <- ErrRecovered
		return false, chanErr
	}

	// Copy lineage data for recovery, update term to the recent record, and we are ready for write operatoins.
	var old *types.LineageTerm
	if !meta.Backup {
		old = &types.LineageTerm{
			Term:    s.lineage.Term,
			Updates: s.lineage.Updates,
		}
		s.lineage.Term = meta.Term
		s.lineage.Updates = meta.Updates
		s.lineage.Hash = meta.Hash
		s.log.Debug("During recovery, write operations enabled at term %d", s.lineage.Term+1)
	} else if s.backupMeta != nil &&
		s.backupMeta.Meta.Id == meta.Meta.Id &&
		s.backupMeta.BackupId == meta.BackupId &&
		s.backupMeta.BackupTotal == meta.BackupTotal {
		// Compare metas of backups for the same lambda
		old = types.LineageTermFromMeta(s.backupMeta)
	} else {
		// New backup lambda
		old = types.LineageTermFromMeta(nil)
		if s.backupMeta != nil && s.backupMeta.Meta.Id != meta.Meta.Id {
			s.log.Debug("Backup data of node %d cleared to serve %d.", s.backupMeta.Meta.Id, meta.Meta.Id)
			// Clean obsolete backups
			if s.backupRecoveryCanceller != nil {
				s.backupRecoveryCanceller()
			}
			s.ClearBackup()
		}
	}

	ctx := context.Background()
	if meta.Backup {
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
	return !meta.Backup && s.diffrank.IsSignificant(meta.DiffRank), chanErr
}

func (s *LineageStorage) ClearBackup() {
	for keyValue := range s.backup.Iter() {
		chunk := keyValue.Value.(*types.Chunk)
		chunk.Body = nil
		s.repo.Del(keyValue.Key)
		s.backup.Del(keyValue.Key)
	}
	s.backupMeta = nil
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
		s.log.Debug("Term %d commited, resignal to check possible new term during committing.", term)
		s.log.Trace("action,lineage,snapshot,elapsed,bytes")
		s.log.Trace("commit,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop1), end.Sub(start), termBytes+ssBytes)
		collector.AddCommit(
			start, types.OP_COMMIT, false, s.id, int(term),
			stop1.Sub(start), end.Sub(stop1), end.Sub(start), termBytes, ssBytes)
		opt.BytesUploaded += uint64(termBytes + ssBytes)
		s.signalTracker <- opt
		return false
	} else {
		// No operation since last signal.This will be quick and we are ready to exit lambda.
		// DO NOT close "committed", since there will be a double check on stoping the tracker.
		if opt.Checked {
			s.log.Debug("Double checked: no more term to commit, signal committed.")
		} else {
			s.log.Debug("Checked: no more term to commit, signal committed.")
		}
		s.trackerStopped <- opt
		return true
	}
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
	for keyChunk := range s.repo.Iter() {
		chunk := keyChunk.Value.(*types.Chunk)
		if !chunk.Backup && chunk.Term <= lineage.Term {
			allOps = append(allOps, types.LineageOp{
				Op:   chunk.Op(),
				Key:  chunk.Key,
				Id:   chunk.Id,
				Size: chunk.Size,
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

func (s *LineageStorage) doRecover(ctx context.Context, lineage *types.LineageTerm, meta *types.LineageMeta, chanErr chan error) {
	// Initialize s3 api
	downloader := s.getS3Downloader()

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
		if !meta.Backup {
			s.recovered = lineage // Flag for incomplete recovery
		}
		s.log.Error("No term is recovered for node %d.", meta.Meta.Id)

		close(chanErr)
		s.resetGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
		return
	}

	// Replay lineage
	tbds := s.doReplayLineage(meta, terms, numOps)
	// Now get is safe
	s.resetGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
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
	s.log.Debug("End recovery of node %d.", meta.Meta.Id)
	s.log.Trace("action,lineage,objects,elapsed,bytes")
	s.log.Trace("recover,%d,%d,%d,%d", stop1, stop2, end, objectBytes)
	collector.AddRecovery(
		start, types.OP_RECOVERY, meta.Backup, meta.Meta.Id, meta.BackupId,
		stop1, stop2, end, lineageBytes, objectBytes, len(tbds))
	close(chanErr)
}

func (s *LineageStorage) doRecoverLineage(lineage *types.LineageTerm, meta *protocol.Meta, downloader *mys3.Downloader) (int, []*types.LineageTerm, int, error) {
	// If hash not match, invalidate lineage.
	if meta.Term == lineage.Term && meta.Hash != lineage.Hash {
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
	if !meta.Backup && terms[0].DiffRank > 0 {
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

	// Deal with the key that is currently serving.
	servingKey := meta.ServingKey()
	if servingKey != "" {
		// If serving_key exists, we are done. Deteled serving_key is unlikely and will be verified later.
		chunk, existed := s.get(servingKey)
		if !existed {
			chunk = &types.Chunk{
				Key:    servingKey,
				Body:   nil,
				Backup: meta.Backup,
			}
			// Occupy the repository, details will be filled later.
			s.set(servingKey, chunk)
		}

		// Add to head so it will be load first, no duplication is possible because it has been inserted to the repository.
		// Regardless new or existed, we add chunk to download list for possible RESET (unlikely).
		tbds = append(tbds, chunk)
	}

	// Replay operations
	if !meta.Backup && fromSnapshot > 0 {
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
			if !meta.Backup && term.Term > fromSnapshot { // Simplified.
				s.diffrank.AddOp(op)
			}

			chunk, existed := s.get(op.Key)
			if existed {
				if chunk.Term > s.lineage.Term {
					// Skip new incoming write operations during recovery.
					continue
				}

				if op.Op == types.OP_DEL {
					// Deletion after set will be dealed on recovering objects.
					chunk.Status = types.CHUNK_DELETED
					chunk.Body = nil
				} else if op.Key == servingKey {
					// Updaet data of servingKey, overwrite is OK.
					chunk.Id = op.Id
					chunk.Size = op.Size
					chunk.Term = term.Term
					chunk.Accessed = op.Accessed
					chunk.Bucket = op.Bucket
					chunk.Available = 0
					// Unlikely, the servingKey can be RESET (see comments below)
					// For servingKey, the chunk is always added as the first one in the download list and it can't be moved, we simple set Body to nil to free space.
					// The bottom line, setting Body to nil doesn't hurt if servingKey is new.
					chunk.Body = nil
				} else {
					// Reset
					// Although unlikely, dealing reset by deleting it first and add it later.
					chunk.Status = types.CHUNK_DELETED
					chunk.Body = nil
					chunk = nil // Reset to nil, so it can be added back later.
				}
			}
			// New or reset chunk
			if chunk == nil && s.isRecoverable(op, meta, false) {
				// Main repository or backup repository if backup ID matches.
				chunk := &types.Chunk{
					Key:      op.Key,
					Id:       op.Id,
					Body:     nil,
					Size:     op.Size,
					Term:     term.Term,
					Accessed: op.Accessed,
					Bucket:   op.Bucket,
					Backup:   meta.Backup,
				}
				// New chunk can't be a deleted chunk, just in case something wrong.
				if op.Op != types.OP_DEL {
					tbds = append(tbds, chunk)
					s.set(op.Key, chunk)
				}
			}
		}
		// Remove servingKey from the download list if it is not new and no reset is required.
		if servingKey != "" && tbds[0].Available == tbds[0].Size {
			tbds = tbds[1:]
		}

		// Passing by, update local snapshot
		if !meta.Backup && term.Term == meta.SnapshotTerm {
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
			tbd.Notifier.Add(1)
		}
	}

	// Update local lineage.
	lastTerm := terms[len(terms)-1]
	if !meta.Backup {
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
		s.backupMeta = meta
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
	for more || received < atomic.LoadUint32(&requested) {
		select {
		case err := <-chanError:
			return receivedBytes, err
		case input := <-chanNotify:
			received++
			dobj := input.(*mys3.BatchDownloadObject)
			tbd := dobj.Meta.(*types.Chunk)
			receivedBytes += len(dobj.Bytes())
			available := atomic.AddUint64(&tbd.Available, uint64(dobj.Downloaded))
			// objectRange := ""
			// if dobj.Object.Range != nil {
			// 	objectRange = *dobj.Object.Range
			// }
			// s.log.Debug("Update object availability %s(%s): %d of %d", tbd.Key, objectRange, available, tbd.Size)
			if available == tbd.Size {
				succeed++
				tbd.Notifier.Done()
			}
			downloader.Done(dobj)
			// s.log.Info("Object: %s, size: %d, available: %d", *dobj.Object.Key, tbd.Size, available)
			// s.log.Info("Requested: %d, Received: %d, Succeed: %d", requested, received, succeed)
		}
	}
	return receivedBytes, nil
}

func (s *LineageStorage) isRecoverable(op *types.LineageOp, meta *types.LineageMeta, verify bool) bool {
	if !meta.Backup {
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
