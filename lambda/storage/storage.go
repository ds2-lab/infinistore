package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"sort"
	"strconv"

	// "strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	awsRequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cespare/xxhash"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	csync "github.com/mason-leap-lab/infinicache/common/sync"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo/resp"
	"github.com/zhangjyr/hashmap"

	mys3 "github.com/mason-leap-lab/infinicache/common/aws/s3"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/collector"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	CHUNK_KEY    = "%schunks/%s"
	LINEAGE_KEY  = "%s%s/lineage-%d"
	SNAPSHOT_KEY = "%s%s/snapshot-%d.gz"

	RECOVERING_MAIN   uint32 = 0x01
	RECOVERING_BACKUP uint32 = 0x02
)

var (
	Backups               = 10
	Concurrency           = types.DownloadConcurrency
	Buckets               = 1
	FunctionPrefix        string
	FunctionPrefixMatcher = regexp.MustCompile(`\d+$`)

	ERR_TRACKER_NOT_STARTED = errors.New("tracker not started")
	ContextKeyLog           = "log"

	// Updated: 6/30/2010
	// Try download as few as lineage file in batch: try one snapshot each term
	SnapshotInterval = uint64(1)
	// To minimize lineage recovery latency, try download the same file with multiple contenders.
	LineageRecoveryContenders = 3
)

// Storage with lineage
type Storage struct {
	// IMOC repository, operations are supposed to be serialized.
	// NOTE: If serialization of operations can not be guarenteed, reconsider the implementation
	//       of "repo" and "mu"
	id   uint64
	repo *hashmap.HashMap
	log  logger.ILogger

	// Lineage
	lineage       *types.LineageTerm // The lineage of current/recent term. The lineage is updated to recent term while recovering.
	recovered     *types.LineageTerm // Stores recovered lineage if it is not fully recovered, and will replace lineage on returning.
	snapshot      *types.LineageTerm // The latest snapshot of the lineage.
	diffrank      LineageDifferenceRank
	getSafe       csync.WaitGroup
	setSafe       csync.WaitGroup
	safenote      uint32                // Flag what's going on
	chanOps       chan *types.OpWrapper // NOTE: implement an unbounded channel if neccessary.
	signalTracker chan *types.CommitOption
	committed     chan *types.CommitOption
	lineageMu     sync.RWMutex // Mutex for lienage commit.

	// backup
	backup        *hashmap.HashMap   // Just a index, all will be available to repo
	backupMeta    *types.LineageMeta // Only one backup is effective at a time.
	backupLocator protocol.BackupLocator

	// Persistency backpack
	s3bucket        string
	s3bucketDefault string
	s3prefix        string
	s3Downloader    *mys3.Downloader
}

func New(id uint64, persistent bool) *Storage {
	if FunctionPrefix == "" {
		FunctionPrefix = string(FunctionPrefixMatcher.ReplaceAll([]byte(lambdacontext.FunctionName), []byte("")))
	}
	return &Storage{
		id:     id,
		repo:   hashmap.New(10000),
		backup: hashmap.New(1000), // Initilize early to buy time for fast backup recovery.
		lineage: util.Ifelse(!persistent, (*types.LineageTerm)(nil), &types.LineageTerm{
			Term: 1,                             // Term start with 1 to avoid uninitialized term ambigulous.
			Ops:  make([]types.LineageOp, 0, 1), // We expect 1 "write" maximum for each term for sparse workload.
		}).(*types.LineageTerm),
		diffrank: NewSimpleDifferenceRank(Backups),
		log:      &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Storage:"},
	}
}

func (s *Storage) Id() uint64 {
	return s.id
}

func (s *Storage) Init(id uint64, persistent bool) (types.Storage, error) {
	if s != nil && s.id == id {
		return s, nil
	} else {
		return New(id, persistent), nil
	}
}

func (s *Storage) ConfigLogger(lvl int, color bool) {
	if colorLogger, ok := s.log.(*logger.ColorLogger); ok {
		colorLogger.Level = lvl
		colorLogger.Verbose = lvl == logger.LOG_LEVEL_ALL
		colorLogger.Color = color
	}
}

func (s *Storage) get(key string) (*types.Chunk, bool) {
	chunk, ok := s.repo.Get(key)
	if ok {
		return chunk.(*types.Chunk), ok
	} else {
		return nil, ok
	}
}

// Storage Implementation
func (s *Storage) Get(key string) (string, []byte, *types.OpRet) {
	var doubleCheck bool

	// Before getting safely, try what is available so far.
	chunk, ok := s.get(key)
	if !ok {
		// Most likely, this is because an imcomplete lineage.
		doubleCheck = true
	} else {
		note := atomic.LoadUint32(&s.safenote)
		if (!chunk.Backup && (note&RECOVERING_MAIN) > 0) && chunk.Term <= s.lineage.Term ||
			(chunk.Backup && (note&RECOVERING_BACKUP) > 0) {
			// Corresponding lineage is recovering, and the chunk is not just set
			doubleCheck = true
		}
		// else: We are ok to continue.
	}

	if doubleCheck {
		s.getSafe.Wait()
		chunk, ok = s.get(key)
	}

	// Now we're safe to proceed.
	if !ok {
		// No entry
		return "", nil, types.OpError(types.ErrNotFound)
	} else if atomic.LoadUint64(&chunk.Available) == chunk.Size {
		// Not recovering
		if chunk.Body == nil {
			return "", nil, types.OpError(types.ErrNotFound)
		} else {
			return chunk.Id, chunk.Access(), types.OpSuccess()
		}
	} else {
		// Recovering, wait to be notified.
		// s.log.Debug("Waiting for the available of %s, availability: %d of %d", chunk.Key, atomic.LoadUint64(&chunk.Available), chunk.Size)
		chunk.Notifier.Wait()
		return chunk.Id, chunk.Access(), types.OpSuccess()
	}
}

func (s *Storage) GetStream(key string) (string, resp.AllReadCloser, *types.OpRet) {
	chunkId, val, ret := s.Get(key)
	if ret.Error() != nil {
		return chunkId, nil, ret
	}

	return chunkId, resp.NewInlineReader(val), types.OpSuccess()
}

func (s *Storage) set(key string, chunk *types.Chunk) {
	s.repo.Set(key, chunk)
	if chunk.Backup {
		s.backup.Set(key, chunk)
	}
}

func (s *Storage) setImpl(key string, chunkId string, val []byte, opt *types.OpWrapper) *types.OpRet {
	s.setSafe.Wait()
	// s.log.Debug("ready to set key %v", key)
	// Lock lineage, ensure operation get processed in the term.
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()
	// s.log.Debug("in mutex of setting key %v", key)

	chunk := types.NewChunk(key, chunkId, val)
	chunk.Term = 1
	if s.lineage != nil {
		chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
		chunk.Bucket = s.getBucket(key)
	}
	s.set(key, chunk)
	if s.chanOps != nil {
		op := &types.OpWrapper{
			LineageOp: types.LineageOp{
				Op:       types.OP_SET,
				Key:      key,
				Id:       chunkId,
				Size:     chunk.Size,
				Accessed: chunk.Accessed,
				Bucket:   chunk.Bucket,
			},
			OpRet: types.OpDelayedSuccess(),
			Body:  val,
		}
		// Copy options. Field "Persisted" only so far.
		if opt != nil {
			op.Persisted = opt.Persisted
		}
		s.chanOps <- op
		// s.log.Debug("local set ok, key %v", key)
		return op.OpRet
	} else {
		return types.OpSuccess()
	}
}

// Set chunk
func (s *Storage) Set(key string, chunkId string, val []byte) *types.OpRet {
	return s.setImpl(key, chunkId, val, nil)
}

// Set chunk using stream
func (s *Storage) SetStream(key string, chunkId string, valReader resp.AllReadCloser) *types.OpRet {
	val, err := valReader.ReadAll()
	if err != nil {
		return types.OpError(fmt.Errorf("error on read stream: %v", err))
	}

	return s.setImpl(key, chunkId, val, nil)
}

func (s *Storage) SetRecovery(key string, chunkId string, size uint64) *types.OpRet {
	_, _, err := s.Get(key)
	if err.Error() == nil {
		return err
	}

	bucket := s.getBucket(key)
	writer := aws.NewWriteAtBuffer(make([]byte, size))

	downloader := s.getS3Downloader()
	ctx := aws.BackgroundContext()
	ctx = context.WithValue(ctx, &ContextKeyLog, s.log)
	if err := downloader.Download(ctx, func(input *mys3.BatchDownloadObject) {
		input.Object.Bucket = s.bucket(&bucket)
		input.Object.Key = aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, key))
		input.Size = size
		input.Writer = writer
	}); err != nil {
		return types.OpError(err)
	}

	return s.setImpl(key, chunkId, writer.Bytes(), &types.OpWrapper{Persisted: true})
}

func (s *Storage) Del(key string, chunkId string) *types.OpRet {
	s.getSafe.Wait()

	chunk, ok := s.get(key)
	if !ok {
		return types.OpError(types.ErrNotFound)
	}

	s.setSafe.Wait()

	// Lock lineage
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	chunk.Term = util.Ifelse(s.lineage != nil, s.lineage.Term+1, 1).(uint64) // Add one to reflect real term.
	chunk.Access()
	chunk.Deleted = true
	chunk.Body = nil

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

func (s *Storage) len() int {
	return s.repo.Len()
}

func (s *Storage) Len() int {
	s.getSafe.Wait()

	return s.repo.Len()
}

func (s *Storage) Keys() <-chan string {
	s.getSafe.Wait()

	// Gather and send key list. We expected num of keys to be small
	all := make([]*types.Chunk, 0, s.Len())
	for keyChunk := range s.repo.Iter() {
		all = append(all, keyChunk.Value.(*types.Chunk))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Accessed.UnixNano() > all[j].Accessed.UnixNano()
	})

	ch := make(chan string)
	go func() {
		for _, chunk := range all {
			ch <- chunk.Key
		}
		close(ch)
	}()

	return ch
}

func (s *Storage) getBucket(key string) string {
	if s.lineage != nil || Buckets == 1 {
		return ""
	}
	return fmt.Sprintf(s.s3bucket, strconv.FormatUint(xxhash.Sum64([]byte(key))%uint64(Buckets), 10))
}

func (s *Storage) bucket(b *string) *string {
	if *b == "" {
		return &s.s3bucketDefault
	} else {
		return b
	}
}

// Lineage Implementation
func (s *Storage) ConfigS3Lineage(bucket string, prefix string) types.Lineage {
	if s.lineage == nil {
		return nil
	}

	s.s3bucket = bucket
	s.s3bucketDefault = fmt.Sprintf(bucket, "")
	s.s3prefix = prefix
	s.setSafe = csync.WaitGroup{}
	s.getSafe = csync.WaitGroup{}
	s.delaySet()
	return s
}

func (s *Storage) IsConsistent(meta *types.LineageMeta) (bool, error) {
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

func (s *Storage) ResetBackup() {
	s.backupMeta = nil
}

func (s *Storage) TrackLineage() {
	if s.chanOps == nil {
		s.lineage.Ops = s.lineage.Ops[:0] // Reset metalogs
		s.chanOps = make(chan *types.OpWrapper, 10)
		s.signalTracker = make(chan *types.CommitOption, 1)
		s.committed = make(chan *types.CommitOption)
		s.resetSet()
		go s.TrackLineage()
		return
	}

	s.log.Debug("Tracking lineage...")

	// This is essential to minimize download memory consumption.
	bufferProvider := s3manager.NewBufferedReadSeekerWriteToPool(1 * 1024 * 1024)
	// Initialize s3 uploader
	smallUploader := s3manager.NewUploader(types.AWSSession(), func(u *s3manager.Uploader) {
		u.Concurrency = 1
		u.BufferProvider = bufferProvider
	})
	largeUploader := s3manager.NewUploader(types.AWSSession(), func(u *s3manager.Uploader) {
		u.Concurrency = types.UploadConcurrency
		u.BufferProvider = bufferProvider
	})
	attemps := 3
	persistedOps := make([]*types.OpWrapper, 0, 10)
	persisted := 0
	// Token is used as concurrency throttler as well as to accept upload result and keep total ordering.
	freeToken := types.UploadConcurrency
	token := make(chan *types.OpWrapper, freeToken)
	var commitOpt *types.CommitOption

	var trackDuration time.Duration
	var trackStart time.Time
	for {
		select {
		case op := <-s.chanOps:
			if op == nil {
				// closed
				s.chanOps = nil
				s.log.Trace("It took %v to track and persist chunks.", trackDuration)
				return
			}

			// Count duration
			if persisted == len(persistedOps) {
				trackStart = time.Now()
			}

			// Fill token
			if freeToken > 0 {
				freeToken--
				token <- nil
			}

			// Try to get token to continue, if a previously operation persisted, handle that first.
			persistedOp := <-token
			if persistedOp != nil {
				// Accept result
				persistedOps[persistedOp.OpIdx] = persistedOp
				for ; persisted < len(persistedOps) && persistedOps[persisted] != nil; persisted++ {
					persistedOp = persistedOps[persisted]
					if persistedOp.OpRet.Wait() == nil {
						s.lineage.Ops = append(s.lineage.Ops, persistedOp.LineageOp)

						// If lineage is not recovered (get unsafe), skip diffrank, it will be replay when lineage is recovered.
						if !s.getSafe.IsWaiting() {
							s.diffrank.AddOp(&op.LineageOp)
						} // else: Skip
					}
				}
			}

			s.log.Debug("Tracking incoming op: %v", op.LineageOp)

			op.OpIdx = len(persistedOps)
			persistedOps = append(persistedOps, nil)

			// Upload to s3
			var failure error
			if op.LineageOp.Op == types.OP_SET && !op.Persisted {
				go func() {
					uploadStart := time.Now()
					for i := 0; i < attemps; i++ {
						if i > 0 {
							s.log.Info("Attemp %d - uploading %s ...", i+1, op.Key)
						}

						upParams := &s3manager.UploadInput{
							Bucket: s.bucket(&op.LineageOp.Bucket),
							Key:    aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, op.LineageOp.Key)),
							Body:   bytes.NewReader(op.Body),
						}
						// Perform an upload.
						uploader := smallUploader
						if int64(len(op.Body)) >= largeUploader.PartSize {
							uploader = largeUploader
						}

						attemptStart := uploadStart
						_, failure = uploader.Upload(upParams)
						if failure != nil {
							s.log.Warn("Attemp %d - failed to upload %s: %v", i+1, op.Key, failure)
						} else {
							// success
							failure = nil
							s.log.Debug("Success to upload %s, upload takes %v, total %v", op.Key, time.Since(attemptStart), time.Since(uploadStart))
							break
						}
					}

					// Success?
					if failure != nil {
						s.log.Error("Failed to upload %s: %v", op.Key, failure)
					}
					op.OpRet.Done(failure)
					token <- op
				}()
			} else {
				op.OpRet.Done()
				token <- op
			}
		case persistedOp := <-token:
			// A operation has been persisted.
			if persistedOp != nil {
				// Fill in the allocated slot.
				persistedOps[persistedOp.OpIdx] = persistedOp

				// Check persisted yet has not been processed operations.
				for ; persisted < len(persistedOps) && persistedOps[persisted] != nil; persisted++ {
					persistedOp = persistedOps[persisted]
					if persistedOp.OpRet.IsDone() {
						s.lineage.Ops = append(s.lineage.Ops, persistedOp.LineageOp)

						// If lineage is not recovered (get unsafe), skip diffrank, it will be replay when lineage is recovered.
						if !s.getSafe.IsWaiting() {
							s.diffrank.AddOp(&persistedOp.LineageOp)
						} // else: Skip
					} else {
						break
					}
				}
			}
			// Refill freeToken
			freeToken++
			// All persisted?
			if persisted == len(persistedOps) {
				// Count duration
				trackDuration += time.Since(trackStart)

				// Signal tracker if commit initiated.
				if commitOpt != nil {
					s.signalTracker <- commitOpt
				}
			}
		// The tracker will only be signaled after tracked all existing operations.
		case opt := <-s.signalTracker:
			if len(s.chanOps) > 0 {
				// We wait for chanOps get drained.
				s.signalTracker <- opt
			} else if persisted < len(persistedOps) {
				// Wait for being persisted and signalTracker get refilled.
				commitOpt = opt
			} else {
				// All operations persisted.
				persistedOps = persistedOps[:0]
				persisted = 0
				commitOpt = nil
				s.doCommit(opt)
			}
		}
	}
}

func (s *Storage) Commit() (*types.CommitOption, error) {
	if s.signalTracker == nil {
		return nil, ERR_TRACKER_NOT_STARTED
	}

	s.log.Debug("Commiting lineage.")

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
	s.signalTracker <- option
	option = <-s.committed

	// Flag checked
	return option, nil
}

func (s *Storage) StopTracker(option *types.CommitOption) types.LineageStatus {
	if s.signalTracker != nil {
		// Reset bytes uploaded
		option.BytesUploaded = 0

		// Signal for double check and wait for confirmation.
		s.signalTracker <- option
		<-s.committed

		// Clean up
		close(s.chanOps)
		runtime.Gosched() // Take time to finalize.
		if s.recovered != nil {
			// The recovery is not complete, discard current term and replaced with whatever recovered.
			// The node will try recovery in next invocation.
			s.lineage = s.recovered
		}
		s.signalTracker = nil
		s.committed = nil
		s.log.Debug("Tracking lineage stopped.")
	}

	return s.Status()
}

func (s *Storage) Status() types.LineageStatus {
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
func (s *Storage) Recover(meta *types.LineageMeta) (bool, chan error) {
	if meta.Consistent {
		return false, nil
	}

	s.log.Debug("Start recovery of node %d(bak:%v).", meta.Meta.Id, meta.Backup)

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
			s.ClearBackup()
		}
	}

	// Flag get as unsafe
	s.delayGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
	chanErr := make(chan error, 1)
	go s.doRecover(old, meta, chanErr)

	// Fast recovery if the node is not backup and significant enough.
	return !meta.Backup && s.diffrank.IsSignificant(meta.DiffRank), chanErr
}

func (s *Storage) ClearBackup() {
	for keyValue := range s.backup.Iter() {
		chunk := keyValue.Value.(*types.Chunk)
		chunk.Body = nil
		s.repo.Del(keyValue.Key)
		s.backup.Del(keyValue.Key)
	}
}

func (s *Storage) doCommit(opt *types.CommitOption) {
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
	} else {
		// No operation since last signal.This will be quick and we are ready to exit lambda.
		// DO NOT close "committed", since there will be a double check on stoping the tracker.
		if opt.Checked {
			s.log.Debug("Double checked: no more term to commit, signal committed.")
		} else {
			s.log.Debug("Checked: no more term to commit, signal committed.")
		}
		s.committed <- opt
	}
}

func (s *Storage) doCommitTerm(lineage *types.LineageTerm, uploader *s3manager.Uploader) (*types.LineageTerm, uint64, error) {
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

func (s *Storage) doSnapshot(lineage *types.LineageTerm, uploader *s3manager.Uploader) (*types.LineageTerm, error) {
	start := time.Now()
	// Construct object list.
	allOps := make([]types.LineageOp, 0, s.len())
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

func (s *Storage) doRecover(lineage *types.LineageTerm, meta *types.LineageMeta, chanErr chan error) {
	// Initialize s3 api
	downloader := s.getS3Downloader()

	// Recover lineage
	start := time.Now()
	lineageBytes, terms, numOps, err := s.doRecoverLineage(lineage, meta.Meta, downloader)
	stop1 := time.Since(start)
	if err != nil {
		chanErr <- err
	}

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

	// s.log.Debug("tbds %d: %v", meta.Meta.Id, tbds)
	// for i, t := range tbds {
	// 	s.log.Debug("%d: %v", i, *t)
	// }

	start2 := time.Now()
	objectBytes := 0
	if len(tbds) > 0 {
		objectBytes, err = s.doRecoverObjects(tbds, downloader)
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

func (s *Storage) doRecoverLineage(lineage *types.LineageTerm, meta *protocol.Meta, downloader *mys3.Downloader) (int, []*types.LineageTerm, int, error) {
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

func (s *Storage) doReplayLineage(meta *types.LineageMeta, terms []*types.LineageTerm, numOps int) []*types.Chunk {
	var fromSnapshot uint64
	if !meta.Backup && terms[0].DiffRank > 0 {
		// Recover start with a snapshot
		fromSnapshot = terms[0].Term
		numOps -= s.len() + s.backup.Len() // Because snapshot includes what have been in repo, exclued them as estimation.
		if numOps < 10 {
			numOps = 10 // Arbitary 10 minimum.
		}
	}
	tbds := make([]*types.Chunk, 0, numOps) // To be downloaded. Initial capacity is estimated by the # of ops.

	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	// Deal with the key that is currently serving.
	servingKey := meta.Tips.Get(protocol.TIP_SERVING_KEY)
	if servingKey != "" {
		// If serving_key exists, we are done. Deteled serving_key is unlikely and will be verified later.
		if _, existed := s.get(servingKey); !existed {
			chunk := &types.Chunk{
				Key:    servingKey,
				Body:   nil,
				Backup: meta.Backup,
			}
			// Occupy the repository, details will be filled later.
			s.set(servingKey, chunk)
			// Add to head so it will be load first, no duplication is possible because it is inserted to repository.
			tbds = append(tbds, chunk)
		}
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
					chunk.Deleted = true
					chunk.Body = nil
				} else if op.Key == servingKey {
					// Updaet data of servingKey, overwrite is OK.
					chunk.Id = op.Id
					chunk.Size = op.Size
					chunk.Term = term.Term
					chunk.Accessed = op.Accessed
					chunk.Bucket = op.Bucket
				} else {
					// Reset
					// Although unlikely, dealing reset by deleting it first and add it later.
					chunk.Deleted = true
					chunk = nil // Reset so it can be added back later.
				}
			}
			// New or reset chunk
			if chunk == nil && s.isRecoverable(op.Key, meta, false) {
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
		if !tbd.Deleted {
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

func (s *Storage) doRecoverObjects(tbds []*types.Chunk, downloader *mys3.Downloader) (int, error) {
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
	go func() {
		for i := 0; i < len(tbds); i++ {
			if tbds[i].Deleted {
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
		close(inputs)
		more = false
		// s.log.Info("Inputs closed")
	}()

	// Start downloading.
	go func() {
		// iter := &s3manager.DownloadObjectsIterator{ Objects: inputs }
		ctx := aws.BackgroundContext()
		ctx = context.WithValue(ctx, &ContextKeyLog, s.log)
		if err := downloader.DownloadWithIterator(ctx, inputs); err != nil {
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

func (s *Storage) isRecoverable(key string, meta *types.LineageMeta, verify bool) bool {
	if !meta.Backup {
		return true
	}
	s.backupLocator.Reset(int(meta.BackupTotal))
	target, _, _ := s.backupLocator.Locate(key)
	if target == meta.BackupId {
		return true
	} else if verify {
		s.log.Warn("Detected backup reroute error, expected %d, actual %d, key %s", meta.BackupId, target, key)
	}
	return false
}

func (s *Storage) getReadyNotifier(i interface{}, chanNotify chan interface{}) func() error {
	return func() error {
		chanNotify <- i
		return nil
	}
}

func (s *Storage) delaySet() {
	s.setSafe.Add(1)
}

func (s *Storage) resetSet() {
	s.setSafe.Done()
}

func (s *Storage) delayGet(flag uint32) {
	old := atomic.LoadUint32(&s.safenote)
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old|flag) {
		old = atomic.LoadUint32(&s.safenote)
	}
	s.getSafe.Add(1)
}

func (s *Storage) resetGet(flag uint32) {
	s.getSafe.Done()
	old := atomic.LoadUint32(&s.safenote)
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old&^flag) {
		old = atomic.LoadUint32(&s.safenote)
	}
}

func (S *Storage) functionName(id uint64) string {
	return fmt.Sprintf("%s%d", FunctionPrefix, id)
}

func (s *Storage) getS3Downloader() *mys3.Downloader {
	if s.s3Downloader == nil {
		s.s3Downloader = mys3.NewDownloader(types.AWSSession(), func(d *mys3.Downloader) {
			d.Concurrency = Concurrency
			d.RequestOptions = []awsRequest.Option{
				awsRequest.WithResponseReadTimeout(types.AWSServiceTimeout),
			}
		})
	}
	return s.s3Downloader
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
