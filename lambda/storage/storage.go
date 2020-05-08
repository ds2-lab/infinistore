package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/cespare/xxhash"
	"github.com/cornelk/hashmap"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/util"
	csync "github.com/mason-leap-lab/infinicache/common/sync"
	"github.com/mason-leap-lab/redeo/resp"
	"io"
	"regexp"
	"runtime"
	"sort"
	"strconv"
//	"strings"
	"sync"
	"sync/atomic"
	"time"
	"log"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	CHUNK_KEY = "%schunks/%s"
	LINEAGE_KEY = "%s%s/lineage-%d"
	SNAPSHOT_KEY = "%s%s/snapshot-%d.gz"

	RECOVERING_MAIN uint32 = 0x01
	RECOVERING_BACKUP uint32 = 0x02
)

var (
	AWSRegion      string
	Backups        = 10
	Concurrency    = 5
	Buckets        = 1
	FunctionPrefix string
	FunctionPrefixMatcher = regexp.MustCompile(`\d+$`)

	ERR_TRACKER_NOT_STARTED = errors.New("Tracker not started.")
)

// Storage with lineage
type Storage struct {
	// IMOC repository, operations are supposed to be serialized.
	// NOTE: If serialization of operations can not be guarenteed, reconsider the implementation
	//       of "repo" and "mu"
	id         uint64
	repo       *hashmap.HashMap
	log        logger.ILogger

	// Lineage
	lineage    *types.LineageTerm       // The lineage of current/recent term. The lineage is updated to recent term while recovering.
	recovered  *types.LineageTerm       // Stores recovered lineage if it is not fully recovered, and will replace lineage on returning.
	snapshot   *types.LineageTerm       // The latest snapshot of the lineage.
	diffrank   LineageDifferenceRank
	getSafe    csync.WaitGroup
	setSafe    csync.WaitGroup
	safenote   uint32                   // Flag what's going on
	chanOps    chan *types.OpWrapper    // NOTE: implement an unbounded channel if neccessary.
	signalTracker chan *types.CommitOption
	committed  chan *types.CommitOption
	lineageMu  sync.RWMutex             // Mutex for lienage commit.

	// backup
	backup     *hashmap.HashMap        // Just a index, all will be available to repo
	backupMeta *types.LineageMeta      // Only one backup is effective at a time.

	// Persistent backpack
	s3bucket   string
	s3bucketDefault string
	s3prefix   string
	awsSession *awsSession.Session
}

func New(id uint64, persistent bool) *Storage {
	if FunctionPrefix == "" {
		FunctionPrefix = string(FunctionPrefixMatcher.ReplaceAll([]byte(lambdacontext.FunctionName), []byte("")))
	}
	log.Println(FunctionPrefix)
	return &Storage{
		id: id,
		repo: hashmap.New(1024),
		backup: hashmap.New(1024), // Initilize early to buy time for fast backup recovery.
		lineage: util.Ifelse(!persistent, (*types.LineageTerm)(nil), &types.LineageTerm{
			Term: 1,  // Term start with 1 to avoid uninitialized term ambigulous.
			Ops: make([]types.LineageOp, 0, 1), // We expect 1 "write" maximum for each term for sparse workload.
		}).(*types.LineageTerm),
		diffrank: NewSimpleDifferenceRank(Backups),
		log: &logger.ColorLogger{ Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Storage:" },
	}
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
		if ((!chunk.Backup && (note & RECOVERING_MAIN) > 0) && chunk.Term <= s.lineage.Term ||
				(chunk.Backup && (note & RECOVERING_BACKUP) > 0)) {
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
	} else if atomic.LoadUint32(&chunk.Recovering) == types.CHUNK_OK {
		// Not recovering
		if chunk.Body == nil {
			return "", nil, types.OpError(types.ErrNotFound)
		} else {
			return chunk.Id, chunk.Access(), types.OpSuccess()
		}
	} else {
		// Recovering, wait to be notified.
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

	// Lock lineage, ensure operation get processed in the term.
	s.lineageMu.Lock()
	defer s.lineageMu.Unlock()

	chunk := types.NewChunk(key, chunkId, val)
	chunk.Term = util.Ifelse(s.lineage != nil, s.lineage.Term + 1, 1).(uint64) // Add one to reflect real term.
	chunk.Bucket = s.getBucket(key)
	s.set(key, chunk)
	if s.chanOps != nil {
		op := &types.OpWrapper{
			LineageOp: types.LineageOp{
				Op: types.OP_SET,
				Key: key,
				Id: chunkId,
				Size: chunk.Size,
				Accessed: chunk.Accessed,
				Bucket: chunk.Bucket,
			},
			OpRet: types.OpDelayedSuccess(),
			Body: val,
		}
		// Copy options. Field "Persisted" only so far.
		if opt != nil {
			op.Persisted = opt.Persisted
		}
		s.chanOps <- op
		return op.OpRet
	} else {
		return types.OpSuccess()
	}
}

func (s *Storage) Set(key string, chunkId string, val []byte) *types.OpRet {
	return s.setImpl(key, chunkId, val, nil)
}

func (s *Storage) SetStream(key string, chunkId string, valReader resp.AllReadCloser) *types.OpRet {
	val, err := valReader.ReadAll()
	if err != nil {
		return types.OpError(errors.New(fmt.Sprintf("Error on read stream: %v", err)))
	}

	return s.setImpl(key, chunkId, val, nil)
}

func (s *Storage) SetRecovery(key string, chunkId string) *types.OpRet {
	bucket := s.getBucket(key)
	object := &s3.GetObjectInput {
		Bucket: s.bucket(&bucket),
		Key: aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, key)),
	}
	writer := new(aws.WriteAtBuffer)
	downloader := s3manager.NewDownloader(s.GetAWSSession())
	if _, err := downloader.Download(writer, object); err != nil {
		return types.OpError(err)
	}

	return s.setImpl(key, chunkId, writer.Bytes(), &types.OpWrapper{ Persisted: true })
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

	chunk.Term = util.Ifelse(s.lineage != nil, s.lineage.Term + 1, 1).(uint64) // Add one to reflect real term.
	chunk.Access()
	chunk.Deleted = true
	chunk.Body = nil

	if s.chanOps != nil {
		op := &types.OpWrapper{
			LineageOp: types.LineageOp{
				Op: types.OP_DEL,
				Key: key,
				Id: chunkId,
				Size: chunk.Size,
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
	return fmt.Sprintf(s.s3bucket, strconv.FormatUint(xxhash.Sum64([]byte(key)) % uint64(Buckets), 10))
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
	s.delaySet()
	return s
}

func (s *Storage) GetAWSSession() *awsSession.Session {
	if s.awsSession == nil {
		s.awsSession = awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
			SharedConfigState: awsSession.SharedConfigEnable,
			Config:            aws.Config{Region: aws.String(AWSRegion)},
		}))
	}
	return s.awsSession
}

func (s *Storage) IsConsistent(meta *types.LineageMeta) (bool, error) {
	lineage := s.lineage
	if meta.Backup {
		if s.backupMeta == nil || s.backupMeta.BackupId != meta.BackupId || s.backupMeta.BackupTotal != meta.BackupTotal {
			return false, nil
		}
		lineage = types.LineageTermFromMeta(s.backupMeta)
	}
	if lineage.Term > meta.Term {
		return false, errors.New(fmt.Sprintf(
			"Detected staled term of lambda %d, expected at least %d, have %d",
			meta.Id, lineage.Term, meta.Term))
	}
	// Don't check hash if term is the start term(1).
	return lineage.Term == meta.Term && (meta.Term == 1 || lineage.Hash == meta.Hash), nil
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

	// Initialize s3 uploader
	smallUploader := s3manager.NewUploader(s.GetAWSSession(), func(u *s3manager.Uploader) {
		u.Concurrency = 1
	})
	largeUploader := s3manager.NewUploader(s.GetAWSSession())
	attemps := 3
	persistedOps := make([]*types.OpWrapper, 0, 10)
	persisted := 0
	// Token is used as concurrency throttler as well as to accept upload result and keep total ordering.
	freeToken := Concurrency
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
						}	// else: Skip
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
					for i := 0; i < attemps; i++ {
						if i > 0 {
							s.log.Info("Attemp %d - uploading %s ...", i + 1, op.Key)
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
						_, failure = uploader.Upload(upParams)
						if failure != nil {
							s.log.Warn("Attemp %d - failed to upload %s: %v", i + 1, op.Key, failure)
						} else {
							// success
							failure = nil
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
			if persistedOp != nil {
				// Accept result
				persistedOps[persistedOp.OpIdx] = persistedOp
				for ; persisted < len(persistedOps) && persistedOps[persisted] != nil; persisted++ {
					persistedOp = persistedOps[persisted]
					if persistedOp.OpRet.Wait() == nil {
						s.lineage.Ops = append(s.lineage.Ops, persistedOp.LineageOp)

						// If lineage is not recovered (get unsafe), skip diffrank, it will be replay when lineage is recovered.
						if !s.getSafe.IsWaiting() {
							s.diffrank.AddOp(&persistedOp.LineageOp)
						} // else: Skip
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
	// Hard-coded to 5, noted s.lineage.Term is one term smaller before commit
	option.Full = s.recovered == nil && s.lineage.Term - snapshotTerm >= 4

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
		runtime.Gosched()   // Take time to finalize.
		if s.recovered != nil {
			// The recovery is not complete, discard current term and replaced with whatever recovered.
			// The node will try recovery in next invocation.
			s.lineage = s.recovered
		}
		s.signalTracker = nil
		s.committed = nil
	}

	return s.Status()
}

func (s *Storage) Status() types.LineageStatus {
	meta := &protocol.Meta {
		Id: s.id,
		Term: s.lineage.Term,
		Updates: s.lineage.Updates,
		DiffRank: s.diffrank.Rank(),
		Hash: s.lineage.Hash,
	}
	if s.snapshot != nil {
		meta.SnapshotTerm = s.snapshot.Term
		meta.SnapshotUpdates = s.snapshot.Updates
		meta.SnapshotSize = s.snapshot.Size
	}
	if s.backupMeta != nil {
		return types.LineageStatus{ meta, s.backupMeta.Meta }
	} else {
		return types.LineageStatus{ meta }
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

	// Copy lineage data for recovery, update term to the recent record, and we are ready for write operatoins.
	var old *types.LineageTerm
	if !meta.Backup {
		old = &types.LineageTerm{
			Term: s.lineage.Term,
			Updates: s.lineage.Updates,
		}
		s.lineage.Term = meta.Term
		s.lineage.Updates = meta.Updates
		s.lineage.Hash = meta.Hash
		s.log.Debug("During recovery, write operations enabled at term %d", s.lineage.Term + 1)
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
			for keyValue := range s.backup.Iter() {
				s.repo.Del(keyValue.Key)
				s.backup.Del(keyValue.Key)
			}
		}
	}

	// Flag get as unsafe
	s.delayGet(util.Ifelse(meta.Backup, RECOVERING_BACKUP, RECOVERING_MAIN).(uint32))
	chanErr := make(chan error, 1)
	go s.doRecover(old, meta, chanErr)

	// Fast recovery if the node is not backup and significant enough.
	return !meta.Backup && s.diffrank.IsSignificant(meta.DiffRank), chanErr
}

func (s *Storage) doCommit(opt *types.CommitOption) {
	if len(s.lineage.Ops) > 0 {
		var bytesUploaded uint64
		uploader := s3manager.NewUploader(s.GetAWSSession(), func(u *s3manager.Uploader) {
			u.Concurrency = 1
		})

		start := time.Now()
		lineage, term, err := s.doCommitTerm(s.lineage, uploader)
		s.lineage = lineage
		stop1 := time.Now()
		if err != nil {
			s.log.Warn("Failed to commit term %d: %v", term, err)
		} else {
			bytesUploaded += s.lineage.Size

			if !opt.Full || opt.Snapshotted {
				// pass
			} else if snapshot, err := s.doSnapshot(s.lineage, uploader); err != nil {
				s.log.Warn("Failed to snapshot up to term %d: %v", term, err)
			} else {
				bytesUploaded += snapshot.Size
				s.snapshot = snapshot
				opt.Snapshotted = true
			}
		}
		end := time.Now()

		// Can be other operations during persisting, signal tracker again.
		// This time, ignore argument "full" if snapshotted.
		s.log.Debug("Term %d commited, resignal to check possible new term during committing.", term)
		s.log.Trace("action,lineage,snapshot,elapsed,bytes")
		s.log.Trace("commit,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop1), end.Sub(start), bytesUploaded)
		opt.BytesUploaded += bytesUploaded
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
		Term: lineage.Term + 1,
		Updates: lineage.Updates, // Stores "Updates" of the last term, don't forget to fix this on recovery.
		RawOps: raw,
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
	lineage.Updates += lineage.Size   // Fix local "Updates"
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
	if err != nil {
		// TODO: Pending and retry at a later time.
	}
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
				Op: chunk.Op(),
				Key: chunk.Key,
				Id: chunk.Id,
				Size: chunk.Size,
			})
		}
	}

	// Construct the snapshot.
	ss := &types.LineageTerm{
		Term: lineage.Term,
		Updates: lineage.Updates,
		Ops: allOps,
		Hash: lineage.Hash,
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
	downloader := s.getS3Downloader(false)

	// Recover lineage
	start := time.Now()
	receivedBytes, terms, numOps, err := s.doRecoverLineage(lineage, meta.Meta, downloader)
	if err != nil {
		chanErr <- err
	}
	stop1 := time.Now()

	if len(terms) == 0 {
		// No term recovered
		if !meta.Backup {
			s.recovered = lineage   // Flag for incomplete recovery
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

	stop2 := time.Now()
	if len(tbds) > 0 {
		if n, err := s.doRecoverObjects(tbds, downloader); err != nil {
			chanErr <- err
			receivedBytes += n
		} else {
			receivedBytes += n
		}
	}
	end := time.Now()

	s.log.Debug("End recovery node %d.", meta.Meta.Id)
	s.log.Trace("action,lineage,objects,elapsed,bytes")
	s.log.Trace("recover,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop2), end.Sub(start), receivedBytes)
	close(chanErr)
}

func (s *Storage) doRecoverLineage(lineage *types.LineageTerm, meta *protocol.Meta, downloader S3Downloader) (int, []*types.LineageTerm, int, error) {
	// If hash not match, invalidate lineage.
	if meta.Term == lineage.Term && meta.Hash != lineage.Hash {
		lineage.Term = 1
		lineage.Updates = 0
		lineage.Hash = ""
	}

	// meta.Updates - meta.SnapshotUpdates + meta.SnapshotSize < meta.Updates - lineage.Updates
	baseTerm := lineage.Term
	snapshot := false
	if meta.SnapshotUpdates - meta.SnapshotSize > lineage.Updates {
		// Recover lineage from snapshot
		baseTerm = meta.SnapshotTerm - 1
		snapshot = true
		s.log.Info("Recovering node %d from snapshot of term %d to term %d", meta.Id, meta.SnapshotTerm, meta.Term)
	} else {
		s.log.Info("Recovering node %d from term %d to term %d", meta.Id, lineage.Term, meta.Term)
	}

	// Setup receivers
	inputs := make([]S3BatchDownloadObject, meta.Term - baseTerm)
	receivedFlags := make([]bool, len(inputs))
	receivedTerms := make([]*types.LineageTerm, 0, len(inputs))
	chanNotify := make(chan int, len(inputs))
	chanError := make(chan error)
	from := 0
	received := 0
	receivedBytes := 0
	receivedOps := 0

	// Setup input for snapshot downloading.
	if snapshot {
		inputs[0].Object = &s3.GetObjectInput{
			Bucket: aws.String(s.s3bucketDefault),
			Key: aws.String(fmt.Sprintf(SNAPSHOT_KEY, s.s3prefix, s.functionName(meta.Id), baseTerm + 1)), // meta.SnapshotTerm
		}
		inputs[0].Writer = new(aws.WriteAtBuffer) // aws.NewWriteAtBuffer(make([]byte, meta.SnapshotSize))
		inputs[0].After = s.getReadyNotifier(0, chanNotify)
		inputs[0].Small = true
		// Skip 0
		from++
	}
	// Setup inputs for terms downloading.
	for from < len(inputs) {
		inputs[from].Object = &s3.GetObjectInput{
			Bucket: aws.String(s.s3bucketDefault),
			Key: aws.String(fmt.Sprintf(LINEAGE_KEY, s.s3prefix, s.functionName(meta.Id), baseTerm + uint64(from) + 1)),
		}
		inputs[from].Writer = new(aws.WriteAtBuffer)
		inputs[from].After = s.getReadyNotifier(from, chanNotify)
		inputs[from].Small = true
		from++
	}

	// Start downloading.
	go func() {
		// iter := &s3manager.DownloadObjectsIterator{ Objects: inputs }
		ctx := aws.BackgroundContext()
		ctx = context.WithValue(ctx, "log", s.log)
		if err := downloader.DownloadWithIterator(ctx, inputs, true); err != nil {
			chanError <- err
		}
	}()

	// Wait for snapshot and lineage terms.
	for received < len(inputs) {
		select {
		case err := <-chanError:
			return receivedBytes, receivedTerms, receivedOps, err
		case idx := <-chanNotify:
			receivedFlags[idx] = true
			for received < len(inputs) && receivedFlags[received] {
				raw := inputs[received].Writer.(*aws.WriteAtBuffer).Bytes()
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
					term.RawOps = nil   // Release
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
			numOps = 10	// Arbitary 10 minimum.
		}
	}
	tbds := make([]*types.Chunk, 0, numOps)  // To be downloaded. Initial capacity is estimated by the # of ops.

	s.lineageMu.Lock()
  defer s.lineageMu.Unlock()

	// Deal with the key that is currently serving.
	servingKey := meta.Tips.Get(protocol.TIP_SERVING_KEY)
	if servingKey != "" {
		// If serving_key exists, we are done. Deteled serving_key is unlikely and will be verified later.
		if chunk, existed := s.get(servingKey); !existed {
			chunk = &types.Chunk{
				Key: servingKey,
				Body: nil,
				Recovering: 1,
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
		s.diffrank.Reset(terms[0].DiffRank)	// Reset diffrank if recover from the snapshot
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
			if !meta.Backup && term.Term > fromSnapshot {   // Simplified.
				s.diffrank.AddOp(op)
			}

			chunk, existed := s.get(op.Key);
			if existed {
				if chunk.Term > s.lineage.Term {
					// Skip new incoming write operations during recovery.
					continue
				}

				if op.Op == types.OP_DEL {
					// Deletion after set will be dealed on recovering objects.
					chunk.Deleted = true
					chunk.Body = nil
					chunk.Recovering = 0   // Although unlikely, reset in case it is the serving_key.
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
					chunk.Recovering = 0
					chunk = nil           // Reset so it can be added back later.
				}
			}
			// New or reset chunk
			if chunk == nil && s.isRecoverable(op.Key, meta, false) {
				// Main repository or backup repository if backup ID matches.
				chunk := &types.Chunk{
					Key: op.Key,
					Id: op.Id,
					Body: nil,
					Size: op.Size,
					Term: term.Term,
					Accessed: op.Accessed,
					Bucket: op.Bucket,
					Backup: meta.Backup,
				}
				// New chunk can't be a deleted chunk, just in case something wrong.
				if op.Op != types.OP_DEL {
					chunk.Recovering = 1
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
				s.snapshot.Size = term.Size     // Use real value
			} else {
				s.snapshot.Size = meta.SnapshotSize  // We didn't start from a snapshot, copy passed from the proxy.
			}
			s.snapshot.DiffRank = s.diffrank.Rank()
		}
	}

	// Now tbds are settled, initiate notifiers
	for _, tbd := range tbds {
		if tbd.Recovering == types.CHUNK_RECOVERING {
			tbd.Notifier.Add(1)
		}
	}

	// Update local lineage.
	lastTerm := terms[len(terms) - 1]
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

func (s *Storage) doRecoverObjects(tbds []*types.Chunk, downloader S3Downloader) (int, error) {
	// Setup receivers
	inputs := make([]S3BatchDownloadObject, len(tbds))
	chanNotify := make(chan int, Concurrency)
	chanError := make(chan error)
	succeed := 0
	received := 0
	receivedBytes := 0

	// Setup inputs for terms downloading.
	for i := 0; i < len(tbds); i++ {
		inputs[i].Object = &s3.GetObjectInput {
			Bucket: s.bucket(&tbds[i].Bucket),
			Key: aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, tbds[i].Key)),
		}
		// tbds[i].Body = make([]byte, tbds[i].Size)  // We don't initialize buffer here, or partial download can't be detected.
		inputs[i].Writer = new(aws.WriteAtBuffer)     // aws.NewWriteAtBuffer(tbds[i].Body)
		inputs[i].After = s.getReadyNotifier(i, chanNotify)

		// Custom properties, remove if use the downloader in AWS SDK
		// Also, change Notifiers initializations logic in doReplayLineage
		if tbds[i].Size <= downloader.GetDownloadPartSize() {
			inputs[i].Small = true   // Size hint.
		}
		if tbds[i].Deleted {
			// Invalidate and skip deleted objects after set
			inputs[i].Invalid = true
			succeed++
			received++
		}
	}

	// Start downloading.
	go func() {
		// iter := &s3manager.DownloadObjectsIterator{ Objects: inputs }
		ctx := aws.BackgroundContext()
		ctx = context.WithValue(ctx, "log", s.log)
		if err := downloader.DownloadWithIterator(ctx, inputs); err != nil {
			s.log.Error("error on download objects: %v", err)
			chanError <- err
		}
	}()

	// Wait for objects, if all is not successful, wait for err.
	for received < len(inputs) {
		select {
		case err := <-chanError:
			return receivedBytes, err
		case idx := <-chanNotify:
			received++
			buffer := inputs[idx].Writer.(*aws.WriteAtBuffer)
			if uint64(len(buffer.Bytes())) == tbds[idx].Size {
				succeed++
				tbds[idx].Body = buffer.Bytes()
			}
			receivedBytes += len(tbds[idx].Body)

			// Reset "Recovering" status
			if atomic.CompareAndSwapUint32(&tbds[idx].Recovering, types.CHUNK_RECOVERING, types.CHUNK_OK) {
				// Lock acquired, now we are safe to notify.
				tbds[idx].Notifier.Done()
			}
		}
	}
	return receivedBytes, nil
}

func (s *Storage) isRecoverable(key string, meta *types.LineageMeta, verify bool) bool {
	if !meta.Backup {
		return true
	}
	target := xxhash.Sum64([]byte(key)) % uint64(meta.BackupTotal)
	if target == uint64(meta.BackupId) {
		return true
	} else if verify {
		s.log.Warn("Detected backup reroute error, expected %d, actual %d, key %s", meta.BackupId, target, key)
	}
	return false
}

func (s *Storage) getReadyNotifier(i int, chanNotify chan int) func() error {
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
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old | flag) {
		old = atomic.LoadUint32(&s.safenote)
	}
	s.getSafe.Add(1)

}

func (s *Storage) resetGet(flag uint32) {
	s.getSafe.Done()
	old := atomic.LoadUint32(&s.safenote)
	for !atomic.CompareAndSwapUint32(&s.safenote, old, old &^ flag) {
		old = atomic.LoadUint32(&s.safenote)
	}
}

func (S *Storage) functionName(id uint64) string {
	return fmt.Sprintf("%s%d", FunctionPrefix, id)
}

// S3 Downloader Optimization
// Because the minimum concurrent download unit for aws download api is 5M,
// this optimization will effectively lower this limit for batch downloading
type S3Downloader []*s3manager.Downloader

type S3BatchDownloadObject struct {
	Object *s3.GetObjectInput
	Writer io.WriterAt
	After func() error
	Invalid bool
	Small bool
}

func (s *Storage) getS3Downloader(smallOnly bool) S3Downloader{
	num := Concurrency
	if !smallOnly {
		num += 1	// Add 1 more downloader for large objects
	}

	downloader := make([]*s3manager.Downloader, num)
	// Initialize downloaders for small object
	for i := 0; i < Concurrency; i++ {
		downloader[i] = s3manager.NewDownloader(s.GetAWSSession(), func(d *s3manager.Downloader) {
			d.Concurrency = 1
		})
	}
	// Initialize the downloader for large object
	if !smallOnly {
		downloader[Concurrency] = s3manager.NewDownloader(s.GetAWSSession(), func(d *s3manager.Downloader) {
			d.Concurrency = Concurrency
		})
	}
	return downloader
}

func (d S3Downloader) GetDownloadPartSize() uint64 {
	if len(d) > Concurrency {
		return uint64(d[Concurrency].PartSize)
	} else {
		return uint64(s3manager.DefaultDownloadPartSize)
	}
}

func (d S3Downloader) DownloadWithIterator(ctx aws.Context, iter []S3BatchDownloadObject, opts ...interface{}) error {
	smallOnly := len(opts) > 0 && opts[0].(bool)

	var wg sync.WaitGroup
	var errs []s3manager.Error
	chanErr := make(chan s3manager.Error, 1)
	chanSmall := make(chan *S3BatchDownloadObject, Concurrency)
	var chanLarge chan *S3BatchDownloadObject
	// Launch small object downloaders
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go d.Download(ctx, d[i], chanSmall, chanErr, &wg)
	}
	// Launch large object downloaders
	if !smallOnly && len(d) > Concurrency {
		chanLarge = make(chan *S3BatchDownloadObject, 1)
		wg.Add(1)
		go d.Download(ctx, d[Concurrency], chanLarge, chanErr, &wg)
	}
	// Collect errors
	go func() {
		for err := range chanErr {
			errs = append(errs, err)
		}
	}()

	for i := 0; i < len(iter); i++ {
		if iter[i].Invalid {
			continue
		} else if iter[i].Small {
			chanSmall <- &iter[i]
		} else {
			chanLarge <- &iter[i]
		}
	}
	close(chanSmall)
	if chanLarge != nil {
		close(chanLarge)
	}

	wg.Wait()
	close(chanErr)
	if len(errs) > 0 {
		return s3manager.NewBatchError("BatchedDownloadIncomplete", "some objects have failed to download.", errs)
	}
	return nil
}

func (d S3Downloader) Download(ctx aws.Context, downloader *s3manager.Downloader, ch chan *S3BatchDownloadObject, errs chan s3manager.Error, wg *sync.WaitGroup) {
	defer wg.Done()

	for object := range ch {
		if _, err := downloader.Download(object.Writer, object.Object); err != nil {
			errs <- s3manager.Error{err, object.Object.Bucket, object.Object.Key}
		}

		if object.After == nil {
			continue
		}

		if err := object.After(); err != nil {
			errs <- s3manager.Error{err, object.Object.Bucket, object.Object.Key}
		}
	}
}
