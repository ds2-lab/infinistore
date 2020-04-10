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
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo/resp"
	"io"
	"net/url"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	CHUNK_KEY = "%schunks/%s"
	LINEAGE_KEY = "%s%s/lineage-%d"
	SNAPSHOT_KEY = "%s%s/snapshot-%d.gz"
)

var (
	AWSRegion      string
	Backups        = 10
	Concurrency    = 5
	Buckets        = 1

	ERR_TRACKER_NOT_STARTED = errors.New("Tracker not started.")
)

// Storage with lineage
type Storage struct {
	// IMOC repository, operations are supposed to be serialized.
	// NOTE: If serialization of operations can not be guarenteed, reconsider the implementation
	//       of "repo" and "mu"
	repo       map[string]*types.Chunk
	log        logger.ILogger

	// Lineage
	lineage    *types.LineageTerm
	recovered  *types.LineageTerm
	snapshot   *types.LineageTerm
	diffrank   LineageDifferenceRank
	getSafe		 chan struct{}
	setSafe    chan struct{}
	chanOps    chan *types.LineageOp    // NOTE: implement an unbounded channel if neccessary.
	signalTracker chan bool
	committed  chan struct{}
	mu         sync.RWMutex             // Mutex for lienage commit.
	notifier   map[string]chan *types.Chunk

	// Persistent backpack
	s3bucket   string
	s3bucketDefault string
	s3prefix   string
	awsSession *awsSession.Session
	uploader   *s3manager.Uploader
}

func New() *Storage {
	// Get is safe by default
	getSafe := make(chan struct{})
	close(getSafe)
	return &Storage{
		repo: make(map[string]*types.Chunk),
		getSafe: getSafe,
		setSafe: make(chan struct{}),
		lineage: &types.LineageTerm{
			Ops: make([]types.LineageOp, 0, 1), // We expect 1 "write" maximum for each term for sparse workload.
		},
		diffrank: NewSimpleDifferenceRank(Backups),
		log: &logger.ColorLogger{ Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Storage:" },
	}
}

func (s *Storage) SetLogLevel(lvl int) {
	if colorLogger, ok := s.log.(*logger.ColorLogger); ok {
		colorLogger.Level = lvl
		colorLogger.Verbose = lvl == logger.LOG_LEVEL_ALL
	}
}

// Storage Implementation
func (s *Storage) Get(key string) (string, []byte, error) {
	<-s.getSafe

	chunk, ok := s.repo[key]
	if !ok {
		// No entry
		return "", nil, types.ErrNotFound
	} else if atomic.LoadUint32(&chunk.Recovering) == types.CHUNK_OK {
		// Not recovering
		if chunk.Body == nil {
			return "", nil, types.ErrNotFound
		} else {
			return chunk.Id, chunk.Access(), nil
		}
	} else {
		// Recovering, acquire lock
		for !atomic.CompareAndSwapUint32(&chunk.Recovering, types.CHUNK_RECOVERING, types.CHUNK_LOCK) {
			// Failed, check current status
			if atomic.LoadUint32(&chunk.Recovering) == types.CHUNK_OK {
				// Deleted item is not worth recovering.
				return chunk.Id, chunk.Access(), nil
			}
			// Someone got lock, yield
			runtime.Gosched()
		}
		// Lock acquired, register notifier
		chanReady := make(chan *types.Chunk)
		s.notifier[chunk.Key] = chanReady
		// Release Lock
		atomic.StoreUint32(&chunk.Recovering, types.CHUNK_RECOVERING)

		// Wait for recovering, use chunk returned.
		chunk = <-chanReady
		return chunk.Id, chunk.Access(), nil
	}
}

func (s *Storage) GetStream(key string) (string, resp.AllReadCloser, error) {
	chunkId, val, err := s.Get(key)
	if err != nil {
		return chunkId, nil, err
	}

	return chunkId, resp.NewInlineReader(val), nil
}

func (s *Storage) Set(key string, chunkId string, val []byte) error {
	<-s.setSafe

	// Lock lineage
	s.mu.Lock()
	defer s.mu.Unlock()

	chunk := types.NewChunk(chunkId, val)
	chunk.Key = key
	chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
	if Buckets > 1 {
		chunk.Bucket = fmt.Sprintf(s.s3bucket, strconv.Itoa(rand.Int() % Buckets))
	}
	s.repo[key] = chunk
	if s.chanOps != nil {
		s.chanOps <- &types.LineageOp{
			Op: types.OP_SET,
			Key: key,
			Id: chunkId,
			Size: chunk.Size,
			Accessed: chunk.Accessed,
			// Ret: make(chan error, 1),
			Bucket: chunk.Bucket,
		}
	}
	return nil
}

func (s *Storage) SetStream(key string, chunkId string, valReader resp.AllReadCloser) error {
	val, err := valReader.ReadAll()
	if err != nil {
		return errors.New(fmt.Sprintf("Error on read stream: %v", err))
	}

	return s.Set(key, chunkId, val)
}

func (s *Storage) Del(key string, chunkId string) error {
	<-s.setSafe

	chunk, ok := s.repo[key]
	if !ok {
		return types.ErrNotFound
	}

	// Lock lineage
	s.mu.Lock()
	defer s.mu.Unlock()

	chunk.Term = s.lineage.Term + 1 // Add one to reflect real term.
	chunk.Access()
	chunk.Body = nil

	if s.chanOps != nil {
		s.chanOps <- &types.LineageOp{
			Op: types.OP_DEL,
			Key: key,
			Id: chunkId,
			Size: chunk.Size,
			Accessed: chunk.Accessed,
			// Ret: make(chan error, 1),
			Bucket: chunk.Bucket,
		}
	}
	return nil
}

func (s *Storage) Len() int {
	<-s.getSafe

	return len(s.repo)
}

func (s *Storage) Keys() <-chan string {
	<-s.getSafe

	// Gather and send key list. We expected num of keys to be small
	all := make([]*types.Chunk, 0, len(s.repo))
	for _, chunk := range s.repo {
		all = append(all, chunk)
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

// Consensus Implementation

func (s *Storage) ConfigS3Lineage(bucket string, prefix string) {
	s.s3bucket = bucket
	s.s3bucketDefault = fmt.Sprintf(bucket, "")
	s.s3prefix = prefix
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

func (s *Storage) IsConsistent(meta *protocol.Meta) bool {
	return s.lineage.Term == meta.Term && s.lineage.Hash == meta.Hash
}

func (s *Storage) TrackLineage() {
	if s.chanOps == nil {
		s.lineage.Ops = s.lineage.Ops[:0] // Reset metalogs
		s.chanOps = make(chan *types.LineageOp, 10)
		s.signalTracker = make(chan bool, 1)
		s.committed = make(chan struct{})
		rand.Seed(time.Now().UnixNano()) // Reseed random.
		close(s.setSafe)
		go s.TrackLineage()
		return
	}

	s.log.Debug("Tracking lineage...")

	// Initialize s3 uploader
	s.uploader = s3manager.NewUploader(s.GetAWSSession())
	attemps := 3

	var trackDuration time.Duration
	for {
		select {
		case op := <-s.chanOps:
			if op == nil {
				// closed
				s.chanOps = nil
				s.log.Trace("It took %v to track and persist chunks.", trackDuration)
				return
			}

			s.log.Debug("Tracking incoming op: %v", op)
			trackStart := time.Now()

			// Upload to s3
			var failure error
			for i := 0; i < attemps; i++ {
				if i > 0 {
					s.log.Info("Attemp %d - uploading %s ...", i + 1, op.Key)
				}

				bucket := op.Bucket
				if bucket == "" {
					bucket = s.s3bucketDefault
				}
				upParams := &s3manager.UploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, op.Key)),
					Body:   bytes.NewReader(s.repo[op.Key].Body),
				}
				// Perform an upload.
				_, failure = s.uploader.Upload(upParams)
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
				// op.Ret <- failure
			} else {
				// Record after upload success, so if upload failed, the lineage will not have the operation.
				s.lineage.Ops = append(s.lineage.Ops, *op)

				// If lineage is not recovered (get unsafe), skip diffrank, it will be replay when lineage is recovered.
				select {
				case <-s.getSafe:
					s.diffrank.AddOp(op)
				default:
					// Skip
				}
				// close(op.Ret)
			}
			trackDuration += time.Since(trackStart)
		// The tracker will only be signaled after tracked all existing operations.
		case full := <-s.signalTracker:
			if len(s.chanOps) > 0 {
				// We wait for chanOps get drained.
				s.signalTracker <- full
			} else {
				s.doCommit(full)
			}
		}
	}
}

func (s *Storage) Commit() error {
	if s.signalTracker == nil {
		return ERR_TRACKER_NOT_STARTED
	}

	s.log.Debug("Commiting lineage.")

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
	full := s.recovered == nil && s.lineage.Term - snapshotTerm >= 4

	// Signal and wait for committed.
	s.signalTracker <- full
	<-s.committed

	return nil
}

func (s *Storage) StopTracker() *protocol.Meta {
	if s.signalTracker != nil {
		// Signal for double check and wait for confirmation.
		s.signalTracker <- false
		<-s.committed

		// Clean up
		close(s.chanOps)
		s.chanOps = nil
		s.signalTracker = nil
		s.committed = nil
		s.uploader = nil
	}

	meta := &protocol.Meta {
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
	return meta
}

// Recover based on the term of specified meta.
// We support partial recovery. Errors during recovery will be sent to returned channel.
// The recovery ends if returned channel is closed.
// If the first return value is false, no fast recovery is needed.
func (s *Storage) Recover(meta *protocol.Meta) (bool, chan error) {
	if s.IsConsistent(meta) {
		return false, nil
	}

	tips, err := url.ParseQuery(meta.Tip)
	if err != nil {
		s.log.Warn("Invalid tips(%s) in protocol meta: %v", meta.Tip, err)
	}

	// Copy lineage data for recovery, update term to the recent record, and we are ready for write operatoins.
	old := &types.LineageTerm{
		Term: s.lineage.Term,
		Updates: s.lineage.Updates,
	}
	s.lineage.Term = meta.Term
	s.lineage.Updates = meta.Updates
	s.lineage.Hash = meta.Hash

	// Flag get as unsafe
	s.getSafe = make(chan struct{})
	chanErr := make(chan error, 1)
	go s.doRecover(old, meta, tips, chanErr)

	// Fast recovery if the node is not backup and significant enough.
	return s.diffrank.IsSignificant(meta.DiffRank) && tips.Get(protocol.TIP_ID) == "", chanErr
}

func (s *Storage) doCommit(full bool) {
	if len(s.lineage.Ops) > 0 {
		snapshotted := false
		var uploadedBytes uint64

		start := time.Now()
		term, err := s.doCommitTerm(s.lineage)
		stop1 := time.Now()
		if err != nil {
			s.log.Warn("Failed to commit term %d: %v", term, err)
		} else if full {
			uploadedBytes += s.lineage.Size
			err = s.doSnapshot(s.lineage)
			if err != nil {
				s.log.Warn("Failed to snapshot up to term %d: %v", term, err)
			} else {
				snapshotted = true
				uploadedBytes += s.snapshot.Size
			}
		} else {
			uploadedBytes += s.lineage.Size
		}
		end := time.Now()

		// Can be other operations during persisting, signal tracker again.
		// This time, ignore argument "full" if snapshotted.
		s.log.Debug("Term %d commited, resignal to check possible new term during committing.", term)
		s.log.Trace("action,lineage,snapshot,elapsed,bytes")
		s.log.Trace("commit,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop1), end.Sub(start), uploadedBytes)
		s.signalTracker <- full && !snapshotted
	} else {
		// No operation since last signal.This will be quick and we are ready to exit lambda.
		// DO NOT close "committed", since there will be a double check on stoping the tracker.
		s.log.Debug("No more term to commit, signal committed.")
		s.committed <- struct{}{}
	}
}

func (s *Storage) doCommitTerm(lineage *types.LineageTerm) (uint64, error) {
	// Lock local lineage
	s.mu.Lock()

	// Marshal ops first, so it can be reused largely in calculating hash and to be uploaded.
	raw, err := binary.Marshal(lineage.Ops)
	if err != nil {
		s.mu.Unlock()
		return lineage.Term + 1, err
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
		s.mu.Unlock()
		return term.Term, err
	}
	if err := zipWriter.Close(); err != nil {
		s.mu.Unlock()
		return term.Term, err
	}

	// Update local lineage. Size must be updated before used for uploading.
	lineage.Size = uint64(buf.Len())
	lineage.Ops = lineage.Ops[:0]
	lineage.Term = term.Term
	lineage.Updates += lineage.Size   // Fix local "Updates"
	lineage.Hash = term.Hash
	lineage.DiffRank = s.diffrank.Rank() // Store for snapshot use.
	// Unlock lineage, the storage can server next term while uploading
	s.mu.Unlock()

	// Upload
	params := &s3manager.UploadInput{
		Bucket: aws.String(s.s3bucketDefault),
		Key:    aws.String(fmt.Sprintf(LINEAGE_KEY, s.s3prefix, lambdacontext.FunctionName, term.Term)),
		Body:   buf,
	}
	_, err = s.uploader.Upload(params)
	if err != nil {
		// TODO: Pending and retry at a later time.
		return term.Term, err
	}

	return term.Term, nil
}

func (s *Storage) doSnapshot(lineage *types.LineageTerm) error {
	start := time.Now()
	// Construct object list.
	allOps := make([]types.LineageOp, 0, len(s.repo))
	for _, chunk := range s.repo {
		if chunk.Term <= lineage.Term {
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
		return err
	}
	if err := zipWriter.Close(); err != nil {
		return err
	}
	// Release "Ops" and update size. Size must be updated before used for uploading.
	ss.Ops = nil
	ss.Size = uint64(buf.Len())

	s.log.Trace("It took %v to snapshot %d chunks.", time.Since(start), len(allOps))

	// Persists.
	params := &s3manager.UploadInput{
		Bucket: aws.String(s.s3bucketDefault),
		Key:    aws.String(fmt.Sprintf(SNAPSHOT_KEY, s.s3prefix, lambdacontext.FunctionName, ss.Term)),
		Body:   buf,
	}
	if _, err := s.uploader.Upload(params); err != nil {
		// TODO: Add retrial
		return err
	}
	s.log.Debug("buflen: %v", buf.Len())

	// Update local snapshot.
	s.snapshot = ss

	return nil
}

func (s *Storage) doRecover(lineage *types.LineageTerm, meta *protocol.Meta, tips url.Values, chanErr chan error) {
	// Initialize s3 api
	downloader := s.getS3Downloader(false)

	// Recover lineage
	start := time.Now()
	receivedBytes, terms, numOps, err := s.doRecoverLineage(lineage, meta, downloader)
	if err != nil {
		chanErr <- err
		if lineage == nil {
			// Unable to continue, stop
			close(s.getSafe)
			close(chanErr)
			return
		}
		// Continue to recover anything in lineage.
	}
	stop1 := time.Now()

	if len(terms) == 0 {
		// No term recovered
		s.recovered = lineage   // Flag for incomplete recovery
		s.log.Error("No term is recovered.")

		close(s.getSafe)
		close(chanErr)
		return
	}

	// Replay lineage
	tbd := s.doReplayLineage(meta, tips, terms, numOps)
	// Now get is safe
	close(s.getSafe)

	stop2 := time.Now()
	if len(tbd) > 0 {
		if n, err := s.doRecoverObjects(tbd, downloader); err != nil {
			chanErr <- err
			receivedBytes += n
		} else {
			receivedBytes += n
		}
	}
	end := time.Now()

	s.notifier = nil
	s.log.Debug("End recovery")
	s.log.Trace("action,lineage,objects,elapsed,bytes")
	s.log.Trace("recover,%d,%d,%d,%d", stop1.Sub(start), end.Sub(stop2), end.Sub(start), receivedBytes)
	close(chanErr)
}

func (s *Storage) doRecoverLineage(lineage *types.LineageTerm, meta *protocol.Meta, downloader S3Downloader) (int, []*types.LineageTerm, int, error) {
	// meta.Updates - meta.SnapshotUpdates + meta.SnapshotSize < meta.Updates - lineage.Updates
	baseTerm := lineage.Term
	snapshot := false
	if meta.SnapshotUpdates - meta.SnapshotSize > lineage.Updates {
		// Recover lineage from snapshot
		baseTerm = meta.SnapshotTerm - 1
		snapshot = true
		s.log.Info("Recovering from snapshot of term %d to term %d", meta.SnapshotTerm, meta.Term)
	} else {
		s.log.Info("Recovering from term %d to term %d", lineage.Term, meta.Term)
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
		inputs[0].Object = &s3.GetObjectInput {
			Bucket: aws.String(s.s3bucketDefault),
			Key: aws.String(fmt.Sprintf(SNAPSHOT_KEY, s.s3prefix, lambdacontext.FunctionName, baseTerm + 1)), // meta.SnapshotTerm
		}
		inputs[0].Writer = new(aws.WriteAtBuffer) // aws.NewWriteAtBuffer(make([]byte, meta.SnapshotSize))
		inputs[0].After = s.getReadyNotifier(0, chanNotify)
		inputs[0].Small = true
		// Skip 0
		from++
	}
	// Setup inputs for terms downloading.
	for from < len(inputs) {
		inputs[from].Object = &s3.GetObjectInput {
			Bucket: aws.String(s.s3bucketDefault),
			Key: aws.String(fmt.Sprintf(LINEAGE_KEY, s.s3prefix, lambdacontext.FunctionName, baseTerm + uint64(from) + 1)),
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

func (s *Storage) doReplayLineage(meta *protocol.Meta, tips url.Values, terms []*types.LineageTerm, numOps int) []*types.Chunk {
	var fromSnapshot uint64
	if terms[0].DiffRank > 0 {
		// Recover start with a snapshot
		fromSnapshot = terms[0].Term
		numOps -= len(s.repo)      // Because snapshot includes what have been in repo, exclued them as estimation.
	}
	tbd := make([]*types.Chunk, 0, numOps)  // To be downloaded. Initial capacity is estimated by the # of ops.

	s.mu.Lock()
  defer s.mu.Unlock()

	// Deal with the key that is currently serving.
	if serving_key := tips.Get(protocol.TIP_SERVING_KEY); serving_key != "" {
		// If serving_key exists, we are done. Deteled serving_key is unlikely and will be verified later.
		if chunk, existed := s.repo[serving_key]; !existed {
			chunk = &types.Chunk{
				Key: serving_key,
				Body: nil,
				Recovering: 1,
			}
			// Occupy the repository, details will be filled later.
			s.repo[serving_key] = chunk
			// Add to head so it will be load first, no duplication is possible because it is inserted to repository.
			tbd = append(tbd, chunk)
		}
	}
	// Replay operations
	if fromSnapshot > 0 {
		// Diffrank is supposed to be a moving value, we should replay it as long as possible.
		s.diffrank.Reset(terms[0].DiffRank)	// Reset diffrank if recover from the snapshot
	}
	for _, term := range terms {
		for i := 0; i < len(term.Ops); i++ {
			op := &term.Ops[i]

			// Replay diffrank, skip ops in snapshot.
			// Condition: !fromSnapshot || term.Term > meta.SnapshotTerm
			if term.Term > fromSnapshot {   // Simplified.
				s.diffrank.AddOp(op)
			}

			if chunk, existed := s.repo[op.Key]; existed {
				if chunk.Term > s.lineage.Term {
					// Skip new incoming write operations during recovery.
					continue
				}
				if op.Op == types.OP_DEL {
					chunk.Body = nil
					chunk.Recovering = 0   // Reset in case it is the serving_key despite unlikely.
				} else if chunk.Body == nil && chunk.Recovering == 0 {
					// Deal with reset after deletion
					// TODO: eliminate duplication
					chunk.Recovering = 1
					tbd = append(tbd, chunk)
				}
				// Updaet data anyway.
				chunk.Id = op.Id
				chunk.Size = op.Size
				chunk.Term = term.Term
				chunk.Accessed = op.Accessed
				chunk.Bucket = op.Bucket
			} else {
				// TODO: Filter based on TIP_ID
				chunk := &types.Chunk{
					Key: op.Key,
					Id: op.Id,
					Body: nil,
					Size: op.Size,
					Term: term.Term,
					Accessed: op.Accessed,
					Bucket: op.Bucket,
				}
				if op.Op != types.OP_DEL {
					chunk.Recovering = 1
					tbd = append(tbd, chunk)
				}
				s.repo[op.Key] = chunk
			}
		}

		// Passing by, update local snapshot
		if term.Term == meta.SnapshotTerm {
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

	// Update local lineage.
	lastTerm := terms[len(terms) - 1]
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
	// Initialize notifier if neccessary
	if len(tbd) > 0 {
		s.notifier = make(map[string]chan *types.Chunk)
	}

	return tbd
}

func (s *Storage) doRecoverObjects(tbd []*types.Chunk, downloader S3Downloader) (int, error) {
	// Setup receivers
	inputs := make([]S3BatchDownloadObject, len(tbd))
	chanNotify := make(chan int, len(inputs))
	chanError := make(chan error)
	succeed := 0
	received := 0
	receivedBytes := 0

	// Setup inputs for terms downloading.
	for i := 0; i < len(inputs); i++ {
		bucket := tbd[i].Bucket
		if bucket == "" {
			bucket = s.s3bucketDefault
		}
		inputs[i].Object = &s3.GetObjectInput {
			Bucket: aws.String(bucket),
			Key: aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, tbd[i].Key)),
		}
		// tbd[i].Body = make([]byte, 0)
		inputs[i].Writer = new(aws.WriteAtBuffer) // aws.NewWriteAtBuffer(tbd[i].Body)
		inputs[i].After = s.getReadyNotifier(i, chanNotify)
		if tbd[i].Size <= downloader.GetDownloadPartSize() {
			inputs[i].Small = true
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
	for received < len(inputs) || succeed < len(inputs) {
		select {
		case err := <-chanError:
			return receivedBytes, err
		case idx := <-chanNotify:
			received++
			buffer := inputs[idx].Writer.(*aws.WriteAtBuffer)
			if uint64(len(buffer.Bytes())) == tbd[idx].Size {
				succeed++
				tbd[idx].Body = buffer.Bytes()
			}
			receivedBytes += len(tbd[idx].Body)

			// Reset "Recovering" status
			// Acquire lock
			for !atomic.CompareAndSwapUint32(&tbd[idx].Recovering, types.CHUNK_RECOVERING, types.CHUNK_LOCK) {
				// Someone got lock, yield and retry.
				runtime.Gosched()
			}
			// Lock acquired, notify.
			if chanReady, existed := s.notifier[tbd[idx].Key]; existed {
				chanReady <- tbd[idx]
			}
			atomic.StoreUint32(&tbd[idx].Recovering, types.CHUNK_OK)
		}
	}
	return receivedBytes, nil
}

func (s *Storage) getReadyNotifier(i int, chanNotify chan int) func() error {
	return func() error {
		chanNotify <- i
		return nil
	}
}

// S3 Downloader Optimization
// Because the minimum concurrent download unit for aws download api is 5M,
// this optimization will effectively lower this limit for batch downloading
type S3Downloader []*s3manager.Downloader

type S3BatchDownloadObject struct {
	Object *s3.GetObjectInput
	Writer io.WriterAt
	After func() error
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
		go d.Download(ctx, d[i], chanSmall, chanErr, &wg)
	}
	// Launch large object downloaders
	if !smallOnly && len(d) > Concurrency {
		chanLarge = make(chan *S3BatchDownloadObject, 1)
		go d.Download(ctx, d[Concurrency], chanLarge, chanErr, &wg)
	}
	// Collect errors
	go func() {
		for err := range chanErr {
			errs = append(errs, err)
		}
	}()

	for i := 0; i < len(iter); i++ {
		if iter[i].Small {
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
	wg.Add(1)
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
