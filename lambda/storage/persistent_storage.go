package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"

	// "strings"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsRequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cespare/xxhash"

	mys3 "github.com/mason-leap-lab/infinicache/common/aws/s3"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	CHUNK_KEY = "%schunks/%s"
)

var (
	Concurrency = types.DownloadConcurrency
	Buckets     = 1

	ErrTrackerNotStarted     = errors.New("tracker not started")
	ErrFailedToRecoverObject = errors.New("fail to recover object on demand")
)

type PersistHelper interface {
	onPersisted(*types.OpWrapper)
	onTrackerStopped(interface{})
}

// PersistentStorage Storage with S3 as persistent layer
type PersistentStorage struct {
	*Storage

	chanOps        chan *types.OpWrapper // NOTE: implement an unbounded channel if neccessary.
	signalTracker  chan interface{}
	trackerStopped chan interface{}
	persistHelper  PersistHelper

	// Persistency backpack
	s3bucket        string
	s3bucketDefault string
	s3prefix        string
	s3Downloader    *mys3.Downloader
}

func NewPersistentStorage(id uint64) *PersistentStorage {
	storage := &PersistentStorage{
		Storage: NewStorage(id),
	}
	storage.helper = storage
	storage.persistHelper = storage
	return storage
}

// Storage Implementation
func (s *PersistentStorage) getWithOption(key string, opt *types.OpWrapper) (string, []byte, *types.OpRet) {
	chunk, ok := s.helper.get(key)
	if !ok {
		// No entry
		return "", nil, types.OpError(types.ErrNotFound)
	} else if atomic.LoadUint64(&chunk.Available) == chunk.Size {
		if chunk.Body == nil {
			// Not recovering
			return "", nil, types.OpError(types.ErrNotFound)
		} else {
			return chunk.Id, chunk.Access(), types.OpSuccess()
		}
	} else {
		// Recovering, wait to be notified.
		chunk.Notifier.Wait()
		if chunk.Available == chunk.Size {
			return chunk.Id, chunk.Access(), types.OpSuccess()
		} else {
			return "", nil, types.OpError(types.ErrNotFound)
		}
	}
}

func (s *PersistentStorage) setWithOption(key string, chunkId string, val []byte, opt *types.OpWrapper) *types.OpRet {
	chunk, ok := s.helper.get(key)
	if !ok {
		chunk = s.helper.newChunk(key, chunkId, uint64(len(val)), val)
	} else {
		// No version control at store level, val can be changed.
		chunk.Body = val
		chunk.Size = uint64(len(val))
		chunk.Available = chunk.Size
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

func (s *PersistentStorage) newChunk(key string, chunkId string, size uint64, val []byte) *types.Chunk {
	chunk := types.NewChunk(key, chunkId, val)
	chunk.Size = size
	chunk.Term = 1
	chunk.Bucket = s.getBucket(key)
	return chunk
}

func (s *PersistentStorage) SetRecovery(key string, chunkId string, size uint64) *types.OpRet {
	_, _, err := s.helper.getWithOption(key, nil)
	if err.Error() == nil {
		return err
	}

	chunk := s.helper.newChunk(key, chunkId, size, nil)
	chunk.Notifier.Add(1)
	inserted, loaded := s.repo.GetOrInsert(key, chunk)
	chunk = inserted.(*types.Chunk)
	if loaded {
		chunk.Notifier.Wait()
		if chunk.Available == chunk.Size {
			return types.OpSuccess()
		} else {
			return types.OpError(ErrFailedToRecoverObject)
		}
	}

	chunk.Body = make([]byte, size) // Pre-allocate fixed sized buffer.
	downloader := s.getS3Downloader()
	ctx := aws.BackgroundContext()
	ctx = context.WithValue(ctx, &ContextKeyLog, s.log)
	if err := downloader.Download(ctx, func(input *mys3.BatchDownloadObject) {
		input.Object.Bucket = s.bucket(&chunk.Bucket)
		input.Object.Key = aws.String(fmt.Sprintf(CHUNK_KEY, s.s3prefix, key))
		input.Size = size
		input.Writer = aws.NewWriteAtBuffer(chunk.Body)
		input.After = func() error {
			atomic.AddUint64(&chunk.Available, uint64(input.Downloaded))
			return nil
		}
	}); err != nil {
		s.repo.Del(key)
		chunk.Notifier.Done()
		return types.OpError(err)
	}

	ret := s.helper.setWithOption(key, chunkId, chunk.Body, &types.OpWrapper{Persisted: true})
	chunk.Notifier.Done()
	return ret
}

func (s *PersistentStorage) getBucket(key string) string {
	if Buckets == 1 {
		return ""
	}
	return fmt.Sprintf(s.s3bucket, strconv.FormatUint(xxhash.Sum64([]byte(key))%uint64(Buckets), 10))
}

func (s *PersistentStorage) bucket(b *string) *string {
	if *b == "" {
		return &s.s3bucketDefault
	} else {
		return b
	}
}

func (s *PersistentStorage) ConfigS3(bucket string, prefix string) {
	s.s3bucket = bucket
	s.s3bucketDefault = fmt.Sprintf(bucket, "")
	s.s3prefix = prefix
}

func (s *PersistentStorage) StartTracker() {
	if s.chanOps == nil {
		s.chanOps = make(chan *types.OpWrapper, 10)
		s.signalTracker = make(chan interface{}, 1)
		s.trackerStopped = make(chan interface{})
		go s.StartTracker()
		return
	}

	s.log.Debug("Tracking operations...")

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
	var delayedSignal interface{}

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
						s.persistHelper.onPersisted(persistedOp)
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
						s.persistHelper.onPersisted(persistedOp)
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
				if delayedSignal != nil {
					s.signalTracker <- delayedSignal
				}
			}
		// The tracker will only be signaled after tracked all existing operations.
		case signal := <-s.signalTracker:
			if len(s.chanOps) > 0 {
				// We wait for chanOps get drained.
				s.signalTracker <- signal
			} else if persisted < len(persistedOps) {
				// Wait for being persisted and signalTracker get refilled.
				delayedSignal = signal
			} else {
				// All operations persisted.
				persistedOps = persistedOps[:0]
				persisted = 0
				delayedSignal = nil
				s.persistHelper.onTrackerStopped(signal)
			}
		}
	}
}

func (s *PersistentStorage) onPersisted(persisted *types.OpWrapper) {
	// Default by doing nothing
}

func (s *PersistentStorage) onTrackerStopped(signal interface{}) {
	s.trackerStopped <- signal
}

func (s *PersistentStorage) StopTracker(signal interface{}) {
	if s.signalTracker != nil {
		// Signal tracker to stop and wait
		s.signalTracker <- signal
		<-s.trackerStopped

		// Clean up
		close(s.chanOps)
		runtime.Gosched() // Take time to finalize.
		s.signalTracker = nil
		s.trackerStopped = nil
		s.log.Debug("Operation tracking stopped.")
	}
}

func (s *PersistentStorage) getS3Downloader() *mys3.Downloader {
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
