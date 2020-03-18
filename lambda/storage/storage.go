package storage

import (
	"errors"
	"fmt"
	"github.com/kelindar/binary"
	"github.com/mason-leap-lab/redeo/resp"
	"sort"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/lambda/types"
)

type Storage struct {
	meta types.Meta
	snapshot types.Meta
	repo map[string]*types.Chunk
	// TODO: implement an unbounded channel if neccessary
	backpack Backpack
	s3bucket string
	chanLog chan *types.MetaLog
	done chan struct{}
}

func New() *Storage {
	return &Storage{
		repo: make(map[string]*types.Chunk),
		meta: types.Meta{
			MetaLogs: make([]*types.MetaLog, 0, 1), // We expect 1 "write" maximum for each term for sparse workload.
		},
	}
}

// Storage Implementation

func (s *Storage) Get(key string) (string, []byte, error) {
	chunk, ok := s.repo[key]
	if !ok || chunk.Body == nil {
		return "", nil, types.ErrNotFound
	}

	return chunk.Id, chunk.Access(), nil
}

func (s *Storage) Del(key string, chunkId string) error {
	chunk, ok := s.repo[key]
	if !ok {
		return types.ErrNotFound
	}
	chunk.Access()

	chunk.Body = nil
	return nil
}

func (s *Storage) GetStream(key string) (string, resp.AllReadCloser, error) {
	chunkId, val, err := s.Get(key)
	if err != nil {
		return chunkId, nil, err
	}

	return chunkId, resp.NewInlineReader(val), nil
}

func (s *Storage) Set(key string, chunkId string, val []byte) error {
	chunk := types.NewChunk(chunkId, val)
	chunk.Key = key
	s.repo[key] = chunk
	if s.chanLog != nil {
		s.chanLog <- &types.MetaLog{
			Op: types.OP_SET,
			Key: key,
			Id: chunkId,
			Size: len(val),
			Ret: make(chan error, 1),
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

func (s *Storage) Len() int {
	return len(s.repo)
}

func (s *Storage) Keys() <-chan string {
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

func (s *Storage) SetS3Backpack(bucket string) Backpack {
	s.s3bucket = bucket
	s.backpack = s
	return s
}

func (s *Storage) IsConsistent(meta *protocol.Meta) bool {
	return s.meta.Term == meta.Term && s.meta.Hash == meta.Hash
}

func (s *Storage) StartLogging(meta *protocol.Meta) {
	if s.chanLog == nil {
		s.done = make(chan struct{})
		s.meta.MataLogs = s.meta.MetaLogs[:0] // reset metalogs
		s.chanLog = make(chan *types.MetaLog, 10)
		go s.StartLogging(meta)
		return
	}

	// Initialize s3 api
	sess := awsSession.Must(awsSession.NewSessionWithOptions(awsSession.Options{
		SharedConfigState: awsSession.SharedConfigEnable,
		Config:            aws.Config{Region: aws.String(AWSRegion)},
	}))
	uploader := s3manager.NewUploader(sess)

	for {
		select {
		case metaLog := <-s.chanLog:
			if metaLog == nil {
				// closed
				s.chanLog = nil
				close(s.done())
				return
			}

			// Upload to s3
			_, body, err := s.GetStream(metaLog.Key)
			if err != nil {
				metaLog.Ret <- err
				continue
			}

			upParams := &s3manager.UploadInput{
				Bucket: &s.bucket,
				Key:    &metaLog.Key,
				Body:   body,
			}
			// Perform an upload.
			result, err := uploader.Upload(upParams)
			body.Close()
			if err != nil {
				metaLog.Ret <- err
			} else {
				close(metaLog.Ret)
			}
		}
	}
}

func (s *Storage) Commit(full bool) *protocol.Meta {
	if s.chanLog == nil {
		return
	}

	close(s.chanLog)
	// wait for all done.
	<-s.done

	if len(s.meta.MetaLogs) > 0 {
		// TODO implement metaLog
		// s.metaLog(&s.meta)

		if full {
			// TODO implement metaSnapshot
			// s.metaSnapshot()
		}
	}

	return &protocol.Meta {
		Term: s.meta.Term,
		Updates: s.meta.Updates
		Hash: s.meta.Hash
		SnapShotTerm: s.snapshot.Term
		SnapshotUpdates: s.snapshot.Updates
		SnapshotSize: s.snapshot.Size
	}
}

func (s *Storage) Recover(meta *protocol.Meta) error {
	return nil
}

func (s *Storage) metaLog(meta *protocol.Meta) error {

}
