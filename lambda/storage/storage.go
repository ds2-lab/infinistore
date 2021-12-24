package storage

import (
	"errors"
	"fmt"
	"regexp"
	"sort"

	// "strings"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/redeo/resp"
	"github.com/zhangjyr/hashmap"

	"github.com/mason-leap-lab/infinicache/lambda/types"
)

const (
	StorageOverhead = 100000000 // 100 MB
)

var (
	FunctionPrefix        string
	FunctionPrefixMatcher = regexp.MustCompile(`\d+$`)

	ContextKeyLog = "log"

	ErrOOStorage = errors.New("out of storage")
)

type StorageHelper interface {
	get(string) (*types.Chunk, bool)
	getWithOption(string, *types.OpWrapper) (*types.Chunk, *types.OpRet)
	set(string, *types.Chunk)
	setWithOption(string, *types.Chunk, *types.OpWrapper) *types.OpRet
	del(*types.Chunk, string)
	delWithOption(*types.Chunk, string, *types.OpWrapper) *types.OpRet
	newChunk(string, string, uint64, []byte) *types.Chunk
	validate(*types.Chunk, *types.OpWrapper) (*types.OpWrapper, bool)
}

// Storage with lineage
type Storage struct {
	// IMOC repository, operations are supposed to be serialized.
	// NOTE: If serialization of operations can not be guarenteed, reconsider the implementation
	//       of "repo" and "mu"
	id     uint64
	repo   *hashmap.HashMap
	log    logger.ILogger
	helper StorageHelper
	meta   StorageMeta
}

func NewStorage(id uint64, cap uint64) *Storage {
	if FunctionPrefix == "" {
		FunctionPrefix = string(FunctionPrefixMatcher.ReplaceAll([]byte(lambdacontext.FunctionName), []byte("")))
	}
	store := &Storage{
		id:   id,
		repo: hashmap.New(10000),
		log:  &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Storage:"},
		meta: StorageMeta{Cap: cap, Overhead: StorageOverhead},
	}
	store.helper = store
	return store
}

func (s *Storage) Id() uint64 {
	return s.id
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

func (s *Storage) getWithOption(key string, opt *types.OpWrapper) (*types.Chunk, *types.OpRet) {
	chunk, ok := s.helper.get(key)
	if !ok {
		// No entry
		return nil, types.OpError(types.ErrNotFound)
	}

	if opt == nil || !opt.Accessed {
		chunk.Access()
	}
	if chunk.IsDeleted() {
		return nil, types.OpErrorWithMessage(types.ErrDeleted, chunk.Note)
	} else {
		// Ensure val is available regardless chunk is deleted or not.
		return chunk, types.OpSuccess()
	}
}

// Storage Implementation
func (s *Storage) Get(key string) (string, []byte, *types.OpRet) {
	chunk, ret := s.helper.getWithOption(key, nil)
	if ret.Error() != nil {
		return "", nil, ret
	}

	return chunk.Id, chunk.Body, types.OpSuccess()
}

func (s *Storage) GetStream(key string) (string, resp.AllReadCloser, *types.OpRet) {
	chunk, ret := s.helper.getWithOption(key, nil)
	if ret.Error() != nil {
		return "", nil, ret
	}

	return chunk.Id, resp.NewInlineReader(chunk.Body), types.OpSuccess()
}

func (s *Storage) set(key string, chunk *types.Chunk) {
	s.repo.Set(key, chunk)
}

func (s *Storage) setWithOption(key string, chunk *types.Chunk, opt *types.OpWrapper) *types.OpRet {
	ck, ok := s.repo.Get(key)
	s.helper.set(key, chunk)
	change := uint64(0)
	if ok {
		change -= ck.(*types.Chunk).Size
	}
	if opt == nil || !opt.Sized {
		change += chunk.Size
	}
	if change > 0 {
		s.meta.IncreaseSize(change)
	}

	return types.OpSuccess()
}

func (s *Storage) newChunk(key string, chunkId string, size uint64, val []byte) *types.Chunk {
	return types.NewChunk(key, chunkId, val)
}

func (s *Storage) validate(_ *types.Chunk, opt *types.OpWrapper) (*types.OpWrapper, bool) {
	return opt, true
}

// Set chunk
func (s *Storage) Set(key string, chunkId string, val []byte) *types.OpRet {
	chunk := s.helper.newChunk(key, chunkId, uint64(len(val)), val)
	return s.helper.setWithOption(key, chunk, nil)
}

// Set chunk using stream
func (s *Storage) SetStream(key string, chunkId string, valReader resp.AllReadCloser) *types.OpRet {
	val, err := valReader.ReadAll()
	if err != nil {
		return types.OpError(fmt.Errorf("error on read stream: %v", err))
	}

	return s.Set(key, chunkId, val)
}

func (s *Storage) del(chunk *types.Chunk, reason string) {
	chunk.Delete(reason)
}

func (s *Storage) delWithOption(chunk *types.Chunk, reason string, opt *types.OpWrapper) *types.OpRet {
	if opt == nil || !opt.Accessed {
		chunk.Access()
	}
	s.helper.del(chunk, reason)
	if opt == nil || !opt.Sized {
		s.meta.DecreaseSize(chunk.Size)
	}
	return types.OpSuccess()
}

func (s *Storage) Del(key string, reason string) *types.OpRet {
	chunk, ok := s.helper.get(key)
	if !ok {
		return types.OpError(types.ErrNotFound)
	}

	return s.helper.delWithOption(chunk, reason, nil)
}

func (s *Storage) Len() int {
	return s.repo.Len()
}

func (s *Storage) Keys() <-chan string {
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

func (s *Storage) Meta() types.StorageMeta {
	return &s.meta
}
