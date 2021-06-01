package storage

import (
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

var (
	FunctionPrefix        string
	FunctionPrefixMatcher = regexp.MustCompile(`\d+$`)

	ContextKeyLog = "log"
)

type StorageHelper interface {
	get(string) (*types.Chunk, bool)
	getWithOption(string, *types.OpWrapper) (string, []byte, *types.OpRet)
	set(string, *types.Chunk)
	setWithOption(string, string, []byte, *types.OpWrapper) *types.OpRet
	newChunk(string, string, uint64, []byte) *types.Chunk
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
}

func NewStorage(id uint64) *Storage {
	if FunctionPrefix == "" {
		FunctionPrefix = string(FunctionPrefixMatcher.ReplaceAll([]byte(lambdacontext.FunctionName), []byte("")))
	}
	store := &Storage{
		id:   id,
		repo: hashmap.New(10000),
		log:  &logger.ColorLogger{Level: logger.LOG_LEVEL_INFO, Color: false, Prefix: "Storage:"},
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

func (s *Storage) getWithOption(key string, opt *types.OpWrapper) (string, []byte, *types.OpRet) {
	chunk, ok := s.helper.get(key)
	if !ok {
		// No entry
		return "", nil, types.OpError(types.ErrNotFound)
	} else {
		return chunk.Id, chunk.Access(), types.OpSuccess()
	}
}

// Storage Implementation
func (s *Storage) Get(key string) (string, []byte, *types.OpRet) {
	return s.helper.getWithOption(key, nil)
}

func (s *Storage) GetStream(key string) (string, resp.AllReadCloser, *types.OpRet) {
	chunkId, val, ret := s.helper.getWithOption(key, nil)
	if ret.Error() != nil {
		return chunkId, nil, ret
	}

	return chunkId, resp.NewInlineReader(val), types.OpSuccess()
}

func (s *Storage) set(key string, chunk *types.Chunk) {
	s.repo.Set(key, chunk)
}

func (s *Storage) setWithOption(key string, chunkId string, val []byte, opt *types.OpWrapper) *types.OpRet {
	chunk := s.helper.newChunk(key, chunkId, uint64(len(val)), val)
	s.helper.set(key, chunk)
	return types.OpSuccess()
}

func (s *Storage) newChunk(key string, chunkId string, size uint64, val []byte) *types.Chunk {
	return types.NewChunk(key, chunkId, val)
}

// Set chunk
func (s *Storage) Set(key string, chunkId string, val []byte) *types.OpRet {
	return s.helper.setWithOption(key, chunkId, val, nil)
}

// Set chunk using stream
func (s *Storage) SetStream(key string, chunkId string, valReader resp.AllReadCloser) *types.OpRet {
	val, err := valReader.ReadAll()
	if err != nil {
		return types.OpError(fmt.Errorf("error on read stream: %v", err))
	}

	return s.helper.setWithOption(key, chunkId, val, nil)
}

func (s *Storage) Del(key string, chunkId string) *types.OpRet {
	_, ok := s.helper.get(key)
	if !ok {
		return types.OpError(types.ErrNotFound)
	}

	return types.OpSuccess()
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
