package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/go-utils"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/redeo/client"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo/resp"
)

const (
	LargeObjectThreshold = 30000000 // 20 MB per chunk
	LargeObjectSplitUnit = 10000000 // 10 MB per chunk
)

var (
	// ErrUnexpectedResponse Unexplected response
	ErrUnexpectedResponse      = errors.New("unexpected response")
	ErrUnexpectedPreflightPong = errors.New("unexpected preflight pong")
	ErrMaxPreflightsReached    = errors.New("max preflight attempts reached")
	ErrAbandonRequest          = errors.New("abandon request")
	ErrKeyNotFound             = errors.New("key not found")
	ErrEmptyChunk              = errors.New("empty chunk")
	ErrUnknown                 = errors.New("unknown error")
	RequestAttempts            = 3

	OccupantReadAllCloser = &JoinReader{}
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type hasher struct {
	util.Hasher
	partitionCount uint64
}

func (h *hasher) PartitionID(key []byte) int {
	hkey := h.Sum64(key)
	return int(hkey % h.partitionCount)
}

// Set New set API
// Internal error if result is false.
func (c *Client) Set(key string, val []byte) bool {
	_, err := c.EcSet(key, val)
	return err == nil
}

// EcSet Internal API
func (c *Client) EcSet(key string, val []byte, args ...interface{}) (string, error) {
	// Debuging options
	var dryrun int
	var placements []int
	reset := false
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if len(args) > 1 {
		p, ok := args[1].([]int)
		if ok && len(p) >= c.Shards {
			placements = p
		}
	}
	if len(args) > 2 {
		reset = args[2] == "Reset"
	}

	reqId := uuid.New().String()

	// randomly generate destiny lambda store id
	numClusters := MaxLambdaStores
	if dryrun > 0 {
		numClusters = dryrun
	}
	index := random(numClusters, c.Shards)
	if dryrun > 0 && placements != nil {
		if !reset {
			copy(placements, index)
		}
		return reqId, nil
	}

	stats := &c.logEntry
	stats.Begin(reqId)

	//addr, ok := c.getHost(key)
	//fmt.Println("in SET, key is: ", key)
	member := c.Ring.GetPartitionOwner(Hasher.PartitionID([]byte(key)))
	host := member.String()
	// log.Debug("ring LocateKey costs: %v", time.Since(stats.Begin))
	// log.Debug("SET located host: %s", host)

	var ret *ecRet
	if len(val) <= LargeObjectThreshold*len(index) {
		ret = c.set(host, key, reqId, val, index)
	} else {
		ret = c.setLarge(host, key, reqId, val, index)
	}
	stats.ReqLatency = stats.Since()
	stats.Duration = stats.ReqLatency

	if ret == nil {
		log.Warn("Failed to set %s,%s: %v", key, reqId, ErrUnknown)
		return reqId, ErrClient
	} else if ret.Err != nil {
		ret.PrintErrors("Failed to set %s,%s", key, reqId)
		return reqId, ErrClient
	}

	nanoLog(logClient, "set", stats.ReqId, stats.Start.UnixNano(),
		int64(stats.Duration), int64(stats.ReqLatency), int64(0), int64(0),
		false, false, len(val))
	log.Info("Set %s %d %d", key, len(val), int64(stats.Duration))

	if placements != nil {
		for i := 0; i < ret.Len(); i++ {
			placements[i], _ = strconv.Atoi(ret.RetStore(i))
		}
	}

	return stats.ReqId, nil
}

// Get New get API. No size is required.
// Internal error if the bool is set to false
func (c *Client) Get(key string) (ReadAllCloser, bool) {
	_, reader, err := c.EcGet(key, 0)
	return reader, err != nil
}

// EcGet Internal API
// returns reqId, reader, and a bool indicate error. If not found, the reader will be nil.
func (c *Client) EcGet(key string, args ...interface{}) (string, ReadAllCloser, error) {
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}

	reqId := uuid.New().String()

	if dryrun > 0 {
		return reqId, nil, nil
	}

	//addr, ok := c.getHost(key)
	member := c.Ring.GetPartitionOwner(Hasher.PartitionID([]byte(key)))
	host := member.String()
	//fmt.Println("ring LocateKey costs:", time.Since(t))
	//fmt.Println("GET located host: ", host)

	reader, allRets := c.get(host, key, reqId)
	ret := allRets[0]
	if ret.Err != nil {
		ret.PrintErrors("Failed to get %s,%s", key, reqId)
		return reqId, nil, utils.Ifelse(ret.Err == ErrNotFound, ret.Err, ErrClient).(error)
	}

	nanoLog(logClient, "get", reqId, ret.Stats.Start.UnixNano(),
		int64(ret.Stats.Duration), int64(0), int64(ret.Stats.RecLatency), int64(ret.Stats.CodingLatency),
		ret.Stats.AllGood, ret.Stats.Corrupted, ret.Meta.Size)
	log.Info("Got %s %d %d ( %d %d )", key, ret.Meta.Size, int64(ret.Stats.Duration), int64(ret.Stats.RecLatency), int64(ret.Stats.CodingLatency))

	return reqId, reader, nil
}

func (c *Client) ReadResponse(req client.Request) error {
	cliReq := req.(*ClientRequest)
	switch cliReq.Cmd {
	case protocol.CMD_SET_CHUNK:
		return c.readSetResponse(cliReq)
	case protocol.CMD_GET_CHUNK:
		return c.readGetResponse(cliReq)
	default:
		return ErrUnexpectedResponse
	}
}

func (c *Client) readErrorResponse(req *ClientRequest) (error, error) {
	cn := req.Conn()
	respType, err := cn.PeekType()
	if err != nil {
		return nil, err
	}

	switch respType {
	case resp.TypeError:
		strErr, err := cn.ReadError()
		if err != nil {
			return nil, err
		}
		return errors.New(strErr), nil
	case resp.TypeNil:
		err := cn.ReadNil()
		if err != nil {
			return nil, err
		}
		return ErrKeyNotFound, nil
	}

	return nil, nil
}

// random will generate random sequence within the lambda stores
// index and get top n id
func random(cluster, n int) []int {
	return rand.Perm(cluster)[:n]
}

func (c *Client) set(host string, key string, reqId string, val []byte, placements []int) *ecRet {
	shards, err := c.encode(val)
	if err != nil {
		log.Warn("EcSet failed to encode: %v", err)
		return nil
	}

	ret := newEcRet(c.Shards)
	for i := 0; i < ret.Len(); i++ {
		ret.Add(1)
		go c.sendSet(host, key, reqId, strconv.Itoa(len(val)), i, shards[i], placements[i], ret)
	}
	ret.Wait()

	return ret
}

func (c *Client) setLarge(host string, key string, reqId string, val []byte, placements []int) *ecRet {
	numFrags := int(math.Round(float64(len(val)) / LargeObjectSplitUnit / float64(len(placements))))
	fragments, _ := NewEncoder(numFrags, 0, 0).Split(val)
	shardsSet := make([][][]byte, numFrags)
	notifiers := make([]WaitGroup, numFrags)
	for i := 0; i < numFrags; i++ {
		notifiers[i] = &sync.WaitGroup{}
		notifiers[i].Add(1)
	}
	go c.encodeFragments(fragments, shardsSet, notifiers)

	allRets := make([]*ecRet, numFrags)
	for i := 0; i < len(allRets); i++ {
		ret := newEcRet(c.Shards)
		ret.Add(ret.Len())
		allRets[i] = ret
	}
	ret := allRets[0]
	strSize := fmt.Sprintf("%d-%d", len(val), numFrags)
	for i := 0; i < ret.Len(); i++ {
		go func(i int) {
			var lastError error
			j := 0
			for k, rid := key, reqId; j < numFrags; {
				// Wait for fragments
				notifiers[j].Wait()
				// TODO: sendSet set total size
				// Use non-postfixed key and reqId in first iteration for backward compatibility
				// and dynamic fragments detection in Get API
				c.sendSet(host, k, rid, strSize, i, shardsSet[j][i], placements[i], allRets[j])
				j++
				// Abort reset fragments on any error.
				if allRets[j-1].Err != nil {
					lastError = ErrAbandonRequest
					break
				}
				k, rid = fmt.Sprintf("%s-%d", key, j), fmt.Sprintf("%s-%d", reqId, j)
			}
			// Abandon rest on error
			for ; j < numFrags; j++ {
				allRets[j].Err = lastError
				allRets[j].Done()
			}
		}(i)
	}
	// Wait for all rets, so next request can use existed connection.
	ret.Wait()
	i := 1
	for ; ret.Err == nil && i < len(allRets); i++ {
		allRets[i].Wait()
		ret.Err = allRets[i].Err
	}
	for ; i < len(allRets); i++ {
		allRets[i].Wait()
	}

	// If any err, ret.Err will be error.
	// If success, only placements of first fragment are returned.
	return ret
}

func (c *Client) encodeFragments(fragments [][]byte, shardsSet [][][]byte, notifiers []WaitGroup) {
	for i, fragment := range fragments {
		shardsSet[i], _ = c.encode(fragment)
		notifiers[i].Done()
	}
}

func (c *Client) sendSet(addr string, key string, reqId string, size string, i int, val []byte, lambdaId int, ret *ecRet) {
	req := ret.Request(i)
	if req == nil {
		// Ret abandoned
		return
	}
	req.Cmd = protocol.CMD_SET_CHUNK
	req.ReqId = reqId

	for attempt := 0; attempt < RequestAttempts; attempt++ {
		if attempt > 0 {
			log.Info("Retry setting %d@%s(%s), %s, attempt %d", i, key, addr, reqId, attempt+1)
		}

		cn, err := c.validate(addr, i)
		if err != nil {
			req.SetResponse(fmt.Errorf("error on validating connection(%s): %v", addr, err))
			return
		}

		req.SetConn(cn)
		err = cn.StartRequest(req, func(_ client.Request) error {
			cn.SetWriteDeadline(time.Now().Add(HeaderTimeout)) // Set deadline for request
			defer cn.SetWriteDeadline(time.Time{})             // One defered reset is enough.

			cn.WriteMultiBulkSize(11)
			cn.WriteBulkString(req.Cmd)
			cn.WriteBulkString(strconv.FormatInt(req.Seq(), 10))
			cn.WriteBulkString(key)
			cn.WriteBulkString(req.ReqId)
			cn.WriteBulkString(size)
			cn.WriteBulkString(strconv.Itoa(i))
			cn.WriteBulkString(strconv.Itoa(c.DataShards))
			cn.WriteBulkString(strconv.Itoa(c.ParityShards))
			cn.WriteBulkString(strconv.Itoa(lambdaId))
			cn.WriteBulkString(strconv.Itoa(MaxLambdaStores))
			if err := cn.Flush(); err != nil {
				log.Warn("Failed to flush headers of setting %d@%s(%s): %v", i, key, addr, err)
				return err
			}

			// Flush pipeline
			//if err := c.W[i].Flush(); err != nil {
			cn.SetWriteDeadline(time.Now().Add(Timeout))
			if err := cn.CopyBulk(bytes.NewReader(val), int64(len(val))); err != nil {
				log.Warn("Failed to stream body of setting %d@%s(%s): %v", i, key, addr, err)
				return err
			}
			return nil
		})
		if err != nil && c.closed {
			req.SetResponse(ErrClientClosed)
			return
		} else if err != nil {
			continue
		}

		log.Debug("Initiated setting %d@%s(%s), attempt %d", i, key, addr, attempt+1)
		ctx, cancel := context.WithTimeout(req.Context(), Timeout)
		req.Cancel = cancel
		req.SetContext(ctx)
		return
	}

	req.SetResponse(fmt.Errorf("stop attempts: %v", ErrMaxPreflightsReached))
}

func (c *Client) readSetResponse(req *ClientRequest) error {
	// TODO: Add read timeout
	// Set deadline for response. Noted writing can be buffered, we use a long timeout to wait for long enough for transmission.
	// cn.conn.SetReadDeadline(time.Now().Add(Timeout))
	// defer cn.conn.SetReadDeadline(time.Time{})

	cn := req.Conn()
	cm := cn.Meta.(*ClientConnMeta)

	// Read header fields
	cn.SetReadDeadline(time.Now().Add(HeaderTimeout))
	appErr, err := c.readErrorResponse(req)
	if err != nil {
		req.SetResponse(err)
		return err
	} else if appErr != nil {
		req.SetResponse(appErr)
		return nil
	}

	respId, _ := cn.ReadBulkString()
	chunkId, _ := cn.ReadBulkString()
	storeId, err := cn.ReadBulkString()
	if err != nil {
		req.SetResponse(fmt.Errorf("error on reading header: %v", err))
		return err
	}

	// Match reqId and chunk
	if respId != req.ReqId || chunkId != strconv.Itoa(cm.AddrIdx) {
		log.Warn("Unexpected response %s(%s), expects %s(%d)", logger.SafeString(respId, len(req.ReqId)), logger.SafeString(chunkId, 2), req.ReqId, cm.AddrIdx)
		req.SetResponse(ErrUnexpectedResponse)
		return nil
	}

	log.Debug("Set chunk %s(%d)", req.ReqId, cm.AddrIdx)
	req.SetResponse(storeId)
	return nil
}

// TODO, read first, return total Size, and request more if neccessary
func (c *Client) get(host string, key string, reqId string) (ReadAllCloser, []*ecRet) {
	// Send request and wait
	ret := newEcRet(c.Shards)
	ret.Stats = &c.logEntry
	ret.Stats.Begin(reqId)
	ret.Stats.ReqLatency = 0
	for i := 0; i < ret.Len(); i++ {
		ret.Add(1)
		go c.sendGet(host, key, reqId, i, ret)
	}
	ret.Wait()

	if ret.Err != nil && ret.NumOK() < c.DataShards {
		return nil, []*ecRet{ret}
	} else {
		ret.Err = nil
	}

	var err error
	sizeFragments := strings.Split(ret.Meta.Raw, "-")
	ret.Meta.Size, err = strconv.Atoi(sizeFragments[0])
	if err != nil {
		ret.Err = ErrInvalidSize
		return nil, []*ecRet{ret}
	} else if ret.Meta.Size == 0 {
		ret.Err = ErrNotFound
		return nil, []*ecRet{ret}
	}

	// Default to be in one piece if no info on the # of fragments.
	ret.Meta.NumFrags = 1
	if len(sizeFragments) > 1 {
		ret.Meta.NumFrags, _ = strconv.Atoi(sizeFragments[1])
	}
	switch ret.Meta.NumFrags {
	case 0:
		ret.Err = ErrInvalidSize
		return nil, []*ecRet{ret}
	case 1:
		reader, err := c.decodeFragment(ret, ret.Meta.Size)
		if err != nil {
			ret.Err = err
		}
		return reader, []*ecRet{ret}
	default:
		// Continue to process multi-part(large) object.
	}

	// For multi-part object, we need to read all fragments and merge them.
	allRets := make([]*ecRet, ret.Meta.NumFrags)
	allRets[0] = ret
	for i := 1; i < len(allRets); i++ {
		ret := newEcRet(c.Shards)
		ret.Stats = &logEntry{}
		ret.Stats.Begin(fmt.Sprintf("%s-%d", reqId, i))
		ret.Stats.ReqLatency = 0
		ret.Add(ret.Len())
		allRets[i] = ret
	}
	readers := make([]ReadAllCloser, ret.Meta.NumFrags)
	chanErr := make(chan error, 1)
	go c.decodeFragments(allRets, readers, chanErr)

	// Parallelly send all fragments of the same chunk id.
	for i := 0; i < ret.Len(); i++ {
		go func(i int) {
			for j := 1; j < ret.Meta.NumFrags; j++ {
				c.sendGet(host, fmt.Sprintf("%s-%d", key, j), fmt.Sprintf("%s-%d", reqId, j), i, allRets[j])
			}
		}(i)
	}

	err = <-chanErr
	if err != nil {
		ret.Err = err
		return nil, []*ecRet{ret}
	}

	ret.Stats.CodingLatency = 0
	ret.Stats.Duration = ret.Stats.Since()
	return NewJoinReader(readers, ret.Meta.Size), allRets
}

func (c *Client) decodeFragments(rets []*ecRet, readers []ReadAllCloser, chanErr chan<- error) {
	var err error
	fragSize := (rets[0].Meta.Size + rets[0].Meta.NumFrags - 1) / rets[0].Meta.NumFrags
	read := fragSize
	for i, ret := range rets {
		if read > rets[0].Meta.Size {
			fragSize = fragSize + rets[0].Meta.Size - read
		}

		// Keep a copy of original value for atomic operation
		reader := readers[i]
		ret.Wait()

		ret.Add(1)
		if reader != nil || !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&readers[i])), *(*unsafe.Pointer)(unsafe.Pointer(&reader)), unsafe.Pointer(OccupantReadAllCloser)) {
			// Other routing may have decoded the fragment
			ret.Done()
			continue
		}

		// Decode, return on error
		readers[i], err = c.decodeFragment(ret, fragSize)
		if err != nil {
			chanErr <- err
			return
		}

		read += fragSize
	}
	close(chanErr)
}

func (c *Client) decodeFragment(ret *ecRet, size int) (ReadAllCloser, error) {
	ret.Stats.RecLatency = ret.Stats.Since()

	if ret.Err != nil {
		return nil, ret.Err
	}

	// Filter results, either reading all shards or up to the number of data shards, or data reconstruction will fail.
	// e.g. if we have 4 data shards and 2 parity shards, following are valid cases:
	// 1. 4 shards
	// 2. 6 shards
	// Unexpectedly, 5 shards will fail.
	chunks := make([][]byte, ret.Len())
	tbRead := ret.NumOK()
	if tbRead < c.DataShards+c.ParityShards {
		tbRead = c.DataShards
	}
	read := 0
	for i := 0; i < ret.Len() && read < tbRead; i++ {
		chunks[i] = ret.RetChunk(i)
		if len(chunks[i]) > 0 {
			read++
		}
	}

	decodeStart := time.Now()
	reader, err := c.decode(ret.Stats, chunks, size)
	ret.Stats.CodingLatency = time.Since(decodeStart)
	ret.Stats.Duration = ret.Stats.Since()
	return reader, err
}

func (c *Client) sendGet(addr string, key string, reqId string, i int, ret *ecRet) {
	req := ret.Request(i)
	if req == nil {
		// Ret abandoned
		return
	}
	req.Cmd = protocol.CMD_GET_CHUNK
	req.ReqId = reqId

	for attempt := 0; attempt < RequestAttempts; attempt++ {
		if attempt > 0 {
			log.Info("Retry getting %d@%s(%s), %s, attempt %d", i, key, addr, reqId, attempt+1)
		}

		cn, err := c.validate(addr, i)
		if err != nil {
			req.SetResponse(fmt.Errorf("error on validating connection(%s): %v", addr, err))
			return
		}

		req.SetConn(cn)
		err = cn.StartRequest(req, func(_ client.Request) error {
			// cmd seq key reqId chunkId
			cn.WriteCmdString(req.Cmd, strconv.FormatInt(req.Seq(), 10), key, req.ReqId, strconv.Itoa(i))
			return nil
		})
		if err != nil && err == client.ErrResponded {
			// Already responded, may be first-d abandoned.
			return
		} else if err != nil && c.closed {
			req.SetResponse(ErrClientClosed)
			return
		} else if err != nil {
			log.Warn("Failed to initiate getting %d@%s(%s): %v, left attempts: %d", i, key, addr, err, RequestAttempts-attempt-1)
			continue
		}

		log.Debug("Initiated getting %d@%s(%s), attempt %d", i, key, addr, attempt+1)
		ctx, cancel := context.WithTimeout(req.Context(), Timeout)
		req.Cancel = cancel
		req.SetContext(ctx)
		return
	}

	req.SetResponse(fmt.Errorf("stop attempts: %v", ErrMaxPreflightsReached))
}

func (c *Client) readGetResponse(req *ClientRequest) error {
	cn := req.Conn()
	cm := cn.Meta.(*ClientConnMeta)

	// Read header fields
	cn.SetReadDeadline(time.Now().Add(HeaderTimeout))
	appErr, err := c.readErrorResponse(req)
	if err != nil {
		req.SetResponse(err)
		return err
	} else if appErr != nil {
		req.SetResponse(appErr)
		return nil
	}

	respId, _ := cn.ReadBulkString()
	meta, _ := cn.ReadBulkString()
	chunkId, err := cn.ReadBulkString()
	if err != nil {
		req.SetResponse(fmt.Errorf("error on reading header: %v", err))
		// Could acceptable for client first-D optimization.
		return err
	}

	// Matching chunk
	if respId != req.ReqId || (chunkId != strconv.Itoa(cm.AddrIdx) && chunkId != "-1") {
		log.Warn("Unexpected response %s(%s), expects %s(%d)", logger.SafeString(respId, len(req.ReqId)), logger.SafeString(chunkId, 2), req.ReqId, cm.AddrIdx)
		req.SetResponse(ErrUnexpectedResponse)
		// Skip body
		if err := cn.SkipBulk(); err != nil {
			return err
		} else {
			return nil
		}
	}

	// Abandon?
	if chunkId == "-1" {
		req.SetResponse(ErrAbandon)
		return nil
	}

	// Read value
	cn.SetReadDeadline(time.Now().Add(Timeout))
	valReader, err := cn.StreamBulk()
	if err != nil {
		req.SetResponse(fmt.Errorf("error on getting reader of received chunk: %v", err))
		return err
	}
	if valReader.Len() == 0 {
		req.SetResponse(fmt.Errorf("got empty chunk"))
		valReader.ReadAll()
		return ErrEmptyChunk
	}

	val, err := valReader.ReadAll()
	if err != nil {
		req.SetResponse(fmt.Errorf("error on streaming received chunk: %v", err))
		return err
	}

	ret := req.Context().Value(CtxKeyECRet).(*ecRet)
	if ret.Meta.Raw == "" {
		ret.Meta.Raw = meta
	}

	log.Debug("Got chunk %s(%d)", req.ReqId, cm.AddrIdx)
	req.SetResponse(val)
	return nil
}

// func (c *Client) recover(addr string, key string, reqId string, size int, failed []int, shards [][]byte) {
// 	var wg sync.WaitGroup
// 	ret := newEcRet(c.Shards)
// 	for _, i := range failed {
// 		wg.Add(1)
// 		// lambdaId = 0, for lambdaID of a specified key is fixed on setting.
// 		go c.set(addr, key, reqId, size, i, shards[i], 0, ret, &wg)
// 	}
// 	wg.Wait()

// 	if ret.Err != nil {
// 		log.Warn("Failed to recover shards of %s: %v", key, failed)
// 	} else {
// 		log.Info("Succeeded to recover shards of %s: %v", key, failed)
// 	}
// }

func (c *Client) encode(obj []byte) ([][]byte, error) {
	// split obj first
	shards, err := c.EC.Split(obj)
	if err != nil {
		log.Warn("Encoding split err: %v", err)
		return nil, err
	}
	// Encode parity
	err = c.EC.Encode(shards)
	if err != nil {
		log.Warn("Encoding encode err: %v", err)
		return nil, err
	}
	ok, err := c.EC.Verify(shards)
	if !ok {
		log.Warn("Failed to verify encoding: %v", err)
		return nil, err
	}
	log.Debug("Encoding succeeded.")
	return shards, err
}

func (c *Client) decode(stats *logEntry, data [][]byte, size int) (ReadAllCloser, error) {
	// var err error
	stats.AllGood, _ = c.EC.Verify(data)
	if stats.AllGood {
		log.Debug("No reconstruction needed.")
		// } else if err != nil {
		// 	stats.Corrupted = true
		// 	log.Debug("Verification error, impossible to reconstructing data: %v", err)
		// 	return nil, err
	} else {
		log.Debug("Verification failed. Reconstructing data...")
		if err := c.EC.Reconstruct(data); err != nil {
			// log.Warn("Reconstruction failed: %v", err)
			return nil, err
		}
		if good, err := c.EC.Verify(data); err != nil {
			return nil, err
		} else if !good {
			// log.Warn("Verification failed after reconstruction, data could be corrupted: %v", err)
			stats.Corrupted = true
			return nil, ErrCorrupted
		}

		log.Debug("Reconstructed")
	}

	return NewByteJoinReader(data, size, c.EC.Join), nil
}
