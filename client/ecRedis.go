package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ScottMansfield/nanolog"
	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/redeo/resp"
)

var (
	log = &logger.ColorLogger{
		Prefix: "EcRedis ",
		Level:  logger.LOG_LEVEL_INFO,
		Color:  true,
	}
	ErrUnexpectedResponse = errors.New("Unexpected response")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func NewRequestWriter(wr io.Writer) *resp.RequestWriter {
	return resp.NewRequestWriter(wr)
}
func NewResponseReader(rd io.Reader) resp.ResponseReader {
	return resp.NewResponseReader(rd)
}

// New set API
// Internal error if result is false.
func (c *Client) Set(key string, val []byte) bool {
	_, ok := c.EcSet(key, val)
	return ok
}

// Internal API
func (c *Client) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	// Debuging options
	var dryrun int
	var placements []int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if len(args) > 1 {
		p, ok := args[1].([]int)
		if ok && len(p) >= c.Shards {
			placements = p
		}
	}

	stats := &c.Data
	stats.Begin = time.Now()
	stats.ReqId = uuid.New().String()

	// randomly generate destiny lambda store id
	numClusters := MaxLambdaStores
	if dryrun > 0 {
		numClusters = dryrun
	}
	index := random(numClusters, c.Shards)
	if dryrun > 0 && placements != nil {
		for i, ret := range index {
			placements[i] = ret
		}
		return stats.ReqId, true
	}
	//addr, ok := c.getHost(key)
	//fmt.Println("in SET, key is: ", key)
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	// log.Debug("ring LocateKey costs: %v", time.Since(stats.Begin))
	// log.Debug("SET located host: %s", host)

	shards, err := c.encode(val)
	if err != nil {
		log.Warn("EcSet failed to encode: %v", err)
		return stats.ReqId, false
	}

	var wg sync.WaitGroup
	ret := newEcRet(c.Shards)
	for i := 0; i < ret.Len(); i++ {
		//fmt.Println("shards", i, "is", shards[i])
		wg.Add(1)
		go c.set(host, key, stats.ReqId, len(val), i, shards[i], index[i], ret, &wg)
	}
	wg.Wait()
	stats.ReqLatency = time.Since(stats.Begin)
	stats.Duration = stats.ReqLatency

	if ret.Err != nil {
		return stats.ReqId, false
	}

	nanolog.Log(LogClient, "set", stats.ReqId, stats.Begin.UnixNano(),
		int64(stats.Duration), int64(stats.ReqLatency), int64(0), int64(0),
		false, false)
	log.Info("Set %s %d", key, int64(stats.Duration))

	if placements != nil {
		for i, ret := range ret.Rets {
			placements[i], _ = strconv.Atoi(string(ret.([]byte)))
		}
	}
	fmt.Println("placements is", index)

	return stats.ReqId, true
}

// New get API, not size is required.
// Internal error if the bool is set to false
func (c *Client) Get(key string) (io.ReadCloser, bool) {
	_, reader, ok := c.EcGet(key, 0)
	return reader, ok
}

// Internal API
// returns reqId, reader, and a bool indicate error. If not found, the reader will be nil.
func (c *Client) EcGet(key string, args ...interface{}) (string, ReadAllCloser, bool) {
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}

	stats := &c.Data
	stats.Begin = time.Now()
	stats.ReqId = uuid.New().String()
	if dryrun > 0 {
		return stats.ReqId, nil, true
	}

	//addr, ok := c.getHost(key)
	member := c.Ring.LocateKey([]byte(key))
	host := member.String()
	//fmt.Println("ring LocateKey costs:", time.Since(t))
	//fmt.Println("GET located host: ", host)

	// Send request and wait
	var wg sync.WaitGroup
	ret := newEcRet(c.Shards)
	for i := 0; i < ret.Len(); i++ {
		wg.Add(1)
		go c.get(host, key, stats.ReqId, i, ret, &wg)
	}
	wg.Wait()
	stats.RecLatency = time.Since(stats.Begin)

	// Nil check
	if ret.Size == 0 {
		return stats.ReqId, nil, true // Set true to differentiate not found and error.
	}

	// Filter results
	chunks := make([][]byte, ret.Len())
	failed := make([]int, 0, ret.Len())
	for i, _ := range ret.Rets {
		err := ret.Error(i)
		if err != nil {
			failed = append(failed, i)
		} else {
			chunks[i] = ret.Ret(i)
		}
	}

	decodeStart := time.Now()
	reader, err := c.decode(stats, chunks, ret.Size)
	if err != nil {
		return stats.ReqId, nil, false
	}

	end := time.Now()
	stats.Duration = end.Sub(stats.Begin)
	nanolog.Log(LogClient, "get", stats.ReqId, stats.Begin.UnixNano(),
		int64(stats.Duration), int64(0), int64(stats.RecLatency), int64(end.Sub(decodeStart)),
		stats.AllGood, stats.Corrupted)
	log.Info("Got %s %d ( %d %d )", key, int64(stats.Duration), int64(stats.RecLatency), int64(end.Sub(decodeStart)))

	// Try recover
	if len(failed) > 0 {
		c.recover(host, key, uuid.New().String(), ret.Size, failed, chunks)
	}

	return stats.ReqId, reader, true
}

func (c *Client) getHost(key string) (addr string, ok bool) {
	// linear search through all filters and locate the one that holds the key
	for addr, filter := range c.MappingTable {
		found := filter.Lookup([]byte(key))
		if found { // if found, return the address
			return addr, true
		}
	}
	// otherwise, return nil
	return "", false
}

// random will generate random sequence within the lambda stores
// index and get top n id
func random(cluster, n int) []int {
	return rand.Perm(cluster)[:n]
}

func (c *Client) setError(ret *ecRet, addr string, i int, err error) {
	if err == io.EOF {
		c.disconnect(addr, i)
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		c.disconnect(addr, i)
	}
	ret.SetError(i, err)
}

func (c *Client) set(addr string, key string, reqId string, size int, i int, val []byte, lambdaId int, ret *ecRet, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if err := c.validate(addr, i); err != nil {
		c.setError(ret, addr, i, err)
		log.Warn("Failed to validate connection %d@%s(%s): %v", i, key, addr, err)
		return
	}
	cn := c.Conns[addr][i]
	cn.conn.SetWriteDeadline(time.Now().Add(Timeout)) // Set deadline for request
	defer cn.conn.SetWriteDeadline(time.Time{})

	w := cn.W
	w.WriteMultiBulkSize(10)
	w.WriteBulkString(protocol.CMD_SET_CHUNK)
	w.WriteBulkString(key)
	w.WriteBulkString(reqId)
	w.WriteBulkString(strconv.Itoa(size))
	w.WriteBulkString(strconv.Itoa(i))
	w.WriteBulkString(strconv.Itoa(c.DataShards))
	w.WriteBulkString(strconv.Itoa(c.ParityShards))
	w.WriteBulkString(strconv.Itoa(lambdaId))
	w.WriteBulkString(strconv.Itoa(MaxLambdaStores))

	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := w.CopyBulk(bytes.NewReader(val), int64(len(val))); err != nil {
		c.setError(ret, addr, i, err)
		log.Warn("Failed to initiate setting %d@%s(%s): %v", i, key, addr, err)
		return
	}
	if err := w.Flush(); err != nil {
		c.setError(ret, addr, i, err)
		log.Warn("Failed to initiate setting %d@%s(%s): %v", i, key, addr, err)
		return
	}
	cn.conn.SetWriteDeadline(time.Time{})

	log.Debug("Initiated setting %d@%s(%s)", i, key, addr)
	c.recvSet("Set", addr, reqId, i, ret, nil)
}

func (c *Client) get(addr string, key string, reqId string, i int, ret *ecRet, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if err := c.validate(addr, i); err != nil {
		c.setError(ret, addr, i, err)
		log.Warn("Failed to validate connection %d@%s(%s): %v", i, key, addr, err)
		return
	}
	cn := c.Conns[addr][i]
	cn.conn.SetWriteDeadline(time.Now().Add(Timeout)) // Set deadline for request
	defer cn.conn.SetWriteDeadline(time.Time{})

	// tGet := time.Now()
	// fmt.Println("Client send GET req timeStamp", tGet, "chunkId is", i)
	// cmd key reqId chunkId
	cn.W.WriteCmdString(protocol.CMD_GET_CHUNK, key, reqId, strconv.Itoa(i))

	// Flush pipeline
	//if err := c.W[i].Flush(); err != nil {
	if err := cn.W.Flush(); err != nil {
		c.setError(ret, addr, i, err)
		log.Warn("Failed to initiate getting %d@%s(%s): %v", i, key, addr, err)
		return
	}
	cn.conn.SetWriteDeadline(time.Time{})

	log.Debug("Initiated getting %d@%s(%s)", i, key, addr)
	c.recvGet("Got", addr, reqId, i, ret, nil)
}

func (c *Client) recvSet(prompt string, addr string, reqId string, i int, ret *ecRet, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	cn := c.Conns[addr][i]
	cn.conn.SetReadDeadline(time.Now().Add(Timeout)) // Set deadline for response
	defer cn.conn.SetReadDeadline(time.Time{})

	// peeking response type and receive
	// chunk id
	type0, err := cn.R.PeekType()
	if err != nil {
		log.Warn("PeekType error on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	}

	// Check error
	switch type0 {
	case resp.TypeError:
		strErr, err := c.Conns[addr][i].R.ReadError()
		if err == nil {
			err = errors.New(strErr)
		}
		log.Warn("Error on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	}

	// Read fields
	respId, _ := c.Conns[addr][i].R.ReadBulkString()
	chunkId, _ := c.Conns[addr][i].R.ReadBulkString()
	storeId, _ := c.Conns[addr][i].R.ReadBulkString()

	// Match reqId and chunk
	if respId != reqId || chunkId != strconv.Itoa(i) {
		log.Warn("Unexpected response %s(%s), expects %s(%d)", respId, chunkId, reqId, i)
		ret.SetError(i, ErrUnexpectedResponse)
		return
	}

	log.Debug("%s chunk %d", prompt, i)
	ret.Set(i, storeId)
}

func (c *Client) recvGet(prompt string, addr string, reqId string, i int, ret *ecRet, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	cn := c.Conns[addr][i]
	cn.conn.SetReadDeadline(time.Now().Add(Timeout))  // Set deadline for response
	defer cn.conn.SetReadDeadline(time.Time{})

	// peeking response type and receive
	// chunk id
	type0, err := cn.R.PeekType()
	if err != nil {
		log.Warn("PeekType error on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	}

	// Check error
	switch type0 {
	case resp.TypeError:
		strErr, err := c.Conns[addr][i].R.ReadError()
		if err == nil {
			err = errors.New(strErr)
		}
		log.Warn("Error on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	case resp.TypeNil:
		err := c.Conns[addr][i].R.ReadNil()
		if err != nil {
			log.Warn("Error on receiving chunk %d: %v", i, err)
			c.setError(ret, addr, i, err)
			return
		}
		log.Debug("Not found chunk %d", prompt, i)
		ret.Set(i, nil)
		return
	}

	// Read header fields
	respId, _ := c.Conns[addr][i].R.ReadBulkString()
	strSize, _ := c.Conns[addr][i].R.ReadBulkString()
	chunkId, _ := c.Conns[addr][i].R.ReadBulkString()

	// Matching reqId and chunk
	if respId != reqId || (chunkId != strconv.Itoa(i) && chunkId != "-1") {
		log.Warn("Unexpected response %s(%s), expects %s(%d)", respId, chunkId, reqId, i)
		ret.SetError(i, ErrUnexpectedResponse)
		return
	}

	// Abandon?
	if chunkId == "-1" {
		log.Debug("Abandon late chunk %d", i)
		return
	}

	// Read value
	valReader, err := c.Conns[addr][i].R.StreamBulk()
	if err != nil {
		log.Warn("Error on get value reader on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	}
	val, err := valReader.ReadAll()
	if err != nil {
		log.Error("Error on get value on receiving chunk %d: %v", i, err)
		c.setError(ret, addr, i, err)
		return
	}

	if ret.Size == 0 {
		ret.Size, _ = strconv.Atoi(strSize)   // If err, we can try in another chunk
	}

	log.Debug("%s chunk %d", prompt, i)
	ret.Set(i, val)
}

func (c *Client) recover(addr string, key string, reqId string, size int, failed []int, shards [][]byte) {
	var wg sync.WaitGroup
	ret := newEcRet(c.Shards)
	for _, i := range failed {
		wg.Add(1)
		// lambdaId = 0, for lambdaID of a specified key is fixed on setting.
		go c.set(addr, key, reqId, size, i, shards[i], 0, ret, &wg)
	}
	wg.Wait()

	if ret.Err != nil {
		log.Warn("Failed to recover shards of %s: %v", key, failed)
	} else {
		log.Info("Succeeded to recover shards of %s: %v", key, failed)
	}
}

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
	if ok == false {
		log.Warn("Failed to verify encoding: %v", err)
		return nil, err
	}
	log.Debug("Encoding succeeded.")
	return shards, err
}

func (c *Client) decode(stats *DataEntry, data [][]byte, size int) (ReadAllCloser, error) {
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
		err := c.EC.Reconstruct(data)
		if err != nil {
			log.Warn("Reconstruction failed: %v", err)
			return nil, err
		}
		stats.Corrupted, err = c.EC.Verify(data)
		if !stats.Corrupted {
			log.Warn("Verification failed after reconstruction, data could be corrupted: %v", err)
			return nil, err
		} else {
			log.Debug("Reconstructed")
		}
	}

	return NewJoinReader(data, size, c.EC.Join), nil
}

type Joiner func(io.Writer, [][]byte, int) error

type JoinReader struct {
	io.ReadCloser
	writer io.Writer
	data [][]byte
	read int
	size int
	once sync.Once
	joiner Joiner
}

func NewJoinReader(data [][]byte, size int, joiner Joiner) *JoinReader {
	reader, writer := io.Pipe()
	return &JoinReader{
		ReadCloser: reader,
		writer: writer,
		data: data,
		size: size,
		joiner: joiner,
	}
}

func (r *JoinReader) Read(p []byte) (n int, err error) {
	r.once.Do(r.join)
	n, err = r.ReadCloser.Read(p)
	r.read += n
	return
}

func (r *JoinReader) Len() int {
	return r.size - r.read
}

func (r *JoinReader) ReadAll() (buf []byte, err error) {
	buf = make([]byte, r.Len())
	n, err := io.ReadFull(r.ReadCloser, buf)
	r.read += n
	r.ReadCloser.Close()
	return
}

func (r *JoinReader) join() {
	go r.joiner(r.writer, r.data, r.size)
}
