package client

import (
	"context"
	"errors"
	"fmt"
	sysnet "net"
	"sync/atomic"

	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"

	// cuckoo "github.com/seiflotfy/cuckoofilter"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/common/net"
	"github.com/mason-leap-lab/infinicache/common/redeo/client"
	"github.com/mason-leap-lab/infinicache/common/sync"
	"github.com/mason-leap-lab/infinicache/common/util"
)

var (
	log = &logger.ColorLogger{
		Prefix: "EcRedis ",
		Level:  logger.LOG_LEVEL_WARN,
		Color:  false,
	}
	Hasher   = &hasher{partitionCount: 271}
	ECConfig = consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            Hasher,
	}
	ErrNotFound     = errors.New("not found")
	ErrClient       = errors.New("client internal error")
	ErrDialShortcut = errors.New("failed to dial shortcut, check the RedisAdapter")
	ErrNoRequest    = errors.New("request not present")
	ErrClientClosed = errors.New("client closed")

	CtxKeyECRet = reqCtxKey("ecret")
)

type reqCtxKey string

// Client InfiniCache client
type Client struct {
	EC           reedsolomon.Encoder
	Ring         *consistent.Consistent
	DataShards   int
	ParityShards int
	Shards       int

	conns map[string][]*client.Conn
	// mappingTable map[string]*cuckoo.Filter
	logEntry logEntry
	shortcut *net.ShortcutConn
	closed   bool
}

// NewClient Create a client instance.
func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	return &Client{
		conns: make(map[string][]*client.Conn),
		EC:    NewEncoder(dataShards, parityShards, ecMaxGoroutine),
		// mappingTable: make(map[string]*cuckoo.Filter),
		DataShards:   dataShards,
		ParityShards: parityShards,
		Shards:       dataShards + parityShards,
	}
}

// Dial Dial proxies
func (c *Client) Dial(addrArr []string) bool {
	//t0 := time.Now()
	members := make([]consistent.Member, len(addrArr))
	for i, addr := range addrArr {
		log.Debug("Dialing %s...", addr)
		newaddr, err := c.initDial(addr)
		members[i] = clientMember(newaddr)
		if err != nil {
			log.Error("Fail to dial %s: %v", addr, err)
			c.Close()
			return false
		}
	}
	log.Debug("Creating consistent ring %v...", members)
	c.Ring = consistent.New(members, ECConfig)
	//time0 := time.Since(t0)
	//fmt.Println("Dial all goroutines are done!")
	//if err := nanolog.Log(LogClient, "Dial", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	return true
}

// Close Close the client
func (c *Client) Close() {
	c.closed = true
	// log.Debug("Cleaning up...")
	for addr, conns := range c.conns {
		for i, cn := range conns {
			if cn != nil {
				cn.Close()
			}
			c.conns[addr][i] = nil
		}
	}
	// log.Debug("Client closed.")
}

//func (c *Client) initDial(address string, wg *sync.WaitGroup) {
func (c *Client) initDial(address string) (string, error) {
	// initialize parallel connections under address
	connect := c.connect
	if _, ok := net.Shortcut.Validate(address); ok {
		connect = c.connectShortcut
	}
	// Connect use original address
	err := connect(address, c.Shards)
	return address, err
}

func (c *Client) connect(addr string, n int) error {
	c.conns[addr] = make([]*client.Conn, n)
	for i := 0; i < n; i++ {
		_, err := c.validate(addr, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) connectShortcut(addr string, n int) error {
	var ok bool
	c.shortcut, ok = net.Shortcut.GetConn(addr)
	if !ok {
		return ErrDialShortcut
	}
	c.conns[addr] = make([]*client.Conn, n)
	for i := 0; i < n; i++ {
		c.validate(addr, i)
	}
	return nil
}

func (c *Client) validate(address string, i int) (cn *client.Conn, err error) {
	if c.conns[address][i] == nil || c.conns[address][i].IsClosed() {
		var conn sysnet.Conn
		if c.shortcut == nil || c.shortcut.Address != address {
			conn, err = sysnet.Dial("tcp", address)
			if err == nil {
				c.conns[address][i] = client.NewConn(conn, func(cn *client.Conn) {
					cn.Meta = &ClientConnMeta{Addr: address, AddrIdx: i}
					cn.SetWindowSize(2) // use 2 to form pipeline
					cn.Handler = c
				})
			}
		} else {
			c.conns[address][i] = client.NewShortcut(c.shortcut.Validate(i).Conns[i], func(cn *client.Conn) {
				cn.Meta = &ClientConnMeta{Addr: address, AddrIdx: i}
				cn.SetWindowSize(2) // use 2 to form pipeline
				cn.Handler = c
			})
		}
	}
	cn = c.conns[address][i]
	return
}

type clientMember string

func (m clientMember) String() string {
	return string(m)
}

type ecRetMeta struct {
	Raw      string
	Size     int
	NumFrags int
}

type ecRet struct {
	sync.WaitGroup
	reqs  []*ClientRequest
	numOK int32

	Shards int
	Err    error
	Meta   ecRetMeta // only for get chunk
	Stats  *logEntry
}

func newEcRet(shards int) *ecRet {
	return &ecRet{
		reqs:   make([]*ClientRequest, shards),
		Shards: shards,
	}
}

func (r *ecRet) Len() int {
	return r.Shards
}

func (r *ecRet) Request(i int) *ClientRequest {
	if r.reqs[i] == nil {
		ctx := context.WithValue(context.Background(), CtxKeyECRet, r)
		req := &ClientRequest{Request: client.NewRequestWithContext(ctx)}
		req.OnRespond(func(rsp interface{}, err error) {
			if req.Cancel != nil {
				req.Cancel()
			}
			// In case of deadline exceeded, we don't know what blocks the connection. Close it to force a new connection to be created next time.
			if err == context.DeadlineExceeded || util.IsConnectionFailed(err) {
				req.Conn().Close()
			}

			// Keep record of the last error
			if err != nil {
				r.Err = err
			} else if rsp != nil {
				// Only count ok if response is not nil
				atomic.AddInt32(&r.numOK, 1)
			}
			r.Done()
		})
		r.reqs[i] = req
	}
	return r.reqs[i]
}

func (r *ecRet) Set(i int, ret interface{}) {
	req := r.reqs[i]
	if req != nil {
		req.SetResponse(ret)
	}
}

func (r *ecRet) SetError(i int, err error) {
	r.Set(i, err)
}

func (r *ecRet) RetStore(i int) (ret string) {
	req := r.reqs[i]
	if req == nil {
		return ""
	}
	val, _ := req.Response()
	ret, _ = val.(string)
	return
}

func (r *ecRet) RetChunk(i int) (ret []byte) {
	req := r.reqs[i]
	if req == nil {
		return nil
	}
	val, _ := req.Response()
	ret, _ = val.([]byte)
	return
}

func (r *ecRet) Error(i int) (err error) {
	req := r.reqs[i]
	if req == nil {
		return ErrNoRequest
	}
	_, err = r.Request(i).Response()
	return
}

func (r *ecRet) PrintErrors(prompts string, args ...interface{}) {
	if r.Err == nil {
		return
	}

	// Format prompts
	if len(args) > 0 {
		prompts = fmt.Sprintf(prompts, args...)
	}

	// Print errors
	if r.NumOK() < len(r.reqs) {
		log.Warn("%s:%v, details(%d):", prompts, r.Err, r.NumOK())
	} else {
		log.Warn("%s:%v", prompts, r.Err)
	}
	for i := 0; i < len(r.reqs); i++ {
		if err := r.Error(i); err != nil {
			log.Warn("%s(%d):%v", prompts, i, err)
		}
	}
}

func (r *ecRet) NumOK() int {
	return int(atomic.LoadInt32(&r.numOK))
}
