package client

import (
	"errors"
	"net"

	"github.com/buraksezer/consistent"
	mock "github.com/jordwest/mock-conn"
	"github.com/klauspost/reedsolomon"
	"github.com/mason-leap-lab/redeo/resp"

	// cuckoo "github.com/seiflotfy/cuckoofilter"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

// Client InfiniCache client
type Client struct {
	EC           reedsolomon.Encoder
	Ring         *consistent.Consistent
	DataShards   int
	ParityShards int
	Shards       int

	conns map[string][]*clientConn
	// mappingTable map[string]*cuckoo.Filter
	logEntry logEntry
}

// NewClient Create a client instance.
func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	return &Client{
		conns: make(map[string][]*clientConn),
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
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
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
	c.Ring = consistent.New(members, cfg)
	//time0 := time.Since(t0)
	//fmt.Println("Dial all goroutines are done!")
	//if err := nanolog.Log(LogClient, "Dial", time0.String()); err != nil {
	//	fmt.Println(err)
	//}
	return true
}

// Close Close the client
func (c *Client) Close() {
	log.Info("Cleaning up...")
	for addr, conns := range c.conns {
		for i := range conns {
			c.disconnect(addr, i)
		}
	}
	log.Info("Client closed.")
}

//func (c *Client) initDial(address string, wg *sync.WaitGroup) {
func (c *Client) initDial(address string) (addr string, err error) {
	// initialize parallel connections under address
	connect := c.connect
	addr, ok := protocol.Shortcut.Validate(address)
	if ok {
		connect = c.connectShortcut
	} else {
		addr = address
	}
	// Connect use original address
	c.conns[addr], err = connect(address, c.Shards)
	// if err == nil {
	// 	// initialize the cuckoo filter under address
	// 	c.mappingTable[addr] = cuckoo.NewFilter(1000000)
	// }
	return
}

func (c *Client) connect(address string, n int) ([]*clientConn, error) {
	conns := make([]*clientConn, n)
	for i := 0; i < n; i++ {
		cn, err := net.Dial("tcp", address)
		if err != nil {
			return conns, err
		}
		conns[i] = newClientConn(cn)
	}
	return conns, nil
}

func (c *Client) connectShortcut(address string, n int) ([]*clientConn, error) {
	shortcuts, ok := protocol.Shortcut.Dial(address)
	if !ok {
		return nil, errors.New("oops, check the RedisAdapter")
	}
	conns := make([]*clientConn, n)
	for i := 0; i < n && i < len(shortcuts); i++ {
		conns[i] = newClientConn(shortcuts[i].Client)
	}
	return conns, nil
}

func (c *Client) disconnect(address string, i int) {
	if c.conns[address][i] != nil {
		c.conns[address][i] = c.conns[address][i].Close() // Noted a shortcut will not be closed.
	}
}

func (c *Client) validate(address string, i int) (err error) {
	// Because shortcut can not be closed, we don't consider it here.
	if c.conns[address][i] == nil {
		var conn net.Conn
		conn, err = net.Dial("tcp", address)
		if err == nil {
			c.conns[address][i] = newClientConn(conn)
		}
	}
	return
}

type clientConn struct {
	conn     net.Conn
	shortcut bool
	W        *resp.RequestWriter
	R        resp.ResponseReader
}

func newClientConn(cn net.Conn) *clientConn {
	_, shortcut := cn.(*mock.End)
	return &clientConn{
		conn:     cn,
		shortcut: shortcut,
		W:        resp.NewRequestWriter(cn),
		R:        resp.NewResponseReader(cn),
	}
}

// Close close connection and return value for reset
func (c *clientConn) Close() *clientConn {
	// No need to close shortcut. It is supposed to be usable always.
	if c.shortcut {
		return c
	}
	c.conn.Close()
	return nil
}

type clientMember string

func (m clientMember) String() string {
	return string(m)
}

type ecRet struct {
	Shards int
	Rets   []interface{}
	Err    error
	Size   int // only for get chunk
}

func newEcRet(shards int) *ecRet {
	return &ecRet{
		Shards: shards,
		Rets:   make([]interface{}, shards),
	}
}

func (r *ecRet) Len() int {
	return r.Shards
}

func (r *ecRet) Set(i int, ret interface{}) {
	r.Rets[i] = ret
}

func (r *ecRet) SetError(i int, ret interface{}) {
	r.Rets[i] = ret
	r.Err = ret.(error)
}

func (r *ecRet) Ret(i int) (ret []byte) {
	ret, _ = r.Rets[i].([]byte)
	return
}

func (r *ecRet) Error(i int) (err error) {
	err, _ = r.Rets[i].(error)
	return
}
