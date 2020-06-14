package client

import (
	"errors"
	"net"

	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"
	"github.com/mason-leap-lab/redeo/resp"
	cuckoo "github.com/seiflotfy/cuckoofilter"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

// Client InfiniCache client
type Client struct {
	EC           reedsolomon.Encoder
	MappingTable map[string]*cuckoo.Filter
	Ring         *consistent.Consistent
	DataShards   int
	ParityShards int
	Shards       int

	conns    map[string][]*clientConn
	logEntry logEntry
}

// NewClient Create a client instance.
func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	return &Client{
		conns:        make(map[string][]*clientConn),
		EC:           NewEncoder(dataShards, parityShards, ecMaxGoroutine),
		MappingTable: make(map[string]*cuckoo.Filter),
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
		members[i] = clientMember(addr)
		log.Debug("Dialing %s...", addr)
		if err := c.initDial(addr); err != nil {
			log.Error("Fail to dial %s: %v", addr, err)
			c.Close()
			return false
		}
	}
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
func (c *Client) initDial(address string) (err error) {
	// initialize parallel connections under address
	connect := c.connect
	if protocol.Shortcut.Validate(address) {
		connect = c.connectShortcut
	}
	c.conns[address], err = connect(address, c.Shards)
	if err == nil {
		// initialize the cuckoo filter under address
		c.MappingTable[address] = cuckoo.NewFilter(1000000)
	}
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
		c.conns[address][i].Close()
		c.conns[address][i] = nil
	}
}

func (c *Client) validate(address string, i int) (err error) {
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
	conn net.Conn
	W    *resp.RequestWriter
	R    resp.ResponseReader
}

func newClientConn(cn net.Conn) *clientConn {
	return &clientConn{
		conn: cn,
		W:    resp.NewRequestWriter(cn),
		R:    resp.NewResponseReader(cn),
	}
}

func (c *clientConn) Close() {
	c.conn.Close()
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
