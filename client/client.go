package client

import (
	"errors"
	"github.com/buraksezer/consistent"
	"github.com/klauspost/reedsolomon"
	"github.com/mason-leap-lab/redeo/resp"
	"github.com/seiflotfy/cuckoofilter"
	"io"
	"net"
	"time"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
)

type Conn struct {
	conn net.Conn
	W    *resp.RequestWriter
	R    resp.ResponseReader
}

func NewConn(cn net.Conn) *Conn {
	return &Conn{
		conn: cn,
		W:    NewRequestWriter(cn),
		R:    NewResponseReader(cn),
	}
}

func (c *Conn) Close() {
	c.conn.Close()
}

type DataEntry struct {
	Cmd        string
	ReqId      string
	Begin      time.Time
	ReqLatency time.Duration
	RecLatency time.Duration
	Duration   time.Duration
	AllGood    bool
	Corrupted  bool
}

type Client struct {
	//ConnArr  []net.Conn
	//W        []*resp.RequestWriter
	//R        []resp.ResponseReader
	Conns        map[string][]*Conn
	EC           reedsolomon.Encoder
	MappingTable map[string]*cuckoo.Filter
	Ring         *consistent.Consistent
	Data         DataEntry
	DataShards   int
	ParityShards int
	Shards       int
}

func NewClient(dataShards int, parityShards int, ecMaxGoroutine int) *Client {
	return &Client{
		//ConnArr:  make([]net.Conn, dataShards+parityShards),
		//W:        make([]*resp.RequestWriter, dataShards+parityShards),
		//R:        make([]resp.ResponseReader, dataShards+parityShards),
		Conns:        make(map[string][]*Conn),
		EC:           NewEncoder(dataShards, parityShards, ecMaxGoroutine),
		MappingTable: make(map[string]*cuckoo.Filter),
		DataShards:   dataShards,
		ParityShards: parityShards,
		Shards:       dataShards + parityShards,
	}
}

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
		members[i] = Member(addr)
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

//func (c *Client) initDial(address string, wg *sync.WaitGroup) {
func (c *Client) initDial(address string) (err error) {
	// initialize parallel connections under address
	connect := c.connect
	if protocol.Shortcut.Validate(address) {
		connect = c.connectShortcut
	}
	c.Conns[address], err = connect(address, c.Shards)
	if err == nil {
		// initialize the cuckoo filter under address
		c.MappingTable[address] = cuckoo.NewFilter(1000000)
	}
	return
}

func (c *Client) connect(address string, n int) ([]*Conn, error) {
	conns := make([]*Conn, n)
	for i := 0; i < n; i++ {
		cn, err := net.Dial("tcp", address)
		if err != nil {
			return conns, err
		}
		conns[i] = NewConn(cn)
	}
	return conns, nil
}

func (c *Client) connectShortcut(address string, n int) ([]*Conn, error) {
	shortcuts, ok := protocol.Shortcut.Dial(address)
	if !ok {
		return nil, errors.New("oops, check the RedisAdapter")
	}
	conns := make([]*Conn, n)
	for i := 0; i < n && i < len(shortcuts); i++ {
		conns[i] = NewConn(shortcuts[i].Client)
	}
	return conns, nil
}

func (c *Client) disconnect(address string, i int) {
	if c.Conns[address][i] != nil {
		c.Conns[address][i].Close()
		c.Conns[address][i] = nil
	}
}

func (c *Client) validate(address string, i int) (err error) {
	if c.Conns[address][i] == nil {
		var conn net.Conn
		conn, err = net.Dial("tcp", address)
		if err == nil {
			c.Conns[address][i] = NewConn(conn)
		}
	}
	return
}

func (c *Client) Close() {
	log.Info("Cleaning up...")
	for addr, conns := range c.Conns {
		for i, _ := range conns {
			c.disconnect(addr, i)
		}
	}
	log.Info("Client closed.")
}

type ecRet struct {
	Shards          int
	Rets            []interface{}
	Err             error
	Size            int    // only for get chunk
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

type ReadAllCloser interface {
	io.Reader
	io.Closer

	Len() int
	ReadAll() ([]byte, error)
}
