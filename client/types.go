package client

import (
	"context"

	"github.com/ds2-lab/infinistore/common/redeo/client"
)

type WaitGroup interface {
	Add(int)
	Done()
	Wait()
}

type ClientConnMeta struct {
	Addr    string
	AddrIdx int
}

type ClientRequest struct {
	client.Request
	Cmd    string
	ReqId  string
	Cancel context.CancelFunc
	cn     *client.Conn
}

func (r *ClientRequest) Conn() *client.Conn {
	return r.cn
}

func (r *ClientRequest) SetConn(cn *client.Conn) {
	r.cn = cn
}
