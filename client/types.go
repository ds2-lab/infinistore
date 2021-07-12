package client

import "github.com/mason-leap-lab/infinicache/common/redeo/client"

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
	Cmd   string
	ReqId string
}
