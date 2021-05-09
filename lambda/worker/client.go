package worker

import (
	"net"

	"github.com/mason-leap-lab/redeo/resp"
)

// Simple client of worker for debuging.
type Client struct {
	Ctrl   bool
	Conn   net.Conn
	Writer *resp.RequestWriter
	Reader resp.ResponseReader
}

func NewClient(cn net.Conn, ctrl bool) *Client {
	return &Client{
		Ctrl:   ctrl,
		Conn:   cn,
		Writer: resp.NewRequestWriter(cn),
		Reader: resp.NewResponseReader(cn),
	}
}
