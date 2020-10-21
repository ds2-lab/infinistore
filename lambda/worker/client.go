package worker

import (
	"net"

	"github.com/mason-leap-lab/redeo/resp"
)

// Simple client of worker for debuging.
type Client struct {
	Conn   net.Conn
	Writer *resp.RequestWriter
	Reader resp.ResponseReader
}

func NewClient(cn net.Conn) *Client {
	return &Client{
		Conn:   cn,
		Writer: resp.NewRequestWriter(cn),
		Reader: resp.NewResponseReader(cn),
	}
}
