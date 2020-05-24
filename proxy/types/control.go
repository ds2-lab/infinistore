package types

import (
	"errors"
	"strconv"
	"time"
)

type Control struct {
	Cmd        string
	Addr       string
	Deployment string
	Id         uint64
	Payload    []byte
	*Request
	conn       Conn
}

func (req *Control) String() string {
	return req.Cmd
}

func (req *Control) GetRequest() *Request {
	return req.Request
}

func (req *Control) Retriable() bool {
	return true
}

func (ctrl *Control) PrepareForData(conn Conn) {
	conn.Writer().WriteCmdString(ctrl.Cmd)
	ctrl.conn = conn
}

func (ctrl *Control) PrepareForMigrate(conn Conn) {
	conn.Writer().WriteCmdString(ctrl.Cmd, ctrl.Addr, ctrl.Deployment, strconv.FormatUint(ctrl.Id, 10))
	ctrl.conn = conn
}

func (ctrl *Control) PrepareForDel(conn Conn) {
	ctrl.Request.PrepareForDel(conn)
	ctrl.Request.conn = nil
	ctrl.conn = conn
}

func (ctrl *Control) Flush(timeout time.Duration) (err error) {
	if ctrl.conn == nil {
		return errors.New("Connection for control not set.")
	}
	conn := ctrl.conn
	ctrl.conn = nil

	conn.SetWriteDeadline(time.Now().Add(timeout)) // Set deadline for write
	defer conn.SetWriteDeadline(time.Time{})
	return conn.Writer().Flush()
}
