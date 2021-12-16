package types

import (
	"errors"
	"strconv"
	"strings"
)

var (
	ErrControlCmd = errors.New("will not retry control command")
)

type ControlCallback func(*Control, interface{})

type Control struct {
	Cmd        string
	Addr       string
	Deployment string
	Id         uint64
	Info       interface{}
	Payload    []byte
	*Request
	conn     Conn
	err      error
	Callback ControlCallback
}

func (req *Control) String() string {
	return req.Cmd
}

func (req *Control) Name() string {
	return strings.ToLower(req.Cmd)
}

func (req *Control) GetInfo() interface{} {
	return req.Info
}

func (req *Control) GetRequest() *Request {
	return req.Request
}

func (req *Control) MarkError(err error) int {
	req.err = err
	return 0
}

func (req *Control) LastError() (int, error) {
	if req.err == nil {
		return 1, nil
	}
	return 0, req.err
}

func (req *Control) FailureError() error {
	return ErrControlCmd
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

func (ctrl *Control) PrepareForRecover(conn Conn) {
	ctrl.Request.PrepareForRecover(conn)
	ctrl.Request.conn = nil
	ctrl.conn = conn
}

func (ctrl *Control) Flush() (err error) {
	if ctrl.conn == nil {
		return errors.New("connection for control not set")
	}
	conn := ctrl.conn
	ctrl.conn = nil

	return conn.Writer().Flush()
}
