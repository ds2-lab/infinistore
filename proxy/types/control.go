package types

import (
	"errors"
	"github.com/mason-leap-lab/redeo/resp"
	"strconv"
)

type ControlCallback func(*Control, interface{})

type Control struct {
	Cmd        string
	Addr       string
	Deployment string
	Id         uint64
	Payload    []byte
	*Request
	w          *resp.RequestWriter
	Callback   ControlCallback
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

func (ctrl *Control) PrepareForData(w *resp.RequestWriter) {
	w.WriteCmdString(ctrl.Cmd)
	ctrl.w = w
}

func (ctrl *Control) PrepareForMigrate(w *resp.RequestWriter) {
	w.WriteCmdString(ctrl.Cmd, ctrl.Addr, ctrl.Deployment, strconv.FormatUint(ctrl.Id, 10))
	ctrl.w = w
}

func (ctrl *Control) PrepareForDel(w *resp.RequestWriter) {
	ctrl.Request.PrepareForDel(w)
	ctrl.w = w
}

func (ctrl *Control) PrepareForRecover(w *resp.RequestWriter) {
	ctrl.Request.PrepareForRecover(w)
	ctrl.w = w
}

func (ctrl *Control) Flush() (err error) {
	if ctrl.w == nil {
		return errors.New("Writer for request not set.")
	}
	w := ctrl.w
	ctrl.w = nil

	return w.Flush()
}
