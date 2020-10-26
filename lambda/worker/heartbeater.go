package worker

import (
	"errors"
)

var (
	ErrHeartbeatFailed = errors.New("heartbeat failed")
)

type Heartbeater interface {
	SendToLink(*Link) error
}

type DefaultHeartbeater struct {
}

func (hb *DefaultHeartbeater) SendToLink(link *Link) error {
	return nil
}
