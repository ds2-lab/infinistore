package worker

import (
	"errors"
)

var (
	ErrHeartbeatFailed = errors.New("heartbeat failed")
)

type Heartbeater interface {
	SendToLink(*Link, int64) error
}

type DefaultHeartbeater struct {
}

func (hb *DefaultHeartbeater) SendToLink(link *Link, flags int64) error {
	return nil
}

type HeartbeatError interface {
	Flags() int64
}
