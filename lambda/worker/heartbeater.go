package worker

import (
	"errors"

	"github.com/mason-leap-lab/redeo"
)

var (
	ErrHeartbeatFailed = errors.New("heartbeat failed")
)

type Heartbeater interface {
	SendToLink(*redeo.Client) error
}

type DefaultHeartbeater struct {
}

func (hb *DefaultHeartbeater) SendToLink(link *redeo.Client) error {
	return nil
}
