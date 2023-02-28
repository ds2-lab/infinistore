package metastore

import "github.com/ds2-lab/infinistore/proxy/types"

const (
	PlacerEventBeforePlacing = "before_placing"
)

type PlacerEvent string

type PlacerHandler func(meta *Meta, chunkId int, req types.Command)

type PlacerEvents struct {
	beforePlacing PlacerHandler
}

func newPlacerEvents() *PlacerEvents {
	events := &PlacerEvents{}
	events.beforePlacing = events.defaultPlacerEventHandler
	return events
}

func (e *PlacerEvents) RegisterHandler(event PlacerEvent, handler PlacerHandler) {
	switch event {
	case PlacerEventBeforePlacing:
		e.beforePlacing = handler
	}
}

func (e *PlacerEvents) defaultPlacerEventHandler(_ *Meta, _ int, _ types.Command) {
	// Do nothing
}
