package worker

import (
	"github.com/mason-leap-lab/redeo"
	"github.com/mason-leap-lab/redeo/resp"
)

type HandlerProxy struct {
	worker       *Worker
	handle       redeo.HandlerFunc
	streamHandle redeo.StreamHandlerFunc
}

func (h *HandlerProxy) HandlerFunc(w resp.ResponseWriter, c *resp.Command) {
	h.TryRevokeToken(redeo.GetClient(c.Context()))
	h.handle(w, c)
}

func (h *HandlerProxy) StreamHandlerFunc(w resp.ResponseWriter, c *resp.CommandStream) {
	h.TryRevokeToken(redeo.GetClient(c.Context()))
	h.streamHandle(w, c)
}

func (h *HandlerProxy) TryRevokeToken(client *redeo.Client) {
	h.worker.flagReservationUsed(LinkFromClient(client))
}
