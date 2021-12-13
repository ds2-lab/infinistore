package lambdastore

import (
	protocol "github.com/mason-leap-lab/infinicache/common/types"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

// Delegate offers new Backup impletation for delagation.
type Delegate struct {
	*Instance
}

// Start serving as the delegate for specified instance.
// Return true always.
func (ins *Delegate) StartBacking(deleIns *Instance, bakId int, total int) bool {
	// Manually trigger ping with payload to initiate parallel recovery
	payload, err := deleIns.Meta.ToDelegatePayload(deleIns.Id(), bakId, total, ins.getRerouteThreshold())
	if err != nil {
		ins.log.Warn("Failed to prepare payload to trigger delegation: %v", err)
	} else {
		ins.chanPriorCmd <- &types.Control{
			Cmd:     protocol.CMD_PING,
			Payload: payload,
		}
	}
	return true
}
