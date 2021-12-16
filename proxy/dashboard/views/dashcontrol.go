package views

import "github.com/mason-leap-lab/infinicache/proxy/types"

type DashControl interface {
	Quit(string)
	GetOccupancyMode() types.InstanceOccupancyMode
}
