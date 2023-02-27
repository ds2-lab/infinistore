package views

import "github.com/ds2-lab/infinistore/proxy/types"

type DashControl interface {
	Quit(string)
	GetOccupancyMode() types.InstanceOccupancyMode
}
