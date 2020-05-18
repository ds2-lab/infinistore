package global

import (
	"github.com/mason-leap-lab/infinicache/proxy/config"
)

type CommandlineOptions struct {
	Pid           string
	Debug         bool
	Prefix        string
	D             int
	P             int
	NoDashboard   bool
	NoColor       bool
	LogPath       string
	LogFile       string
	Evaluation    bool
	NumBackups    int
	NoFirstD      bool
	FuncCapacity  uint64
}

func (o *CommandlineOptions) GetInstanceCapacity() uint64 {
	if o.FuncCapacity == 0 {
		return config.InstanceCapacity
	} else if o.FuncCapacity < 128000000 {
		return o.FuncCapacity * 1000000
	} else {
		return o.FuncCapacity
	}
}
