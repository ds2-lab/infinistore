package global

import (
	"github.com/mason-leap-lab/infinicache/proxy/config"
)

type CommandlineOptions struct {
	Pid                string
	Debug              bool
	Prefix             string
	LambdaPrefix       string
	D                  int
	P                  int
	NoDashboard        bool
	NoColor            bool
	LogPath            string
	LogFile            string
	Evaluation         bool
	NumBackups         int
	NoFirstD           bool
	FuncCapacity       uint64
	FuncThreshold      uint64
	FuncChunkThreshold int
}

func (o *CommandlineOptions) GetInstanceCapacity() uint64 {
	if o.FuncCapacity == 0 {
		// Reset as default value
		o.FuncCapacity = config.DefaultInstanceCapacity
	} else if o.FuncCapacity < 128000000 {
		// Normalize to bytes
		o.FuncCapacity = o.FuncCapacity * 1000000
	}
	return o.FuncCapacity
}

func (o *CommandlineOptions) GetInstanceThreshold() uint64 {
	if o.FuncThreshold == 0 {
		o.FuncThreshold = uint64(float64(o.GetInstanceCapacity()) * config.Threshold)
	}

	return o.FuncThreshold
}

func (o *CommandlineOptions) GetInstanceChunkThreshold() int {
	if o.FuncChunkThreshold == 0 {
		o.FuncChunkThreshold = int(o.GetInstanceCapacity() / config.ChunkThreshold)
	}

	return o.FuncChunkThreshold
}
