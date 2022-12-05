package util

import (
	"github.com/cespare/xxhash"
)

type Hasher struct {
}

func (h *Hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}
