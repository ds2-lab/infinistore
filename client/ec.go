package client

import (
	"errors"
	"io"

	"github.com/klauspost/reedsolomon"
)

var (
	// ErrNotImplemented The error indicates specified function is not implemented.
	ErrNotImplemented = errors.New("not implemented")
)

// NewEncoder Helper function to create a encoder
func NewEncoder(dataShards int, parityShards int, ecMaxGoroutine int) reedsolomon.Encoder {
	if parityShards == 0 {
		return &DummyEncoder{DataShards: dataShards}
	}

	enc, err := reedsolomon.New(dataShards, parityShards, reedsolomon.WithMaxGoroutines(ecMaxGoroutine))
	if err != nil {
		log.Error("Failed to create reedsolomon ec instance: %v", err)
		return nil
	}
	return enc
}

// DummyEncoder Dummpy encoder to support 0 parity.
type DummyEncoder struct {
	DataShards int
}

// Encode reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Encode(shards [][]byte) error {
	return nil
}

// Verify reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Verify(shards [][]byte) (bool, error) {
	if len(shards) != enc.DataShards {
		return false, reedsolomon.ErrTooFewShards
	}

	for _, shard := range shards {
		if len(shard) == 0 {
			return false, reedsolomon.ErrTooFewShards
		}
	}
	return true, nil
}

// Reconstruct reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Reconstruct(shards [][]byte) (err error) {
	_, err = enc.Verify(shards)
	return
}

// ReconstructData reedsolomon.Encoder implmentation
func (enc *DummyEncoder) ReconstructData(shards [][]byte) (err error) {
	_, err = enc.Verify(shards)
	return
}

// Update reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Update(shards [][]byte, newDatashards [][]byte) error {
	return ErrNotImplemented
}

// Split reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, reedsolomon.ErrShortData
	}
	// Calculate number of bytes per data shard.
	perShard := (len(data) + enc.DataShards - 1) / enc.DataShards

	// Split into shards, the size of shards may be not the same.
	dst := make([][]byte, enc.DataShards)
	i := 0
	for ; i < len(dst) && len(data) >= perShard; i++ {
		dst[i] = data[:perShard]
		data = data[perShard:]
	}

	if i < len(dst) {
		dst[i] = data
	}

	return dst, nil
}

// Join reedsolomon.Encoder implmentation
func (enc *DummyEncoder) Join(dst io.Writer, shards [][]byte, outSize int) error {
	// Do we have enough shards?
	if len(shards) < enc.DataShards {
		return reedsolomon.ErrTooFewShards
	}
	shards = shards[:enc.DataShards]

	// Do we have enough data?
	size := 0
	for _, shard := range shards {
		if shard == nil {
			return reedsolomon.ErrReconstructRequired
		}
		size += len(shard)

		// Do we have enough data already?
		if size >= outSize {
			break
		}
	}
	if size < outSize {
		return reedsolomon.ErrShortData
	}

	// Copy data to dst
	write := outSize
	for _, shard := range shards {
		if write < len(shard) {
			_, err := dst.Write(shard[:write])
			return err
		}
		n, err := dst.Write(shard)
		if err != nil {
			return err
		}
		write -= n
	}
	return nil
}
