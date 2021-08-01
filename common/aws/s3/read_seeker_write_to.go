package s3

import (
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type bufferedReadFrom struct {
	bufferedWriter
}

func (b *bufferedReadFrom) ReadFrom(r io.Reader) (int64, error) {
	n, err := b.bufferedWriter.ReadFrom(r)
	if flushErr := b.Flush(); flushErr != nil && err == nil {
		err = flushErr
	}
	return n, err
}

// BufferedReadSeekerWriteToPool uses a sync.Pool to create and reuse
// []byte slices for buffering parts in memory
type BufferedReadSeekerWriteToPool struct {
	pool sync.Pool
	size int
}

// NewBufferedReadSeekerWriteToPool will return a new BufferedReadSeekerWriteToPool that will create
// a pool of reusable buffers . If size is less then < 64 KiB then the buffer
// will default to 64 KiB. Reason: io.Copy from writers or readers that don't support io.WriteTo or io.ReadFrom
// respectively will default to copying 32 KiB.
func NewBufferedReadSeekerWriteToPool(size int) *BufferedReadSeekerWriteToPool {
	if size < 65536 {
		size = 65536
	}

	return &BufferedReadSeekerWriteToPool{size: size}
}

// GetWriteTo will wrap the provided io.ReadSeeker with a BufferedReadSeekerWriteTo.
// The provided cleanup must be called after operations have been completed on the
// returned io.ReadSeekerWriteTo in order to signal the return of resources to the pool.
func (p *BufferedReadSeekerWriteToPool) GetWriteTo(seeker io.ReadSeeker) (r s3manager.ReadSeekerWriteTo, cleanup func()) {
	buffer, _ := p.pool.Get().([]byte)
	if buffer == nil {
		buffer = make([]byte, p.size)
	}

	r = &s3manager.BufferedReadSeekerWriteTo{BufferedReadSeeker: s3manager.NewBufferedReadSeeker(seeker, buffer)}
	cleanup = func() {
		p.pool.Put(buffer)
	}

	return r, cleanup
}

func (p *BufferedReadSeekerWriteToPool) Close() {
	alloced, _ := p.pool.Get().([]byte)
	for alloced != nil {
		alloced = nil
		alloced, _ = p.pool.Get().([]byte)
	}
}
