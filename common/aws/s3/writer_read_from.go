package s3

import (
	"bufio"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type bufferedWriter interface {
	s3manager.WriterReadFrom
	Flush() error
	Reset(io.Writer)
}

// PooledBufferedReadFromProvider is a WriterReadFromProvider that uses a sync.Pool
// to manage allocation and reuse of *bufio.Writer structures.
type PooledBufferedReadFromProvider struct {
	size int
	pool sync.Pool
}

// NewPooledBufferedWriterReadFromProvider returns a new PooledBufferedReadFromProvider
// Size is used to control the size of the underlying *bufio.Writer created for
// calls to GetReadFrom.
func NewPooledBufferedWriterReadFromProvider(size int) *PooledBufferedReadFromProvider {
	if size < 65536 {
		size = 65536
	}

	return &PooledBufferedReadFromProvider{size: size}
}

// GetReadFrom takes an io.Writer and wraps it with a type which satisfies the WriterReadFrom
// interface/ Additionally a cleanup function is provided which must be called after usage of the WriterReadFrom
// has been completed in order to allow the reuse of the *bufio.Writer
func (p *PooledBufferedReadFromProvider) GetReadFrom(writer io.Writer) (r s3manager.WriterReadFrom, cleanup func()) {
	buffer, _ := p.pool.Get().(*bufferedReadFrom)
	if buffer == nil {
		buffer = &bufferedReadFrom{bufferedWriter: bufio.NewWriterSize(writer, p.size)}
	} else {
		buffer.Reset(writer)
	}

	r = buffer
	cleanup = func() {
		buffer.Reset(nil) // Reset to nil writer to release reference
		p.pool.Put(buffer)
	}
	return r, cleanup
}

func (p *PooledBufferedReadFromProvider) Close() {
	alloced, _ := p.pool.Get().(*bufferedReadFrom)
	for alloced != nil {
		alloced = nil
		alloced, _ = p.pool.Get().(*bufferedReadFrom)
	}
}
