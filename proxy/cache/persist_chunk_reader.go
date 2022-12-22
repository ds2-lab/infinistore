package cache

import (
	"context"
	"fmt"
	"io"

	"github.com/mason-leap-lab/infinicache/common/util"
)

type persistChunkReader struct {
	chunk        *persistChunk
	r            int64
	done         util.Closer
	doneWaitData chan struct{}
	ctx          context.Context
	lastError    error
}

func newPersistChunkReader(ctx context.Context, c *persistChunk) *persistChunkReader {
	return &persistChunkReader{
		chunk:        c,
		r:            0,
		doneWaitData: make(chan struct{}),
		ctx:          ctx,
	}
}

// String returns a description of the chunk.
func (b *persistChunkReader) String() string {
	return fmt.Sprintf("persistChunkReader(%v)", b.chunk)
}

func (b *persistChunkReader) Read(p []byte) (n int, err error) {
	if b.r >= b.chunk.Size() {
		return 0, io.EOF
	}

	available := b.chunk.BytesStored()
	if available <= b.r {
		// Wait for the chunk to be buffered
		available, err = b.chunk.waitDataWithContext(b.ctx, b.r, b.doneWaitData)
		b.lastError = err
	}
	if available <= b.r {
		if err != nil {
			b.Unhold()
		}
		return
	}

	if available >= b.r+int64(len(p)) {
		available = b.r + int64(len(p))
	} else {
		p = p[0 : available-b.r]
	}
	n = copy(p, b.chunk.data[b.r:available])

	b.r += int64(n)

	// Auto unhold
	if b.r >= b.chunk.Size() || err != nil {
		b.Unhold()
	}
	return
}

func (b *persistChunkReader) Len() int64 { return b.chunk.Size() }

func (b *persistChunkReader) ReadAll() (data []byte, err error) {
	if b.r >= b.chunk.Size() {
		return nil, io.EOF
	}

	ret, err := b.chunk.LoadAll(b.ctx)
	b.Unhold()
	if b.r > 0 {
		data = data[b.r:]
	} else {
		data = ret
	}
	b.r = int64(len(ret))
	return
}

func (b *persistChunkReader) Hold() {
	b.done.Init()
}

func (b *persistChunkReader) Unhold() {
	b.done.Close()
}

// Close discards any unread data
func (b *persistChunkReader) Close() (err error) {
	b.done.Wait()

	if !b.chunk.IsStored() {
		err = b.lastError
	}

	read := b.r
	b.r = b.chunk.Size()
	b.chunk.waitReader(b, read)
	return
}
