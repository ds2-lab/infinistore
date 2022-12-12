package server

import (
	"fmt"
	"io"

	"github.com/mason-leap-lab/infinicache/common/util"
	"github.com/mason-leap-lab/redeo/resp"
)

type InterceptEvent func(*InterceptReader)

type InterceptReader struct {
	resp.AllReadCloser

	// Private fields
	buf       []byte
	r         int64
	lastError error
	done      util.Closer

	// Events
	onIntercept InterceptEvent
	onClose     InterceptEvent
}

// NewInterceptReader creates a new InterceptReader.
func NewInterceptReader(reader resp.AllReadCloser) *InterceptReader {
	return NewInterceptReaderWithBuffer(reader, make([]byte, reader.Len()))
}

// NewInterceptReaderWithBuffer creates a new InterceptReader with a given buffer.
func NewInterceptReaderWithBuffer(reader resp.AllReadCloser, buf []byte) *InterceptReader {
	return &InterceptReader{
		AllReadCloser: reader,
		buf:           buf,
		onIntercept:   defaultInterceptHandler,
		onClose:       defaultInterceptHandler,
	}
}

// String returns a description of the chunk.
func (ir *InterceptReader) String() string {
	return fmt.Sprintf("InterceptReader(%d)", ir.Len())
}

func (ir *InterceptReader) Read(p []byte) (n int, err error) {
	if ir.r == int64(len(ir.buf)) {
		return 0, io.EOF
	}

	n, err = ir.AllReadCloser.Read(p)
	ir.lastError = err
	if n > 0 {
		copy(ir.buf[ir.r:], p[0:n])
		ir.r += int64(n)
	}
	ir.onIntercept(ir)
	if ir.r == int64(len(ir.buf)) || err != nil {
		ir.Unhold()
	}
	return
}

func (ir *InterceptReader) ReadAll() ([]byte, error) {
	// Since we have allocated the buffer, we can simply read all the data into it.
	n, err := io.ReadFull(ir.AllReadCloser, ir.buf[ir.r:])
	ir.r += int64(n)
	return ir.buf[:ir.r], err
}

func (ir *InterceptReader) Intercepted() []byte {
	return ir.buf
}

func (ir *InterceptReader) LastError() error {
	return ir.lastError
}

func (ir *InterceptReader) BytesIntercepted() int64 {
	return ir.r
}

func (ir *InterceptReader) Hold() {
	ir.done.Init()
	// No need to call AllReadCloser.Hold() since we will always wait our own done signal and Close the AllReadCloser.
}

func (ir *InterceptReader) Unhold() {
	if holdable, ok := ir.AllReadCloser.(resp.Holdable); ok {
		holdable.Unhold()
	}
	ir.done.Close()
}

// Close discards any unread data
func (ir *InterceptReader) Close() error {
	ir.done.Wait()
	// Drain the reader
	_, err := ir.ReadAll()
	ir.onClose(ir)
	return err
}

func (ir *InterceptReader) OnIntercept(handler InterceptEvent) {
	ir.onIntercept = handler
}

func (ir *InterceptReader) OnClose(handler InterceptEvent) {
	ir.onClose = handler
}

func defaultInterceptHandler(_ *InterceptReader) {
}