package client

import (
	"io"
	"sync"
)

// ReadAllCloser Allows to get length and read all from the reader.
type ReadAllCloser interface {
	io.Reader
	io.Closer

	Len() int
	ReadAll() ([]byte, error)
}

// Joiner The utility function helps to concat an array of []bytes.
type Joiner func(io.Writer, [][]byte, int) error

// JoinReader A ReadAllCloser implementation that can concat an array of []bytes
type JoinReader struct {
	io.ReadCloser
	writer  io.Writer
	data    [][]byte
	read    int
	size    int
	once    sync.Once
	joiner  Joiner
	started bool
}

// NewJoinReader Create the join reader
func NewJoinReader(data [][]byte, size int, joiner Joiner) *JoinReader {
	reader, writer := io.Pipe()
	return &JoinReader{
		ReadCloser: reader,
		writer:     writer,
		data:       data,
		size:       size,
		joiner:     joiner,
	}
}

// Read io.Reader implementation
func (r *JoinReader) Read(p []byte) (n int, err error) {
	r.once.Do(r.join)
	n, err = r.ReadCloser.Read(p)
	r.read += n
	return
}

// Len ReaderAllCloser implementation
func (r *JoinReader) Len() int {
	return r.size - r.read
}

// ReadAll ReaderAllCloser implementation
func (r *JoinReader) ReadAll() (buf []byte, err error) {
	buf = make([]byte, r.Len())
	_, err = io.ReadFull(r, buf)
	r.ReadCloser.Close()
	return
}

// Close ReaderAllCloser implementation
// Drain reader if any read.
func (r *JoinReader) Close() error {
	if r.started && r.Len() > 0 {
		buf := make([]byte, 1024)
		var err error
		for r.Len() > 0 && err == nil {
			r.Read(buf)
		}
	}
	r.ReadCloser.Close()
	return nil
}

func (r *JoinReader) join() {
	r.started = true
	go r.joiner(r.writer, r.data, r.size)
}
