package client

import (
	"io"
	"reflect"
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
type ByteJoiner func(io.Writer, [][]byte, int) error

// JoinReader A ReadAllCloser implementation that can concat an array of []bytes
type JoinReader struct {
	io.Reader
	io.Closer
	writer  io.Writer
	data    interface{}
	read    int
	size    int
	once    sync.Once
	joiner  interface{}
	started bool
}

func NewJoinReader(data interface{}, size int) *JoinReader {
	return &JoinReader{
		data: data,
		size: size,
	}
}

// NewByteJoinReader Create the join reader for []byte array
func NewByteJoinReader(data [][]byte, size int, joiner ByteJoiner) *JoinReader {
	reader, writer := io.Pipe()
	return &JoinReader{
		Reader: reader,
		Closer: reader,
		writer: writer,
		data:   data,
		size:   size,
		joiner: joiner,
	}
}

// Read io.Reader implementation
func (r *JoinReader) Read(p []byte) (n int, err error) {
	r.once.Do(r.join)
	n, err = r.Reader.Read(p)
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
	r.Close()
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
	if r.Closer != nil {
		r.Closer.Close()
	}
	return nil
}

func (r *JoinReader) join() {
	r.started = true
	switch joiner := r.joiner.(type) {
	case ByteJoiner:
		go joiner(r.writer, r.data.([][]byte), r.size)
	default:
		val := reflect.ValueOf(r.data)
		readers := make([]io.Reader, val.Len())
		for i := 0; i <= len(readers); i++ {
			readers[i] = val.Index(i).Interface().(io.Reader)
		}
		r.Reader = io.MultiReader(readers...)
	}
}
