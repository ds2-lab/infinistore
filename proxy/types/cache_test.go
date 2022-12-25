package types

// No test case here. We offer some dummy cache implementation for other tests.

type testPersistChunkForResponse struct {
	size   int
	stored int
}

func newEmptyTestPersistChunkForResponse(size int) *testPersistChunkForResponse {
	return newTestPersistChunkForResponse(size, 0)
}

func newTestPersistChunkForResponse(size int, stored int) *testPersistChunkForResponse {
	return &testPersistChunkForResponse{size, stored}
}

func (c *testPersistChunkForResponse) IsStored() bool {
	return c.stored == c.size
}

// BytesStored returns the number of bytes stored in the chunk.
func (c *testPersistChunkForResponse) BytesStored() int64 {
	return int64(c.stored)
}
