package net

import (
	"fmt"

	mock "github.com/jordwest/mock-conn"
)

type MockEnd struct {
	*mock.End
	parent *MockConn
	status string
}

func (c *MockEnd) String() string {
	if len(c.status) == 0 {
		return c.parent.String()
	} else {
		return fmt.Sprintf("%v(%s)", c.parent, c.status)
	}
}

func (c *MockEnd) Status() string {
	return c.status
}

func (c *MockEnd) Close() (err error) {
	return c.CloseWithReason("closed")
}

func (c *MockEnd) CloseWithReason(reason string) (err error) {
	if c.parent.invalid() {
		// End that first closes
		c.status = reason
	} else {
		// End that second closes
		c.setStatus("dropped")
	}
	c.End.Writer.Close()
	err = c.End.Reader.Close()
	return
}

func (c *MockEnd) setStatus(reason string) {
	if len(c.status) == 0 {
		c.status = reason
	}
}
