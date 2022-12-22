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

func (c *MockEnd) Close() error {
	c.status = "closed"
	c.parent.Invalid()
	return c.End.Close()
}
