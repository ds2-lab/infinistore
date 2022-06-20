package net

import (
	"fmt"
	"regexp"

	mock "github.com/jordwest/mock-conn"
	"github.com/mason-leap-lab/infinicache/common/util/hashmap"
)

var (
	// Shortcut Registry for build-in shortcut connections.
	Shortcut *shortcut

	// Concurency Estimate concurrency required.
	Concurrency = 100

	shortcutAddress    = "shortcut:%d:%s"
	shortcutRecognizer = regexp.MustCompile(`^shortcut:[0-9]+:(.+)$`)
)

type shortcut struct {
	ports hashmap.HashMap
}

func InitShortcut() *shortcut {
	if Shortcut == nil {
		Shortcut = &shortcut{
			ports: hashmap.NewMap(200), // Concurrency * 2
		}
	}
	return Shortcut
}

func (s *shortcut) Prepare(addr string, id int, nums ...int) *ShortcutConn {
	n := 1
	if len(nums) > 0 {
		n = nums[0]
	}
	if n < 1 {
		n = 1
	}

	// To keep consistent of hash ring, the address must be recoverable and keep consistent.
	address := fmt.Sprintf(shortcutAddress, id, addr)
	conn, existed := s.ports.Load(address) // For specified id, GetOrInsert is not necessary.
	if !existed {
		newConn := NewShortcutConn(address, n)
		s.ports.Store(address, newConn)
		return newConn
	} else {
		return conn.(*ShortcutConn)
	}
}

func (s *shortcut) Validate(address string) (string, bool) {
	match := shortcutRecognizer.FindStringSubmatch(address)
	if len(match) > 0 {
		return match[1], true
	} else {
		return "", false
	}
}

func (s *shortcut) GetConn(address string) (*ShortcutConn, bool) {
	conn, existed := s.ports.Load(address)
	if !existed {
		return nil, false
	} else {
		return conn.(*ShortcutConn), true
	}
}

func (s *shortcut) Dial(address string) ([]*MockConn, bool) {
	conn, existed := s.ports.Load(address)
	if !existed {
		return nil, false
	} else {
		return conn.(*ShortcutConn).Validate().Conns, true
	}
}

func (s *shortcut) Invalidate(conn *ShortcutConn) {
	s.ports.Delete(conn.Address)
}

type MockConn struct {
	*mock.Conn
	parent *ShortcutConn
	idx    int
}

func NewMockConn(scn *ShortcutConn, idx int) *MockConn {
	return &MockConn{
		Conn:   mock.NewConn(),
		parent: scn,
		idx:    idx,
	}
}

func (c *MockConn) String() string {
	return fmt.Sprintf("%s[%d]", c.parent.Address, c.idx)
}

func (c *MockConn) Close() error {
	return c.parent.close(c.idx, c)
}

func (c *MockConn) Invalid() {
	c.parent.Conns[c.idx] = nil
}

type ShortcutConn struct {
	Conns      []*MockConn
	Client     interface{}
	Address    string
	OnValidate func(*MockConn)
}

func NewShortcutConn(addr string, n int) *ShortcutConn {
	conn := &ShortcutConn{Address: addr}
	conn.OnValidate = conn.defaultValidateHandler
	if n == 1 {
		conn.Conns = []*MockConn{nil}
	} else {
		conn.Conns = make([]*MockConn, n)
	}

	return conn
}

func (cn *ShortcutConn) Close(idxes ...int) {
	if len(idxes) == 0 {
		for i, conn := range cn.Conns {
			cn.close(i, conn)
		}
	} else {
		for _, i := range idxes {
			cn.close(i, cn.Conns[i])
		}
	}
}

func (cn *ShortcutConn) close(i int, conn *MockConn) error {
	if conn != nil {
		cn.Conns[i] = nil
		return conn.Close()
	}

	return nil
}

func (cn *ShortcutConn) Validate(idxes ...int) *ShortcutConn {
	if len(idxes) == 0 {
		for i, conn := range cn.Conns {
			cn.validate(i, conn)
		}
	} else {
		for _, i := range idxes {
			cn.validate(i, cn.Conns[i])
		}
	}
	return cn
}

func (cn *ShortcutConn) validate(i int, conn *MockConn) {
	if conn == nil {
		cn.Conns[i] = NewMockConn(cn, i)
		cn.OnValidate(cn.Conns[i])
	}
}

func (cn *ShortcutConn) defaultValidateHandler(_ *MockConn) {
}
