package types

import (
	"fmt"
	"regexp"

	mock "github.com/jordwest/mock-conn"
	"github.com/zhangjyr/hashmap"
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
	ports *hashmap.HashMap
}

func InitShortcut() *shortcut {
	if Shortcut == nil {
		Shortcut = &shortcut{
			ports: hashmap.New(200), // Concurrency * 2
		}
	}
	return Shortcut
}

func (s *shortcut) Prepare(addr string, id int, n int) *ShortcutConn {
	// To keep consistent of hash ring, the address must be recoverable and keep consistent.
	address := fmt.Sprintf(shortcutAddress, id, addr)
	conn, existed := s.ports.Get(address) // For specified id, GetOrInsert is not necessary.
	if !existed {
		newConn := NewShortcutConn(address, n)
		s.ports.Set(address, newConn)
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
	conn, existed := s.ports.Get(address)
	if !existed {
		return nil, false
	} else {
		return conn.(*ShortcutConn), true
	}
}

func (s *shortcut) Dial(address string) ([]*MockConn, bool) {
	conn, existed := s.ports.Get(address)
	if !existed {
		return nil, false
	} else {
		return conn.(*ShortcutConn).Validate().Conns, true
	}
}

func (s *shortcut) Invalidate(conn *ShortcutConn) {
	s.ports.Del(conn.Address)
}

type MockConn struct {
	*mock.Conn
	name string
}

func NewMockConn(addr string, idx int) *MockConn {
	return &MockConn{
		Conn: mock.NewConn(),
		name: fmt.Sprintf("%s[%d]", addr, idx),
	}
}

func (c *MockConn) String() string {
	return c.name
}

type ShortcutConn struct {
	Conns   []*MockConn
	Client  interface{}
	Address string
}

func NewShortcutConn(addr string, n int) *ShortcutConn {
	conn := &ShortcutConn{Conns: make([]*MockConn, n), Address: addr}
	for i := 0; i < n; i++ {
		conn.Conns[i] = NewMockConn(conn.Address, i)
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

func (cn *ShortcutConn) close(i int, conn *MockConn) {
	conn.Close()
	cn.Conns[i] = nil
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
		cn.Conns[i] = NewMockConn(cn.Address, i)
	}
}
