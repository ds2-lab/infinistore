package types

import (
	"fmt"
	"github.com/cornelk/hashmap"
	"regexp"
	mock "github.com/jordwest/mock-conn"
)

var (
	// Shortcut Registry for build-in shortcut connections.
	Shortcut    *shortcut

	// Concurency Estimate concurrency required.
	Concurrency = 100

	shortcutAddress = "shortcut:%d:%s"
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
		newConn := NewShortcutConn(n)
		newConn.Address = address
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

func (s *shortcut) Dial(address string) ([]*mock.Conn, bool)  {
	conn, existed := s.ports.Get(address)
	if !existed {
		return nil, false
	} else {
		return conn.(*ShortcutConn).Conns, true
	}
}

func (s *shortcut) Invalidate(conn *ShortcutConn) {
	s.ports.Del(conn.Address)
}

type ShortcutConn struct {
	Conns   []*mock.Conn
	Client  interface{}
	Address string
}

func NewShortcutConn(n int) *ShortcutConn {
	conn := &ShortcutConn{ Conns: make([]*mock.Conn, n) }
	for i := 0; i < n; i++ {
		conn.Conns[i] = mock.NewConn()
	}
	return conn
}
