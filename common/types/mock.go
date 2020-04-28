package types

import (
	mock "github.com/jordwest/mock-conn"
)

var (
	Shortcut    []*mock.Conn
)

type ShortcutConnection []*mock.Conn

func InitShortcut(n int) ShortcutConnection {
	Shortcut = make(ShortcutConnection, n)
	for i := 0; i < n; i++ {
		Shortcut[i] = mock.NewConn()
	}
	return Shortcut
}

func (s ShortcutConnection) Close() {
	for i := 0; i < len(s); i++ {
		s[i].Server.Close()
		s[i].Client.Close()
	}
}
