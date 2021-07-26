package client

import (
	//"strings"
	//"testing"
	//"time"
	"github.com/mason-leap-lab/infinicache/common/net"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	It("Should close the connection correctly", func() {
		net.InitShortcut()
		shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
		conn := NewShortcut(shortcut.Validate(0).Conns[0])
		req := NewRequest()
		conn.StartRequest(req)
		err := conn.Close()
		Expect(err).To(BeNil())
	})
})
