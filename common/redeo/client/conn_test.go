package client

import (
	//"strings"
	//"testing"
	//"time"

	"io"
	"time"

	"github.com/mason-leap-lab/infinicache/common/net"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	It("Should dummy connection connected", func() {
		net.InitShortcut()
		shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
		mockconn := shortcut.Validate(0).Conns[0]
		conn := NewShortcut(mockconn)
		conn.WriteBulkString("test")
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		go func() {
			Expect(conn.Flush()).To(BeNil())
		}()

		buff := make([]byte, 1024)
		n, err := mockconn.Server.Read(buff)
		Expect(err).To(BeNil())
		// $4\r\ntest\r\n
		Expect(n).To(Equal(10))
		Expect(string(buff[4:8])).To(Equal("test"))

		conn.Close()
		_, err = mockconn.Server.Read(buff)
		Expect(err).To(Equal(io.EOF))
	})

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
