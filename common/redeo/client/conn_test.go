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
		conn := NewConn(mockconn.Client)
		conn.WriteBulkString("test")
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		go func() {
			defer GinkgoRecover()

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

	// Mock connection didn't support timeout.
	// It("Should request timeout by default during sending request", func() {
	// 	net.InitShortcut()
	// 	shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
	// 	mockconn := shortcut.Validate(0).Conns[0]
	// 	conn := NewShortcut(mockconn)

	// 	req := NewRequest()
	// 	err := conn.StartRequest(req, func(_ Request) error {
	// 		conn.WriteBulkString("test")
	// 		return nil
	// 	})
	// 	Expect(err).To(Equal(context.DeadlineExceeded))

	// 	conn.Close()
	// })

	It("Should startRequest return err on failing to send", func() {
		net.InitShortcut()
		shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
		mockconn := shortcut.Validate(0).Conns[0]
		conn := NewConn(mockconn.Client)

		go func() {
			// consume the request
			<-time.After(1 * time.Second)
			conn.Close()
		}()

		req := NewRequest()
		err := conn.StartRequest(req, func(_ Request) error {
			conn.WriteBulkString("test")
			return nil
		})
		Expect(err).To(Equal(io.ErrClosedPipe))
	})

	It("Should request timeout by default after sent request", func() {
		net.InitShortcut()
		shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
		mockconn := shortcut.Validate(0).Conns[0]
		conn := NewConn(mockconn.Client)

		go func() {
			defer GinkgoRecover()

			// consume the request
			buff := make([]byte, 1024)
			n, err := mockconn.Server.Read(buff)
			Expect(err).To(BeNil())
			// $4\r\ntest\r\n
			Expect(n).To(Equal(10))
			Expect(string(buff[4:8])).To(Equal("test"))
		}()

		req := NewRequest()
		err := conn.StartRequest(req, func(_ Request) error {
			conn.WriteBulkString("test")
			return nil
		})
		Expect(err).To(BeNil())

		// wait for the response
		done := make(chan struct{})
		req.OnRespond(func(_ interface{}, err error) {
			Expect(err.Error()).To(Equal("i/o timeout"))
			conn.Close()
			close(done)
		})

		<-done

		conn.Close()
	})

	It("Should close the connection correctly", func() {
		net.InitShortcut()
		shortcut := net.Shortcut.Prepare("shortcut", 1, 1)
		conn := NewConn(shortcut.Validate(0).Conns[0].Client)
		req := NewRequest()
		conn.StartRequest(req)
		err := conn.Close()
		Expect(err).To(BeNil())
	})
})
