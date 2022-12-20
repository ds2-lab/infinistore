package net_test

import (
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	mock "github.com/jordwest/mock-conn"
	"github.com/mason-leap-lab/redeo/resp"

	. "github.com/mason-leap-lab/infinicache/common/net"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client")
}

var (
	shortcutAddress = "shortcut:%d:%s"
)

var _ = Describe("Mock", func() {
	newResponseConn := func() (*resp.RequestReader, *resp.RequestWriter, resp.ResponseReader, resp.ResponseWriter) {
		conn := mock.NewConn()
		return resp.NewRequestReader(conn.Client), resp.NewRequestWriter(conn.Server),
			resp.NewResponseReader(conn.Client), resp.NewResponseWriter(conn.Server)
	}

	It("should mock connection works", func() {
		reqReader, reqWriter, rspReader, rspWriter := newResponseConn()
		// reqReader, reqWriter, _, _ := newResponseConn()
		go func() {
			reqWriter.WriteMultiBulkSize(1)
			reqWriter.WriteBulkString("ping")
			err := reqWriter.Flush()
			log.Println(err)
		}()

		cmd, _ := reqReader.ReadCmd(nil)
		Expect(cmd.Name).To(Equal("ping"))

		go func() {
			rspWriter.AppendInlineString("pong")
			rspWriter.Flush()
		}()

		type1, _ := rspReader.PeekType()
		Expect(type1).To(Not(Equal(resp.TypeError)))

		response, _ := rspReader.ReadInlineString()
		Expect(response).To(Equal("pong"))
	})

	It("should Validate recognize shortcut", func() {
		shortcut := InitShortcut()
		ip := "10.23.4.5"
		fullip := ip + ":6378"

		addr, ok := shortcut.Validate(fmt.Sprintf(shortcutAddress, 1, ip))
		Expect(ok).To(Equal(true))
		Expect(addr).To(Equal(ip))

		addr, ok = shortcut.Validate(fmt.Sprintf(shortcutAddress, 1, fullip))
		Expect(ok).To(Equal(true))
		Expect(addr).To(Equal(fullip))

		addr, ok = shortcut.Validate(fullip)
		Expect(ok).To(Equal(false))
		Expect(addr).To(Equal(""))
	})

	It("should mock end detected err on closing of another end", func() {
		// Read after closed.
		conn := mock.NewConn()
		conn.Server.Close()
		_, err := conn.Client.Read([]byte{0})
		Expect(err).To(Equal(io.EOF))

		// Read before closed
		conn = mock.NewConn()
		go func() {
			_, err := conn.Client.Read([]byte{0})
			Expect(err).To(Equal(io.ErrClosedPipe))
		}()
		<-time.After(100 * time.Millisecond)
		conn.Server.Close()
	})
})
