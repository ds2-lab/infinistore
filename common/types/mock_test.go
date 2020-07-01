package types_test

import (
	"fmt"
	mock "github.com/jordwest/mock-conn"
	"github.com/mason-leap-lab/redeo/resp"
	"log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/mason-leap-lab/infinicache/common/types"
)

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
})
