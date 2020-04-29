package types_test

import (
	mock "github.com/jordwest/mock-conn"
	"github.com/mason-leap-lab/redeo/resp"
	"log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
})
