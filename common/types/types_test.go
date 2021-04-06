package types_test

import (
	"encoding/json"
	"testing"

	protocol "github.com/mason-leap-lab/infinicache/common/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTypes(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Types")
}

var _ = Describe("Types", func() {
	It("should InputEvent be successfully encode/decode the Status", func() {
		var input protocol.InputEvent
		input.Cmd = "warmup"
		input.Status = protocol.Status{protocol.Meta{
			Term: 1,
		}}

		data, err := json.Marshal(input)
		Expect(err).To(BeNil())

		var output protocol.InputEvent
		err = json.Unmarshal(data, &output)
		Expect(err).To(BeNil())
		Expect(output.Status).To(Not(BeNil()))
		Expect(len(output.Status)).To(Equal(1))
		Expect(output.Status[0].Term).To(Equal(uint64(1)))
	})

	It("should Status be successfully encode/decode", func() {
		input := protocol.Status{protocol.Meta{
			Term: 1,
		}}

		data, err := json.Marshal(input)
		Expect(err).To(BeNil())

		var output protocol.Status
		err = json.Unmarshal(data, &output)
		Expect(err).To(BeNil())
		Expect(output).To(Not(BeNil()))
		Expect(len(output)).To(Equal(1))
		Expect(output[0].Term).To(Equal(uint64(1)))
	})
})
