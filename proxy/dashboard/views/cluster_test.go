package views

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	ui "github.com/gizak/termui/v3"
)

type DummpInstance struct {
	status uint64
}

func (ins *DummpInstance) Status() uint64 {
	return ins.status
}

var _ = Describe("Views", func() {
	It("should output correct color.", func() {
		view := &ClusterView{}
		ins := &DummpInstance{}
		// Expect(ins.Status()).To(Equal(uint64(0x0000)))
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorWhite))

		// ins.started = lambdastore.INSTANCE_STARTED
		ins.status = 0x0001
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorGreen))

		// ins.recovering = 10
		ins.status = 0x0101
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorCyan))

		// ins.recovering = 0
		// ins.backing = 2
		ins.status = 0x0201
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorBlue))

		// ins.recovering = 10
		ins.status = 0x0301
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorCyan))

		// ins.phase = lambdastore.PHASE_BACKING_ONLY
		ins.status = 0x1001
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorYellow))

		ins.status = 0x1301
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorCyan))

		// ins.phase = lambdastore.PHASE_RECLAIMED
		ins.status = 0x2001
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorRed))

		// ins.phase = lambdastore.PHASE_EXPIRED
		ins.status = 0x3001
		Expect(view.getColorByInstance(ins)).To(Equal(ui.ColorMagenta))
	})
})
