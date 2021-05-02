package views

import (
	ui "github.com/gizak/termui/v3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
		status := uint64(0x0000)
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorWhite))

		// ins.started = lambdastore.INSTANCE_STARTED
		status = 0x0001
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorGreen))

		// ins.recovering = 10
		status = 0x0101
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorCyan))

		// ins.recovering = 0
		// ins.backing = 2
		status = 0x0201
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorBlue))

		// ins.recovering = 10
		status = 0x0301
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorCyan))

		// ins.phase = lambdastore.PHASE_BACKING_ONLY
		status = 0x1001
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorYellow))

		status = 0x1301
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorCyan))

		// ins.phase = lambdastore.PHASE_RECLAIMED
		status = 0x2001
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorRed))

		// ins.phase = lambdastore.PHASE_EXPIRED
		status = 0x3001
		Expect(view.getColorByStatus(status)).To(Equal(ui.ColorRed))
	})
})
