package views

import (
	"image"
	"os/exec"

	ui "github.com/gizak/termui/v3"

	"github.com/gizak/termui/v3/widgets"
)

type LogView struct {
	*widgets.Paragraph
	logFile string
}

func NewLogView(title string, logFile string) *LogView {
	view := &LogView{
		Paragraph: widgets.NewParagraph(),
		logFile:   logFile,
	}
	view.Title = title
	return view
}

func (v *LogView) Draw(buf *ui.Buffer) {
	tail, err := exec.Command("tail", "-n", "10", v.logFile).Output()
	if err != nil {
		tail = []byte(err.Error())
	}
	v.Text = string(tail)

	// Draw overwrite
	v.Block.Draw(buf)

	cells := ui.ParseStyles(v.Text, v.TextStyle)
	if v.WrapText {
		cells = ui.WrapCells(cells, uint(v.Inner.Dx()))
	}

	rows := ui.SplitCells(cells, '\n')

	for y := 0; y < len(rows); y++ {
		if v.Inner.Max.Y-y-1 <= v.Inner.Min.Y {
			break
		}
		row := rows[len(rows)-y-1]
		row = ui.TrimCells(row, v.Inner.Dx())
		for _, cx := range ui.BuildCellWithXArray(row) {
			x, cell := cx.X, cx.Cell
			buf.SetCell(cell, image.Pt(x, v.Inner.Max.Y-v.Inner.Min.Y-y-1).Add(v.Inner.Min))
		}
	}
}
