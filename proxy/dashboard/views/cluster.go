package views

import (
	"image"
	ui "github.com/gizak/termui/v3"
	"math"
	// "log"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

type ClusterView struct {
	*ui.Canvas
	Cluster types.ClusterStatus
	Rows int
	origin image.Point
	mapper image.Point
}

func NewClusterView(title string) *ClusterView {
	view := &ClusterView{
		Canvas: ui.NewCanvas(),
		origin: image.Pt(0, 0),
		mapper: image.Pt(1, 1),
	}
	view.Title = title
	return view
}

func (v *ClusterView) Update() {
	if v.Cluster == nil {
		return
	}

	v.updateMapper()
	for i := 0; i < v.Cluster.Len(); i++ {
		v.UpdateInstance(i)
	}
}

func (v *ClusterView) UpdateInstance(idx int) {
	row := idx % v.Rows
	col := idx / v.Rows
	instance := v.Cluster.InstanceStatus(idx)
	v.SetPoint(v.mapPoint(image.Pt(col, row)), v.getColorByStatus(instance.Status()))
}

func (v *ClusterView) updateMapper() {
	cols := int(math.Ceil(float64(v.Cluster.Len()) / float64(v.Rows)))
	v.mapper.X = (v.Inner.Max.X - v.Inner.Min.X) * 2 / (cols + 1)
	v.origin.X = v.mapper.X + v.Inner.Min.X * 2

	v.mapper.Y = (v.Inner.Max.Y - v.Inner.Min.Y) * 4 / (v.Rows + 1)
	if v.mapper.Y < 1 {
		v.mapper.Y = 1
	}
	v.origin.Y = v.mapper.Y + v.Inner.Min.Y * 4
	// log.Printf("Demension %v to %v: %v\n", v.Inner.Max, v.Inner.Min, v.mapper)
}

func (v *ClusterView) mapPoint(p image.Point) image.Point {
	ret := image.Pt(v.origin.X + v.mapper.X * p.X, v.origin.Y + v.mapper.Y * p.Y)
	// log.Printf("Map point from %v to %v\n", p, ret)
	return ret
}

func (v *ClusterView) getColorByStatus(status uint64) ui.Color {
	// unstarted
	if status & lambdastore.INSTANCE_MASK_STATUS_START == lambdastore.INSTANCE_UNSTARTED {
		return ui.ColorWhite
	} else {
		return ui.ColorGreen
	}
}

func (v *ClusterView) Draw(buf *ui.Buffer) {
	v.Block.Draw(buf)

	for point, cell := range v.Canvas.Canvas.GetCells() {
		if point.In(v.Rectangle) {
			convertedCell := ui.Cell{
				ui.IRREGULAR_BLOCKS[12],
				ui.Style{
					ui.Color(cell.Color),
					ui.ColorClear,
					ui.ModifierClear,
				},
			}
			buf.SetCell(convertedCell, point)
		}
	}
}
