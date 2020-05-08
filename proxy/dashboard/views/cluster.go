package views

import (
	"image"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/drawille"
	"math"
	// "log"

	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
)

type ClusterView struct {
	*ui.Canvas
	Cluster types.ClusterStats
	Rows    int
	origin  image.Point
	mapper  image.Point
	mapbase int
	invalidate bool
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
	v.invalidate = true
}


func (v *ClusterView) UpdateInstance(idx int) {
	instance := v.Cluster.InstanceStats(idx)
	v.updateMapper(v.Cluster.Len())
	v.updateInstance(idx, instance)
}

func (v *ClusterView) update() {
	if !v.invalidate {
		return
	}

	if v.Cluster == nil {
		return
	}

	if len(v.CellMap) > 0 {
		v.CellMap = make(map[image.Point]drawille.Cell, len(v.CellMap) * 2)
	}

	iter := v.Cluster.AllInstancesStats()
	v.updateMapper(iter.Len())
	for iter.Next() {
		v.updateInstance(v.Cluster.InstanceStatsFromIterator(iter))
	}
	v.invalidate = false
}

func (v *ClusterView) updateInstance(idx int, ins types.InstanceStats) {
	row := idx % v.Rows
	col := idx / v.Rows
	v.SetPoint(v.mapPoint(image.Pt(col, row)), v.getColorByInstance(ins))
}

func (v *ClusterView) updateMapper(len int) {
	if v.mapbase == len {
		return
	}

	// log.Printf("reset mapper for len:%d", len)
	cols := int(math.Ceil(float64(len) / float64(v.Rows)))
	v.mapper.X = (v.Inner.Max.X - v.Inner.Min.X) * 2 / (cols + 1)
	v.origin.X = v.mapper.X + v.Inner.Min.X * 2

	v.mapper.Y = (v.Inner.Max.Y - v.Inner.Min.Y) * 4 / (v.Rows + 1)
	if v.mapper.Y < 1 {
		v.mapper.Y = 1
	}
	v.origin.Y = v.mapper.Y + v.Inner.Min.Y * 4
	// log.Printf("Demension %v to %v: %v\n", v.Inner.Max, v.Inner.Min, v.mapper)

	v.mapbase = len
}

func (v *ClusterView) mapPoint(p image.Point) image.Point {
	ret := image.Pt(v.origin.X + v.mapper.X * p.X, v.origin.Y + v.mapper.Y * p.Y)
	// log.Printf("Map point from %v to %v\n", p, ret)
	return ret
}

func (v *ClusterView) getColorByInstance(ins types.InstanceStats) ui.Color {
	if ins == nil {
		return ui.ColorWhite
	}
	status := ins.Status()

	// unstarted
	if status & lambdastore.INSTANCE_MASK_STATUS_START == lambdastore.INSTANCE_UNSTARTED {
		return ui.ColorWhite
	} else {
		return ui.ColorGreen
	}
}

func (v *ClusterView) Draw(buf *ui.Buffer) {
	v.update()
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
