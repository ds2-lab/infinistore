package views

import (
	"image"
	"math"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/drawille"

	// "log"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type ClusterView struct {
	*ui.Canvas
	Cluster    types.ClusterStats
	Cols       int
	origin     image.Point
	mapper     image.Point
	mapbase    int
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

func NewClusterComponent() *ClusterView {
	view := &ClusterView{
		Canvas: ui.NewCanvas(),
		origin: image.Pt(0, 0),
		mapper: image.Pt(1, 1),
	}
	view.Border = false
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
		v.CellMap = make(map[image.Point]drawille.Cell, len(v.CellMap)*2)
	}

	iter := v.Cluster.AllInstancesStats()
	v.updateMapper(iter.Len())
	for iter.Next() {
		v.updateInstance(v.Cluster.InstanceStatsFromIterator(iter))
	}
	v.invalidate = false
}

func (v *ClusterView) updateInstance(idx int, ins types.InstanceStats) {
	row := idx / v.Cols
	col := idx % v.Cols
	v.SetPoint(v.mapPoint(image.Pt(col, row)), v.getColorByInstance(ins))
}

func (v *ClusterView) updateMapper(len int) {
	if v.mapbase == len {
		return
	}

	// log.Printf("reset mapper for len:%d", len)
	v.mapper.X = (v.Inner.Max.X - v.Inner.Min.X + 1) * 2 / (v.Cols + 1)
	if v.mapper.X < 2 {
		v.mapper.X = 2 // minimum recognizable interval
	}
	v.origin.X = v.Inner.Min.X * 2 // + v.mapper.X

	rows := int(math.Ceil(float64(len) / float64(v.Cols)))
	v.mapper.Y = (v.Inner.Max.Y - v.Inner.Min.Y + 1) * 4 / (rows + 1)
	if v.mapper.Y > v.mapper.X {
		v.mapper.Y = v.mapper.X
	}
	if v.mapper.Y < 4 {
		v.mapper.Y = 4 // minimum recognizable interval
	}
	v.origin.Y = v.Inner.Max.Y * 4 - 2 // reverse
	// log.Printf("Demension %v to %v: %v\n", v.Inner.Max, v.Inner.Min, v.mapper)

	v.mapbase = len
}

func (v *ClusterView) mapPoint(p image.Point) image.Point {
	ret := image.Pt(v.origin.X+v.mapper.X*p.X, v.origin.Y-v.mapper.Y*p.Y)
	// log.Printf("Map point from %v to %v\n", p, ret)
	return ret
}

func (v *ClusterView) getColorByInstance(ins types.InstanceStats) ui.Color {
	if ins == nil {
		return ui.ColorWhite
	}
	status := ins.Status()

	// unstarted
	if status&lambdastore.INSTANCE_MASK_STATUS_START == lambdastore.INSTANCE_UNSTARTED {
		return ui.ColorWhite
	} else if backing := (status & lambdastore.INSTANCE_MASK_STATUS_BACKING >> 8); backing & lambdastore.INSTANCE_RECOVERING > 0 {
		return ui.ColorCyan
	} else if backing & lambdastore.INSTANCE_BACKING > 0 {
		return ui.ColorBlue
	} else if phase := (status & lambdastore.INSTANCE_MASK_STATUS_LIFECYCLE >> 12); phase == lambdastore.PHASE_ACTIVE {
		return ui.ColorGreen
	} else if phase == lambdastore.PHASE_BACKING_ONLY {
		return ui.ColorYellow
	} else if phase == lambdastore.PHASE_RECLAIMED {
		return ui.ColorRed
	} else {
		return ui.ColorMagenta
	}
}

func (v *ClusterView) Draw(buf *ui.Buffer) {
	v.update()
	v.Block.Draw(buf)
	for point, cell := range v.Canvas.Canvas.GetCells() {
		if point.In(v.Rectangle) {
			convertedCell := ui.Cell{
				// cell.Rune,
				ui.DOT,
				// ui.IRREGULAR_BLOCKS[12],
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

func (v *ClusterView) SetRect(x1, y1, x2, y2 int) {
	if v.Border == true {
		v.Canvas.SetRect(x1, y1, x2, y2)
	} else {
		v.Rectangle = image.Rect(x1, y1, x2, y2)
		v.Inner = image.Rect(x1, y1, x2, y2)
	}
}
