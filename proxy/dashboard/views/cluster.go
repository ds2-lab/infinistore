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

var (
	DefaultRune   rune = '▣'
	ProgressRunes      = []rune{'䷁', '䷗', '䷒', '䷊', '䷡', '䷪', '䷀'}
)

type ClusterView struct {
	*ui.Canvas
	Cluster types.ClusterStats
	Cols    int
	Compact bool

	dash       DashControl
	origin     image.Point
	mapper     image.Point
	mapbase    int
	invalidate bool
}

func NewClusterView(dash DashControl, title string) *ClusterView {
	view := &ClusterView{
		Canvas:  ui.NewCanvas(),
		Compact: false,
		dash:    dash,
		origin:  image.Pt(0, 0),
		mapper:  image.Pt(2, 4),
	}
	view.Title = title
	return view
}

func NewClusterComponent(dash DashControl) *ClusterView {
	view := &ClusterView{
		Compact: true,
		Canvas:  ui.NewCanvas(),
		dash:    dash,
		origin:  image.Pt(0, 0),
		mapper:  image.Pt(2, 4),
	}
	view.Border = false
	return view
}

func (v *ClusterView) Update() {
	v.invalidate = true
}

func (v *ClusterView) UpdateInstance(idx int) {
	instance := v.Cluster.InstanceStats(idx)
	v.updateMapper(v.Cluster.InstanceLen())
	v.updateInstance(idx, instance)
}

func (v *ClusterView) SetCell(p image.Point, cell *drawille.Cell) {
	point := image.Pt(p.X/2, p.Y/4)
	cell.Rune -= drawille.BRAILLE_OFFSET
	v.Canvas.Canvas.CellMap[point] = *cell
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
	mapped := v.mapPoint(image.Pt(col, row))
	// Ignore points out of boundary
	if mapped.X < 0 || mapped.Y < 0 {
		return
	}
	v.SetCell(mapped, v.getCellByInstance(ins))
}

func (v *ClusterView) updateMapper(len int) {
	if v.mapbase == len {
		return
	}

	// log.Printf("reset mapper for len:%d", len)
	v.mapper.X = (v.Inner.Max.X - v.Inner.Min.X + 1) * 2 / (v.Cols + 1)
	if v.mapper.X < 4 {
		v.mapper.X = 4 // minimum recognizable interval: 1 character + 1 space
	}
	v.origin.X = v.Inner.Min.X + v.Inner.Max.X + 1 - v.mapper.X*v.Cols/2 // + v.mapper.X

	v.origin.Y = 0
	if !v.Compact {
		rows := int(math.Ceil(float64(len) / float64(v.Cols)))
		v.mapper.Y = (v.Inner.Max.Y - v.Inner.Min.Y + 1) * 4 / rows
		if v.mapper.Y < 4 {
			v.mapper.Y = 4 // minimum recognizable interval
		}
		v.origin.Y = -v.mapper.Y / 2
	}
	v.origin.Y += v.Inner.Max.Y*4 - 2 // reverse
	// log.Printf("Demension %v to %v: %v\n", v.Inner.Max, v.Inner.Min, v.mapper)

	v.mapbase = len
}

func (v *ClusterView) mapPoint(p image.Point) image.Point {
	ret := image.Pt(v.origin.X+v.mapper.X*p.X, v.origin.Y-v.mapper.Y*p.Y)
	// log.Printf("Map point from %v to %v\n", p, ret)
	return ret
}

func (v *ClusterView) getCellByInstance(ins types.InstanceStats) *drawille.Cell {
	if ins == types.InstanceStats(nil) {
		return &drawille.Cell{Rune: '▢', Color: drawille.Color(ui.ColorWhite)}
	}
	status := ins.Status()

	if status&lambdastore.INSTANCE_MASK_STATUS_START == lambdastore.INSTANCE_SHADOW {
		return &drawille.Cell{Rune: '▢', Color: drawille.Color(ui.ColorWhite)}
	}

	ret := &drawille.Cell{Color: drawille.Color(v.getColorByStatus(status))}
	if v.dash.GetOccupancyMode() == types.InstanceOccupancyDisabled {
		ret.Rune = DefaultRune
	} else {
		progress := int(math.Round(ins.Occupancy(v.dash.GetOccupancyMode()) * float64(len(ProgressRunes)-1)))
		if progress >= len(ProgressRunes) {
			progress = len(ProgressRunes) - 1
		}
		ret.Rune = ProgressRunes[progress]
	}
	return ret
}

func (v *ClusterView) getColorByStatus(status uint64) ui.Color {
	if status&lambdastore.INSTANCE_MASK_STATUS_FAILURE > 0 {
		return ui.ColorRed
	} else if status&lambdastore.INSTANCE_MASK_STATUS_START == lambdastore.INSTANCE_UNSTARTED {
		// Unstarted
		return ui.ColorWhite
	} else if backing := (status & lambdastore.INSTANCE_MASK_STATUS_BACKING >> 8); backing&lambdastore.INSTANCE_RECOVERING > 0 {
		// Recovering
		return ui.ColorCyan
	} else if backing&lambdastore.INSTANCE_BACKING > 0 {
		// Backing
		return ui.ColorBlue
	} else if phase := (status & lambdastore.INSTANCE_MASK_STATUS_LIFECYCLE >> 12); phase == lambdastore.PHASE_ACTIVE {
		// Active
		return ui.ColorGreen
	} else if phase == lambdastore.PHASE_BACKING_ONLY {
		// Backing only
		return ui.ColorYellow
	} else if phase >= lambdastore.PHASE_RECLAIMED {
		// Expired or reclaimed
		return ui.ColorRed
	} else {
		return ui.ColorMagenta
	}
}

func (v *ClusterView) Draw(buf *ui.Buffer) {
	v.invalidate = true
	v.update()
	v.Block.Draw(buf)
	var defaultRune rune
	for point, cell := range v.Canvas.Canvas.GetCells() {
		if point.In(v.Rectangle) {
			if cell.Rune == defaultRune {
				cell.Rune = DefaultRune
			}
			convertedCell := ui.Cell{
				// cell.Rune, see https://github.com/gizak/termui/blob/master/v3/symbols.go for options.
				// more Runes: https://en.wikipedia.org/wiki/List_of_Unicode_characters
				Rune: cell.Rune, // or ui.DOT
				// ui.IRREGULAR_BLOCKS[12],
				Style: ui.Style{
					Fg:       ui.Color(cell.Color),
					Bg:       ui.ColorClear,
					Modifier: ui.ModifierClear,
				},
			}
			buf.SetCell(convertedCell, point)
		}
	}
}

func (v *ClusterView) SetRect(x1, y1, x2, y2 int) {
	if v.Border {
		v.Canvas.SetRect(x1, y1, x2, y2)
	} else {
		v.Rectangle = image.Rect(x1, y1, x2, y2)
		v.Inner = image.Rect(x1, y1, x2, y2)
	}
}
