package views

import (
	"math"

	ui "github.com/gizak/termui/v3"

	// "log"

	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type GroupedClusterView struct {
	*ui.Block
	Views       []*ClusterView
	Cluster     types.GroupedClusterStats
	Cols        int
	ClusterCols int

	dash       DashControl
	invalidate bool
}

func NewGroupedClusterView(dash DashControl, title string) *GroupedClusterView {
	view := &GroupedClusterView{
		Block:       ui.NewBlock(),
		ClusterCols: 4,
		dash:        dash,
	}
	view.Title = title
	return view
}

func (v *GroupedClusterView) Update() {
	v.invalidate = true
}

func (v *GroupedClusterView) update() {
	if !v.invalidate {
		return
	}

	if v.Cluster == nil {
		return
	}

	views := v.Views
	if len(views) == 0 {
		return
	}
	iter := v.Cluster.AllClustersStats()
	base := len(views) - iter.Len()
	skip := 0
	if base < 0 {
		skip = -base
	}
	base = 0
	for iter.Next() {
		i, cluster := v.Cluster.ClusterStatsFromIterator(iter)
		if i >= skip {
			view := views[base+i-skip]
			view.Cluster = cluster
			view.Cols = v.ClusterCols
			view.Update()
		}
	}
	v.invalidate = false
}

// SetRect implements the Drawable interface.
func (v *GroupedClusterView) SetRect(x1, y1, x2, y2 int) {
	v.Block.SetRect(x1, y1, x2, y2)

	minWidth := v.ClusterCols * 2                      // ClusterCols + column space(ClusterCols - 1) + cluster space(1)
	cols := (v.Inner.Max.X - v.Inner.Min.X) / minWidth // max possible
	if cols == 0 || v.Cols < cols {
		cols = v.Cols
	}
	views := v.getViews(cols)
	width := float64(v.Inner.Max.X-v.Inner.Min.X) / float64(cols)
	for i := 0; i < cols; i++ {
		xBase := float64(v.Inner.Min.X) + width*float64(i)
		views[i].SetRect(int(math.Round(xBase)), v.Inner.Min.Y, int(math.Round(xBase+width)), v.Inner.Max.Y)
	}
}

func (v *GroupedClusterView) getViews(cols int) []*ClusterView {
	old := len(v.Views)
	if v.Views == nil || cols > cap(v.Views) {
		views := make([]*ClusterView, cols)
		copy(views[:old], v.Views)
		v.Views = views
	}
	v.Views = v.Views[:cols]
	for i := old; i < len(v.Views); i++ {
		v.Views[i] = NewClusterComponent(v.dash)
	}
	return v.Views
}

func (v *GroupedClusterView) Draw(buf *ui.Buffer) {
	v.invalidate = true
	v.update()
	v.Block.Draw(buf)
	for i := 0; i < len(v.Views); i++ {
		v.Views[i].Draw(buf)
	}
}
