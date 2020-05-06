package views

import (
	ui "github.com/gizak/termui/v3"
)

type ClusterView struct {
	*ui.Block
}

func NewClusterView(title string) *ClusterView {
	view := &ClusterView{
		Block: ui.NewBlock(),
	}
	view.Title = title
	return view
}
