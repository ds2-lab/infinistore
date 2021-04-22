package dashboard

import (
	"log"
	"time"

	ui "github.com/gizak/termui/v3"

	"github.com/mason-leap-lab/infinicache/proxy/dashboard/views"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type Dashboard struct {
	*ui.Grid
	ClusterView *views.ClusterView
	GroupedView *views.GroupedClusterView
	LogView     *views.LogView
}

func NewDashboard() *Dashboard {
	if err := ui.Init(); err != nil {
		log.Panic(err)
	}

	dashboard := &Dashboard{
		Grid:        ui.NewGrid(),
		ClusterView: views.NewClusterView(" Nodes "),
		GroupedView: views.NewGroupedClusterView(" Nodes "),
		LogView:     views.NewLogView(" Logs ", global.Options.LogFile),
	}
	// Full screen
	termWidth, termHeight := ui.TerminalDimensions()
	dashboard.Grid.SetRect(0, 0, termWidth, termHeight)

	// Layout
	dashboard.Grid.Set(
		ui.NewRow(4.0/5,
			ui.NewCol(1.0/1, dashboard.GroupedView),
		),
		ui.NewRow(1.0/5,
			ui.NewCol(1.0/1, dashboard.LogView),
		),
	)

	return dashboard
}

// func (dash *Dashboard) ConfigCluster(cluster types.ClusterStats, cols int) {
// 	dash.ClusterView.Cluster = cluster
// 	dash.ClusterView.Cols = cols
// 	dash.ClusterView.Update()
// }

func (dash *Dashboard) ConfigCluster(cluster interface{}, cols int) {
	switch cluster.(type) {
	case types.ClusterStats:
		dash.Grid.Items[0].Entry = dash.ClusterView
		dash.ClusterView.Cluster = cluster.(types.ClusterStats)
		dash.ClusterView.Cols = 25
		dash.ClusterView.Update()
	case types.GroupedClusterStats:
		dash.Grid.Items[0].Entry = dash.GroupedView
		dash.GroupedView.Cluster = cluster.(types.GroupedClusterStats)
		dash.GroupedView.Cols = cols
		dash.GroupedView.Update()
	default:
		log.Println("ConfigCluster(): Invalid cluster type")
	}
}

func (dash *Dashboard) Update() {
	ui.Render(dash)
}

func (dash *Dashboard) Start() {
	uiEvents := ui.PollEvents()
	ticker := time.NewTicker(time.Second).C
	for {
		dash.Update()
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "q", "<C-c>":
				return
			case "<Resize>":
				payload := e.Payload.(ui.Resize)
				dash.SetRect(0, 0, payload.Width, payload.Height)
				ui.Clear()
				// ui.Render(dash)
			}
		case <-ticker:
			// ui.Render(dash)
		}
	}
}

func (dash *Dashboard) Close() {
	ui.Close()
}
