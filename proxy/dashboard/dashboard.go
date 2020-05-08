package dashboard

import (
	ui "github.com/gizak/termui/v3"
	"log"
	"time"

	"github.com/mason-leap-lab/infinicache/proxy/dashboard/views"
	"github.com/mason-leap-lab/infinicache/proxy/types"
	"github.com/mason-leap-lab/infinicache/proxy/global"
)

type Dashboard struct {
	*ui.Grid
	ClusterView   *views.GroupedClusterView
	LogView       *views.LogView
}

func NewDashboard() *Dashboard {
	if err := ui.Init(); err != nil {
		log.Panic("Failed to initialize dashboard: %v", err)
	}

	dashboard := &Dashboard{
		Grid: ui.NewGrid(),
		ClusterView: views.NewGroupedClusterView(" Nodes "),
		LogView: views.NewLogView(" Logs ", global.Options.LogFile),
	}

	// Full screen
	termWidth, termHeight := ui.TerminalDimensions()
	dashboard.Grid.SetRect(0, 0, termWidth, termHeight)

	// Layout
	dashboard.Grid.Set(
		ui.NewRow(1.0/3,
			ui.NewCol(1.0/1, dashboard.ClusterView),
		),
		ui.NewRow(2.0/3,
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

func (dash *Dashboard) ConfigCluster(cluster types.GroupedClusterStats, cols int) {
	dash.ClusterView.Cluster = cluster
	dash.ClusterView.Cols = cols
	dash.ClusterView.Update()
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
