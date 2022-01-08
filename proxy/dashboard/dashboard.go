package dashboard

import (
	"errors"
	"log"
	"time"

	ui "github.com/gizak/termui/v3"

	"github.com/mason-leap-lab/infinicache/proxy/dashboard/views"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

const (
	DONE_CLOSE = 0
	DONE_QUIT  = 1
)

var (
	ErrClosed = errors.New("dashboard closed")
)

type Dashboard struct {
	*ui.Grid
	ClusterView *views.ClusterView
	GroupedView *views.GroupedClusterView
	LogView     *views.LogView
	StatusView  *views.StatusView
	done        chan int
	lastError   error
	icMode      types.InstanceOccupancyMode
}

func NewDashboard() *Dashboard {
	if err := ui.Init(); err != nil {
		log.Panic(err)
	}

	dashboard := &Dashboard{
		Grid:    ui.NewGrid(),
		LogView: views.NewLogView(" Logs ", global.Options.LogFile),
		done:    make(chan int, 1),
	}
	dashboard.ClusterView = views.NewClusterView(dashboard, " Nodes ")
	dashboard.GroupedView = views.NewGroupedClusterView(dashboard, " Nodes ")
	dashboard.StatusView = views.NewStatusView(dashboard)
	// Full screen
	termWidth, termHeight := ui.TerminalDimensions()
	dashboard.Grid.SetRect(0, 0, termWidth, termHeight)

	// Layout
	dashboard.Grid.Set(
		ui.NewRow(16.0/20,
			ui.NewCol(1.0/1, dashboard.GroupedView),
		),
		ui.NewRow(3.0/20,
			ui.NewCol(1.0/1, dashboard.LogView),
		),
		ui.NewRow(1.0/20,
			ui.NewCol(1.0/1, dashboard.StatusView),
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
	switch stats := cluster.(type) {
	case types.ClusterStats:
		dash.Grid.Items[0].Entry = dash.ClusterView
		dash.ClusterView.Cluster = stats
		dash.ClusterView.Cols = 25
		dash.StatusView.Meta = stats.MetaStats()
		dash.ClusterView.Update()
	case types.GroupedClusterStats:
		dash.Grid.Items[0].Entry = dash.GroupedView
		dash.GroupedView.Cluster = stats
		dash.GroupedView.Cols = cols
		dash.StatusView.Meta = stats.MetaStats()
		dash.GroupedView.Update()
	default:
		log.Println("ConfigCluster(): Invalid cluster type")
	}
}

func (dash *Dashboard) Update() {
	ui.Render(dash)
}

func (dash *Dashboard) Start() error {
	uiEvents := ui.PollEvents()
	ticker := time.NewTicker(time.Second).C
	for {
		dash.Update()
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "q", "<C-c>":
				return nil
			case "m":
				dash.icMode = (dash.icMode + 1) % types.InstanceOccupancyMod
			case "<Resize>":
				payload := e.Payload.(ui.Resize)
				dash.SetRect(0, 0, payload.Width, payload.Height)
				ui.Clear()
				// ui.Render(dash)
			}
		case <-ticker:
			// ui.Render(dash)
		case code := <-dash.done:
			if code == DONE_QUIT {
				return dash.lastError
			} else {
				return ErrClosed
			}
		}
	}
}

func (dash *Dashboard) Close() {
	// avoid block
	select {
	case <-dash.done:
	default:
	}
	dash.done <- DONE_CLOSE
	ui.Close()
}

func (dash *Dashboard) Quit(reason string) {
	// avoid block
	select {
	case <-dash.done:
	default:
	}
	dash.lastError = errors.New(reason)
	dash.done <- DONE_QUIT
}

func (dash *Dashboard) GetOccupancyMode() types.InstanceOccupancyMode {
	return dash.icMode
}
