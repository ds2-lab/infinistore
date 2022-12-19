package stats

type StatsProvider interface {
	N() int64
	History() []float64
}

type SumStatsProvider interface {
	StatsProvider
	Sum() float64
}

type SquareStatsProvider interface {
	SumStatsProvider
	Sum2() float64
}
