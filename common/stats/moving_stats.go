package stats

import "sync"

type StepStater func(stats StatsProvider, old float64, removed float64, val float64) (new float64)

type StatHandler int

type MovingStats struct {
	window         int64
	n              int64
	values         []float64
	last           int64
	stats          []float64
	staters        []StepStater
	defaultHandler StatHandler
	mu             sync.RWMutex
}

func NewMovingValue(window int64, newFactor float64, oldFactor float64) *MovingStats {
	stats := NewMovingStats(window)
	stats.defaultHandler = stats.AddStater(stats.GetWeightValueStater(newFactor, oldFactor))
	return stats
}

func NewMovingUnilateralValue(window int64, newFactor float64, oldFactor float64) *MovingStats {
	stats := NewMovingStats(window)
	stats.defaultHandler = stats.AddStater(stats.GetWeightUnilateralValueStater(newFactor, oldFactor))
	return stats
}

func NewMovingSum(window int64) *MovingStats {
	stats := NewMovingStats(window)
	stats.defaultHandler = stats.AddStater(stats.SumStater)
	return stats
}

func NewMovingStats(window int64) *MovingStats {
	return &MovingStats{
		window:  window,
		n:       0,
		values:  make([]float64, window),
		last:    0,
		stats:   make([]float64, 0, 1),
		staters: make([]StepStater, 0, 1),
	}
}

func (stats *MovingStats) AddStater(stater StepStater) StatHandler {
	stats.stats = append(stats.stats, 0.0)
	stats.staters = append(stats.staters, stater)
	return StatHandler(len(stats.stats) - 1)
}

func (stats *MovingStats) GetWeightValueStater(newFactor float64, oldFactor float64) StepStater {
	return func(_ StatsProvider, old float64, removed float64, val float64) (new float64) {
		if old == 0 {
			return val
		}
		return oldFactor*old + newFactor*val
	}
}

func (stats *MovingStats) GetWeightUnilateralValueStater(newFactor float64, oldFactor float64) StepStater {
	return func(_ StatsProvider, old float64, removed float64, val float64) (new float64) {
		if old == 0 {
			return val
		} else if val > old {
			return newFactor*val + oldFactor*old
		} else {
			return newFactor*old + oldFactor*val
		}
	}
}

func (stats *MovingStats) SumStater(_ StatsProvider, old float64, removed float64, val float64) (new float64) {
	return old - removed + val
}

func (stats *MovingStats) Add(val float64) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	// Move forward.
	stats.last = (stats.last + 1) % stats.window

	// Add difference to sum.
	for i, stater := range stats.staters {
		stats.stats[i] = stater(stats, stats.stats[i], stats.values[stats.last], val)
	}

	// Record history value
	stats.values[stats.last] = val

	// update length
	if stats.n < stats.window {
		stats.n += 1
	}
}

func (stats *MovingStats) Value() float64 {
	if len(stats.staters) == 0 {
		return 0
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	return stats.stats[stats.defaultHandler]
}

func (stats *MovingStats) Window() int64 {
	return stats.window
}

func (stats *MovingStats) N() int64 {
	if stats.n >= stats.window {
		return stats.window
	}

	stats.mu.RLock()
	defer stats.mu.RUnlock()

	return stats.n
}

func (stats *MovingStats) Last() float64 {
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	return stats.values[stats.last]
}

func (stats *MovingStats) LastN(n int64) float64 {
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	if n > stats.n {
		n = stats.n
	}
	return stats.values[(stats.last+stats.window-n)%stats.window]
}

func (stats *MovingStats) History() []float64 {
	return stats.values
}
