package lifetime

import (
	"time"
)

var (
	// If immortal, the function will not be reset anytime.
	Immortal = true
)

// Lifetime defines how long a function can survive. It ticks across invocations.
type Lifetime struct {
	birthtime time.Time
	alive     bool
	expected  time.Duration
}

func New(expected time.Duration) *Lifetime {
	return &Lifetime{
		birthtime: time.Now(),
		alive:     true,
		expected:  expected,
	}
}

func (l *Lifetime) Id() int64 {
	return l.birthtime.UnixNano()
}

// Reset function's identification.
func (l *Lifetime) Reborn() {
	l.birthtime = time.Now()
	l.alive = true
}

// Only reset function's identification if the function has been dead.
func (l *Lifetime) RebornIfDead() {
	if !l.alive {
		l.Reborn()
	}
}

// Is it the time for function to dead.
func (l *Lifetime) IsTimeUp() bool {
	if Immortal {
		return false
	}
	return int64(time.Since(l.birthtime)) >= int64(l.expected)
}

// Set function as dead.
func (l *Lifetime) Rest() {
	l.alive = false
}
