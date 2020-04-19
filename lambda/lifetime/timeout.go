package lifetime

import (
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"math"
	"sync/atomic"
	"time"
	// "log"
)

const TICK = 100 * time.Millisecond
// For Lambdas below 0.5vCPU(896M).
const TICK_1_ERROR_EXTEND = 10000 * time.Millisecond
const TICK_1_ERROR = 10 * time.Millisecond
// For Lambdas with 0.5vCPU(896M) and above.
const TICK_5_ERROR_EXTEND = 1000 * time.Millisecond
const TICK_5_ERROR = 10 * time.Millisecond
// For Lambdas with 1vCPU(1792M) and above.
const TICK_10_ERROR_EXTEND = 1000 * time.Millisecond
const TICK_10_ERROR = 2 * time.Millisecond

var (
	TICK_ERROR_EXTEND = TICK_10_ERROR_EXTEND
	TICK_ERROR = TICK_10_ERROR
)

func init() {
	// adapt
	if lambdacontext.MemoryLimitInMB < 896 {
		TICK_ERROR_EXTEND = TICK_1_ERROR_EXTEND
		TICK_ERROR = TICK_1_ERROR
	} else if lambdacontext.MemoryLimitInMB < 1792 {
		TICK_ERROR_EXTEND = TICK_5_ERROR_EXTEND
		TICK_ERROR = TICK_5_ERROR
	} else {
		TICK_ERROR_EXTEND = TICK_10_ERROR_EXTEND
		TICK_ERROR = TICK_10_ERROR
	}
}

type Timeout struct {
	Confirm       func(*Timeout) bool

	session       *Session
	startAt       time.Time
	interruptAt   time.Time
	interruptEnd  time.Time
	timer         *time.Timer
	lastExtension time.Duration
	log           logger.ILogger
	active        int32
	disabled      int32
	reset         chan time.Duration
	c             chan time.Time
	timeout       bool
	due           time.Duration
}

func NewTimeout(s *Session, d time.Duration) *Timeout {
	t := &Timeout{
		session: s,
		lastExtension: d,
		log: logger.NilLogger,
		reset: make(chan time.Duration, 1),
		c: make(chan time.Time, 1),
	}
	timeout, due := t.getTimeout(d)
	t.due = due
	t.timer = time.NewTimer(timeout)
	go t.validateTimeout(s.done)
	return t
}

func (t *Timeout) Start() time.Time {
	return t.StartWithCalibration(time.Now())
}

func (t *Timeout) StartWithCalibration(startAt time.Time) time.Time {
	t.startAt = startAt
	return t.startAt
}

func (t *Timeout) EndInterruption() time.Time {
	t.interruptEnd = time.Now()
	return t.interruptEnd
}

func (t *Timeout) Since() time.Duration {
	return time.Since(t.startAt)
}

func (t *Timeout) Interrupted() time.Duration {
	if t.interruptEnd.After(t.interruptAt) {
		return t.interruptEnd.Sub(t.interruptAt)
	} else {
		return time.Since(t.interruptAt)
	}
}

func (t *Timeout) C() <-chan time.Time {
	return t.c
}

func (t *Timeout) Stop() {
	// Drain the timer to be accurate and safe to reset.
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
}

func (t *Timeout) Halt() {
	// Prevent timeout being triggered or reset after stopped
	t.Disable()

	t.Stop()
}

func (t *Timeout) Restart(ext time.Duration) {
	// Prevent timeout being triggered or reset after stopped
	t.Enable()

	t.ResetWithExtension(ext)
}

func (t *Timeout) Reset() bool {
	t.session.Lock()
	defer t.session.Unlock()

	if t.timeout || t.IsDisabled() {
		return false
	}

	if t.session.isDoneLocked() {
		return false
	}

	t.resetLocked()
	return true
}

func (t *Timeout) ResetWithExtension(ext time.Duration) bool {
	t.lastExtension = ext
	return t.Reset()
}

func (t *Timeout) SetLogger(log logger.ILogger) {
	t.log = log
}

func (t *Timeout) Busy() {
	// log.Println("busy")
	atomic.AddInt32(&t.active, 1)
}

func (t *Timeout) DoneBusy() {
	// log.Println("done busy")
	atomic.AddInt32(&t.active, -1)
}

func (t *Timeout) DoneBusyWithReset(ext time.Duration) {
	// log.Printf("done busy with reset %v\n", ext)
	atomic.AddInt32(&t.active, -1)
	t.ResetWithExtension(ext)
}

func (t *Timeout) IsBusy() bool {
	return atomic.LoadInt32(&t.active) > 0
}

// Disable timeout and returns false if timeout has been disabled already
func (t *Timeout) Disable() bool {
	return atomic.CompareAndSwapInt32(&t.disabled, 0, 1)
}

// Enable timeout and returns false if timeout has been enabled already
func (t *Timeout) Enable() bool {
	return atomic.CompareAndSwapInt32(&t.disabled, 1, 0)
}

func (t *Timeout) IsDisabled() bool {
	return atomic.LoadInt32(&t.disabled) > 0
}

func (t *Timeout) validateTimeout(done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case extension := <-t.reset:
			t.Stop()
			timeout, due := t.getTimeout(extension)
			t.due = due
			t.timer.Reset(timeout)
			t.log.Debug("Due expectation updated: %v, timeout in %v", due, timeout)
		case <-t.timer.C:
			// Timeout channel should be empty, or we clear it
			select{
			case <-t.c:
			default:
				// Nothing
			}

			// t.session.Lock()
			// // Double check timeout after locked.
			// if len(t.reset) > 0 || t.IsDisabled() {
			// 	// pass
			// } else if t.IsBusy() {
			// 	t.resetLocked()
			// } else if t.Since() < t.due {
			// 	// FIXME: This is just a precaution check, remove if possible.
			// 	t.log.Debug("Unexpected timeout before due (%v / %v), try reset.", t.Since(), t.due)
			// 	t.resetLocked()
			// } else if t.OnTimeout != nil && t.OnTimeout(t) { // Final chance to deny timeout.
			// 	t.c <- ti
			// 	t.timeout = true
			// 	t.OnTimeout = nil
			// 	t.interruptAt = time.Now()
			// }
			// t.session.Unlock()
			if !t.tryTimeout(false) {
				continue
			}

			if t.Confirm != nil && t.Confirm(t) {
				t.tryTimeout(true)
			}
		}
	}
}

func (t *Timeout) resetLocked() {
	_, t.due = t.getTimeout(t.lastExtension)
	t.log.Debug("Due expectation updated: %v", t.due)
	select{
	case t.reset <- t.lastExtension:
	default:
		// Consume unread and replace with latest.
		<-t.reset
		t.reset <- t.lastExtension
	}
}

func (t *Timeout) getTimeout(ext time.Duration) (timeout, due time.Duration) {
	if ext < 0 {
		timeout = 1 * time.Millisecond
		due = time.Since(t.startAt) + timeout
		return
	}

	now := time.Since(t.startAt)
	due = time.Duration(math.Ceil(float64(now + ext) / float64(TICK)))*TICK - TICK_ERROR
	timeout = due - now
	return
}

func (t *Timeout) tryTimeout(confirmed bool) bool {
	t.session.Lock()
	defer t.session.Unlock()

	// Double check timeout after locked.
	if len(t.reset) > 0 || t.IsDisabled() {
		return false
	} else if t.IsBusy() {
		t.resetLocked()
		return false
	} else if t.Since() < t.due {
		// FIXME: This is just a precaution check, remove if possible.
		t.log.Debug("Unexpected timeout before due (%v / %v), try reset.", t.Since(), t.due)
		t.resetLocked()
		return false
	} else if confirmed {
		t.timeout = true
		t.interruptAt = time.Now()
		t.c <- t.interruptAt
	}

	return true
}
