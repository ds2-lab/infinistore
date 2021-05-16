package lifetime

import (
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/mason-leap-lab/infinicache/common/logger"
	protocol "github.com/mason-leap-lab/infinicache/common/types"
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

const BANDWIDTH_FACTOR = 25 // 1/bandwidth, while bandwidth = 40MB/s = 0.04B/ns for single connection
const STREAMING_TIMEOUT_FACTOR = 10

var (
	TICK_ERROR_EXTEND = TICK_10_ERROR_EXTEND
	TICK_ERROR        = TICK_10_ERROR

	ErrTimeout = errors.New("timeout")
)

func init() {
	// adapt
	if lambdacontext.MemoryLimitInMB == 0 {
		// Do nothing, default
	} else if lambdacontext.MemoryLimitInMB < 896 {
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

func TimeoutAfter(f func(), timeout time.Duration) error {
	_, err := TimeoutAfterWithReturn(func() (interface{}, error) {
		f()
		return nil, nil
	}, timeout)
	return err
}

func TimeoutAfterWithReturn(f func() (interface{}, error), timeout time.Duration) (rsp interface{}, err error) {
	timer := time.NewTimer(timeout)
	responeded := make(chan struct{})
	go func() {
		rsp, err = f()
		responeded <- struct{}{}
	}()
	select {
	case <-timer.C:
		err = ErrTimeout
	case <-responeded:
		if !timer.Stop() {
			<-timer.C
		}
	}
	return
}

func GetStreamingTimeout(size int64) time.Duration {
	timeout := time.Duration(size * BANDWIDTH_FACTOR * STREAMING_TIMEOUT_FACTOR)
	if timeout < time.Second {
		timeout = time.Second
	}
	return timeout
}

func GetStreamingDeadline(size int64) time.Time {
	return time.Now().Add(GetStreamingTimeout(size))
}

type Timeout struct {
	Confirm func(*Timeout) bool

	session       *Session
	startAt       time.Time
	interruptAt   time.Time
	interruptEnd  time.Time
	timer         *time.Timer
	lastExtension time.Duration
	lastReason    string
	reset         chan time.Duration
	resetReason   string
	log           logger.ILogger
	active        int32
	disabled      int32
	c             chan time.Time
	timeout       bool
	due           time.Duration
	deadline      time.Time
}

func NewTimeout(s *Session, d time.Duration) *Timeout {
	t := &Timeout{
		session:       s,
		lastExtension: d,
		log:           logger.NilLogger,
		reset:         make(chan time.Duration, 1),
		c:             make(chan time.Time, 1),
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

func (t *Timeout) StartWithDeadline(deadline time.Time) time.Time {
	t.deadline = deadline

	// Because timeout must be in seconds, we can calibrate the start time by ceil difference to seconds.
	lifeInSeconds := time.Duration(math.Ceil(float64(time.Until(deadline))/float64(time.Second))) * time.Second
	return t.StartWithCalibration(deadline.Add(-lifeInSeconds))
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

	t.ResetWithExtension(ext, "restart")
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

func (t *Timeout) ResetWithExtension(ext time.Duration, reason string) bool {
	t.lastExtension = ext
	t.lastReason = reason
	return t.Reset()
}

func (t *Timeout) SetLogger(log logger.ILogger) {
	t.log = log
}

func (t *Timeout) Busy(reason string) {
	// log.Println("busy")
	actives := atomic.AddInt32(&t.active, 1)
	t.lastReason = reason // Acknowledge the ping extension.
	t.log.Debug("Busy %s(%d)", reason, actives)
}

func (t *Timeout) DoneBusy(reason string) {
	// log.Println("done busy")
	actives := atomic.AddInt32(&t.active, -1)
	t.log.Debug("Done busy %s(%d)", reason, actives)
}

func (t *Timeout) DoneBusyWithReset(ext time.Duration, reason string) {
	// log.Printf("done busy with reset %v\n", ext)
	t.DoneBusy(reason)
	// Unacknowledged ping will be preserved.
	if t.lastReason != protocol.CMD_PING {
		t.ResetWithExtension(ext, reason)
	}
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
			t.log.Debug("Due extended to %v, timeout in %v: %s", due, timeout, t.resetReason)
		case <-t.timer.C:
			// Timeout channel should be empty, or we clear it
			select {
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

			// Pre-confirmation check
			if t.Confirm != nil && !t.tryTimeout(false) {
				continue
			}

			if t.Confirm == nil || t.Confirm(t) {
				// Confirmed or no need to confirm
				t.tryTimeout(true)
			}
		}
	}
}

func (t *Timeout) resetLocked() {
	_, t.due = t.getTimeout(t.lastExtension)
	t.log.Debug("Due expected at %v: %s", t.due, t.lastReason)
	t.resetReason = t.lastReason
	select {
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
	due = time.Duration(math.Ceil(float64(now+ext)/float64(TICK)))*TICK - TICK_ERROR
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
		t.log.Debug("Timeout triggered")
		t.c <- t.interruptAt
	}

	return true
}
