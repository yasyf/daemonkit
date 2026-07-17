package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// DefaultIdleTimeout is the inactivity window before IdleExit fires when Timeout
// is unset.
const DefaultIdleTimeout = 2 * time.Hour

// DefaultIdleInterval is IdleExit's idle-check cadence when Interval is unset.
const DefaultIdleInterval = time.Minute

// attachKey identifies one live attachment: a named consumer at a pid.
type attachKey struct {
	consumer string
	pid      int
}

// IdleExit fires a callback after Timeout of no activity, unless a veto suppresses
// it. Touch feeds from wire.Server.OnActivity; Attach/Detach track live consumers
// whose presence vetoes, and an attachment whose process has died stops vetoing.
// Exit is a callback, not os.Exit, so the timer is testable.
type IdleExit struct {
	// Timeout is the inactivity window before exit; zero means DefaultIdleTimeout.
	Timeout time.Duration
	// Veto, when set and returning true, suppresses exit (live leases or streams).
	Veto func() bool
	// Exit fires once idle; required.
	Exit func(ctx context.Context)
	// Interval is the idle-check cadence; zero means DefaultIdleInterval.
	Interval time.Duration

	mu       sync.Mutex
	last     time.Time
	attached map[attachKey]struct{}

	clock  clock
	prober prober
}

// Touch records activity now, resetting the idle window.
func (i *IdleExit) Touch() {
	i.mu.Lock()
	i.last = i.clk().Now()
	i.mu.Unlock()
}

// Attach registers a live consumer at pid whose presence vetoes idle exit.
func (i *IdleExit) Attach(consumer string, pid int) {
	i.mu.Lock()
	if i.attached == nil {
		i.attached = map[attachKey]struct{}{}
	}
	i.attached[attachKey{consumer, pid}] = struct{}{}
	i.last = i.clk().Now()
	i.mu.Unlock()
}

// Detach drops a consumer's attachment and counts as activity.
func (i *IdleExit) Detach(consumer string, pid int) {
	i.mu.Lock()
	delete(i.attached, attachKey{consumer, pid})
	i.last = i.clk().Now()
	i.mu.Unlock()
}

// Run checks idleness on the clock seam and calls Exit once the daemon has been
// idle past Timeout with no live attachment and no veto. It blocks until Exit
// fires or ctx is done.
func (i *IdleExit) Run(ctx context.Context) {
	i.mu.Lock()
	if i.last.IsZero() {
		i.last = i.clk().Now()
	}
	i.mu.Unlock()
	interval := i.interval()
	for {
		select {
		case <-ctx.Done():
			return
		case <-i.clk().After(interval):
		}
		if i.idle(i.clk().Now()) {
			i.Exit(ctx)
			return
		}
	}
}

// idle reports whether the daemon may exit at now: no live attachment (a dead
// consumer's attachment is pruned and stops vetoing), no veto, and the idle
// window elapsed.
func (i *IdleExit) idle(now time.Time) bool {
	i.mu.Lock()
	keys := make([]attachKey, 0, len(i.attached))
	for k := range i.attached {
		keys = append(keys, k)
	}
	last := i.last
	i.mu.Unlock()

	live := 0
	for _, k := range keys {
		if _, err := i.prb().probe(k.pid); errors.Is(err, proc.ErrNoProcess) {
			i.mu.Lock()
			delete(i.attached, k)
			i.mu.Unlock()
			continue
		}
		live++ // alive, or an Undetermined probe that fails closed and keeps vetoing
	}
	if live > 0 {
		return false
	}
	if i.Veto != nil && i.Veto() {
		return false
	}
	return now.Sub(last) >= i.timeout()
}

func (i *IdleExit) clk() clock  { return clockOrReal(i.clock) }
func (i *IdleExit) prb() prober { return proberOrSys(i.prober) }

func (i *IdleExit) timeout() time.Duration {
	if i.Timeout > 0 {
		return i.Timeout
	}
	return DefaultIdleTimeout
}

func (i *IdleExit) interval() time.Duration {
	if i.Interval > 0 {
		return i.Interval
	}
	return DefaultIdleInterval
}
