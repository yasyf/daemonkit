package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// DefaultEnsureInterval is EnsureCurrent's poll cadence when Interval is unset.
const DefaultEnsureInterval = 100 * time.Millisecond

// EnsureConfig drives EnsureCurrent.
type EnsureConfig struct {
	// Peer adapts whoever answers the socket. Required.
	Peer Peer
	// Protocol is the exact lifecycle protocol the target must report.
	Protocol int
	// LockPath is the start-serialization flock, distinct from the socket bind
	// lock, so only one caller drives a version transition at a time. Required.
	LockPath string
	// Ensure, when set, is invoked each poll to (re)spawn the target; it must be
	// idempotent — consumers wire proc.Spawn.EnsureRunning.
	Ensure func(ctx context.Context) error
	// Interval is the poll cadence; zero means DefaultEnsureInterval.
	Interval time.Duration
	// Timeout bounds the whole wait; zero means only ctx bounds it.
	Timeout time.Duration

	clock clock
}

// EnsureCurrent serializes through the start flock, then polls until the peer
// answering Health reports EXACTLY target. Reachability alone is not success — a
// retiring older daemon still answers — so it keeps polling until the reported
// Build and Protocol match the targets, returning ErrEnsureTimeout if the
// deadline elapses.
func EnsureCurrent(ctx context.Context, cfg EnsureConfig, target string) error {
	lockDeadline := cfg.Timeout
	if lockDeadline <= 0 {
		lockDeadline = 5 * time.Second
	}
	lock, err := (proc.FileLockSpec{
		Path:     cfg.LockPath,
		Mode:     proc.FileLockExclusive,
		Deadline: lockDeadline,
	}).Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire start lock %s: %w", cfg.LockPath, err)
	}
	defer lock.Close()

	clk := clockOrReal(cfg.clock)
	var deadline time.Time
	if cfg.Timeout > 0 {
		deadline = clk.Now().Add(cfg.Timeout)
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultEnsureInterval
	}
	for {
		if cfg.Ensure != nil {
			if err := cfg.Ensure(ctx); err != nil {
				return fmt.Errorf("ensure target running: %w", err)
			}
		}
		if h, err := cfg.Peer.Health(ctx); err == nil && h.Build == target && h.Protocol == cfg.Protocol {
			return nil
		}
		if !deadline.IsZero() && clk.Now().After(deadline) {
			return fmt.Errorf("%w: target %q after %s", ErrEnsureTimeout, target, cfg.Timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(interval):
		}
	}
}
