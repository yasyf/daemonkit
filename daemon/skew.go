package daemon

import (
	"context"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/version"
)

// DefaultSkewInterval is SkewWatch's tick cadence when Interval is unset.
const DefaultSkewInterval = 30 * time.Second

// SkewConfig drives a SkewWatch.
type SkewConfig struct {
	// Running reports the daemon's own running version; consumers wire version.Running.
	Running func() string
	// Installed reports the on-disk artifact version; consumers wire bundle.ShortVersion.
	Installed func() (string, error)
	// OnSkew fires once a newer artifact is confirmed installed; the drain engine
	// wires in here. Required.
	OnSkew func(ctx context.Context) error
	// Interval is the tick cadence; zero means DefaultSkewInterval.
	Interval time.Duration
	// Confirmations is the consecutive skew observations required before firing;
	// zero or one fires on the first observation.
	Confirmations int
	// Strikes, when set, storm-gates firing: once the strike budget is spent
	// within its window, OnSkew is suppressed until the window rolls off.
	Strikes *proc.Strikes

	clock clock
}

// SkewWatch is the incumbent-initiated version-skew watcher.
type SkewWatch struct {
	cfg    SkewConfig
	consec int
}

// NewSkewWatch builds a SkewWatch from cfg.
func NewSkewWatch(cfg SkewConfig) *SkewWatch { return &SkewWatch{cfg: cfg} }

// Run ticks on the clock seam, comparing the running version against the
// installed artifact, and fires OnSkew once a newer artifact is confirmed the
// required number of consecutive ticks and the strike budget allows. It blocks
// until ctx is done or OnSkew returns an error.
func (w *SkewWatch) Run(ctx context.Context) error {
	clk := clockOrReal(w.cfg.clock)
	interval := w.cfg.Interval
	if interval <= 0 {
		interval = DefaultSkewInterval
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(interval):
		}
		if err := w.tick(ctx, clk.Now()); err != nil {
			return err
		}
	}
}

// tick evaluates one skew observation at now, firing OnSkew when a newer
// artifact has been confirmed enough consecutive ticks and the strike budget
// permits.
func (w *SkewWatch) tick(ctx context.Context, now time.Time) error {
	run := w.cfg.Running()
	inst, err := w.cfg.Installed()
	if err != nil {
		w.consec = 0
		return nil // cannot read the artifact this tick: not a skew, not fatal
	}
	if !version.Newer(inst, run) {
		w.consec = 0
		return nil
	}
	w.consec++
	if w.consec < w.confirmations() {
		return nil
	}
	if w.cfg.Strikes != nil && w.cfg.Strikes.Struck(now) {
		return nil // breaker tripped: suppress until the window rolls off
	}
	if w.cfg.Strikes != nil {
		w.cfg.Strikes.Strike(now)
	}
	w.consec = 0
	if err := w.cfg.OnSkew(ctx); err != nil {
		return fmt.Errorf("on skew: %w", err)
	}
	return nil
}

func (w *SkewWatch) confirmations() int {
	if w.cfg.Confirmations > 1 {
		return w.cfg.Confirmations
	}
	return 1
}
