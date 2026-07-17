package drain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// DefaultScanInterval is ScanLoop's cadence when Interval is unset.
const DefaultScanInterval = time.Minute

// ScanConfig drives the canonical daemon's death scans over drain generations.
type ScanConfig struct {
	// Dotdir roots the drain layout; generations live at Dotdir/drain/<gen>.
	Dotdir string
	// Canonical is the ownership journal adopted rows land in.
	Canonical Journal
	// Intake gates adoptions against a concurrent drain transition: AdoptDead
	// admits through it and refuses with ErrDraining once it closes. Required.
	Intake *Intake
	// Interval is ScanLoop's cadence; zero means DefaultScanInterval.
	Interval time.Duration
	// Backoff spaces per-generation retries after a failed adoption; the zero
	// value uses a default.
	Backoff proc.Backoff
	// Log receives scan diagnostics; nil uses slog.Default.
	Log *slog.Logger

	prober prober
	clock  clock
}

func (cfg ScanConfig) interval() time.Duration {
	if cfg.Interval > 0 {
		return cfg.Interval
	}
	return DefaultScanInterval
}

func (cfg ScanConfig) backoff() proc.Backoff {
	if cfg.Backoff != (proc.Backoff{}) {
		return cfg.Backoff
	}
	return defaultBackoff
}

func (cfg ScanConfig) log() *slog.Logger {
	if cfg.Log != nil {
		return cfg.Log
	}
	return slog.Default()
}

// ScanPeers runs one complete death scan, adopting every proven-dead
// generation. The canonical daemon runs it before accepting registrations
// (e.g. wire.Server.BootReconcile) — the cold-start hole — and periodically after.
func ScanPeers(ctx context.Context, cfg ScanConfig) error {
	return scanOnce(ctx, cfg, nil, time.Time{})
}

// ScanLoop runs ScanPeers on Interval with per-generation breakers, so one
// failing peer never suppresses another's adoption. It blocks until ctx ends.
func ScanLoop(ctx context.Context, cfg ScanConfig) error {
	clk := clockOrReal(cfg.clock)
	brk := NewBreakers(cfg.backoff())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-clk.After(cfg.interval()):
		}
		if err := scanOnce(ctx, cfg, brk, clk.Now()); err != nil {
			cfg.log().Error("drain: scan peers", "err", err)
		}
	}
}

// scanOnce adopts only on a complete successful death scan: an enumeration
// error adopts nothing, and an Undetermined peer stays unadopted.
func scanOnce(ctx context.Context, cfg ScanConfig, brk *Breakers, now time.Time) error {
	gens, err := Generations(cfg.Dotdir)
	if err != nil {
		return fmt.Errorf("drain: %w", err)
	}
	log := cfg.log()
	var errs []error
	for _, g := range gens {
		if brk != nil && !brk.Allow(g.Name(), now) {
			continue
		}
		switch cfg.assess(g) {
		case Alive:
			continue
		case Undetermined:
			log.Warn("drain: peer undetermined; not adopting", "gen", g.Name())
			continue
		case Dead:
			if err := AdoptDead(ctx, cfg.Intake, cfg.Canonical, g); err != nil {
				if errors.Is(err, ErrDraining) {
					log.Info("drain: draining; adoption deferred", "gen", g.Name())
					continue
				}
				errs = append(errs, err)
				if brk != nil {
					brk.Fail(g.Name(), now)
				}
				continue
			}
			log.Info("drain: adopted dead generation", "gen", g.Name())
			if brk != nil {
				brk.OK(g.Name())
			}
		}
	}
	return errors.Join(errs...)
}

// assess maps a generation to its owner's liveness; an unreadable owner record
// is Undetermined, never Dead.
func (cfg ScanConfig) assess(g Generation) Liveness {
	id, err := g.ReadOwner()
	if err != nil {
		return Undetermined
	}
	return assess(proberOrSys(cfg.prober), id)
}

// AdoptDead replays g's pending rows into canonical at the next seq — a stale
// replay no-ops per CAS — then removes g. The canonical write is admitted
// through intake, so a draining canonical refuses with ErrDraining and leaves g
// intact for the successor. Callers must hold death proof; ScanPeers gates it
// on a revalidated identity.
func AdoptDead(ctx context.Context, intake *Intake, canonical Journal, g Generation) error {
	done, err := intake.Admit()
	if err != nil {
		return fmt.Errorf("drain: adopt %s: %w", g.Name(), err)
	}
	defer done()
	rows, err := g.Journal().Rows(ctx)
	if err != nil {
		return fmt.Errorf("drain: read %s journal: %w", g.Name(), err)
	}
	adopt := make([]Row, 0, len(rows))
	for _, r := range rows {
		if r.State != RowPending {
			continue
		}
		adopt = append(adopt, Row{Key: r.Key, Seq: r.Seq + 1, State: RowPending})
	}
	sort.Slice(adopt, func(a, b int) bool { return adopt[a].Key < adopt[b].Key })
	if len(adopt) > 0 {
		if _, err := canonical.Apply(ctx, adopt...); err != nil {
			return fmt.Errorf("drain: adopt %s: %w", g.Name(), err)
		}
	}
	if err := g.Remove(); err != nil {
		return fmt.Errorf("drain: remove %s: %w", g.Name(), err)
	}
	return nil
}
