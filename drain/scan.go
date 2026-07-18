package drain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
	active, activeOK, err := cfg.Canonical.activeTransition(ctx)
	if err != nil {
		return err
	}
	if activeOK {
		found := false
		for _, g := range gens {
			if g.Name() == active.Generation {
				found = true
				break
			}
		}
		if !found && assess(proberOrSys(cfg.prober), active.Owner.identity()) == Dead {
			if err := cfg.Canonical.releaseTransition(ctx, active.Generation, active.Owner.identity()); err != nil {
				return fmt.Errorf("drain: release abandoned transition %s: %w", active.Generation, err)
			}
			activeOK = false
		}
	}
	log := cfg.log()
	probe := proberOrSys(cfg.prober)
	var errs []error
	for _, g := range gens {
		if brk != nil && !brk.Allow(g.Name(), now) {
			continue
		}
		owner, err := g.ReadOwner()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				reclaimed, rerr := reclaimOwnerless(ctx, cfg, g)
				switch {
				case rerr != nil:
					errs = append(errs, fmt.Errorf("drain: reclaim ownerless %s: %w", g.Name(), rerr))
				case reclaimed:
					log.Info("drain: reclaimed ownerless generation", "gen", g.Name())
				}
				continue
			}
			log.Warn("drain: peer undetermined; not adopting", "gen", g.Name())
			continue
		}
		if activeOK && active.Generation == g.Name() && active.Owner.identity() != owner {
			log.Warn("drain: transition owner mismatch; not adopting", "gen", g.Name())
			continue
		}
		switch assess(probe, owner) {
		case Alive:
			continue
		case Undetermined:
			log.Warn("drain: peer undetermined; not adopting", "gen", g.Name())
			continue
		case Dead:
			if err := AdoptDead(ctx, cfg, g, owner); err != nil {
				if errors.Is(err, ErrDraining) {
					log.Info("drain: draining; adoption deferred", "gen", g.Name())
					continue
				}
				if errors.Is(err, errAdoptionRaced) {
					log.Info("drain: adoption raced a reused generation; skipped", "gen", g.Name())
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

// errAdoptionRaced means the owner re-read under the root lock is no longer
// proven dead: a reused generation name changed owners between the advisory
// scan and the locked adoption.
var errAdoptionRaced = errors.New("adoption raced a live owner")

// AdoptDead proves g's owner dead and adopts it in one critical section under
// the drain root lock: the owner is re-read under the lock and must match
// expected — the identity the advisory scan proved dead — then is re-assessed,
// so a generation name reused by a different owner between the advisory scan
// and the adoption is refused instead of torn down; the handle binds to the
// observed incarnation for every subsequent op. Pending rows replay into
// canonical strictly above every issued seq (Bump's advancement rule); g is
// removed only once canonical is proven strictly newer. At sequence saturation, death proof transfers the exact row
// and deletes the generation's representation before removal. The canonical
// write is admitted through intake, so a draining canonical refuses with
// ErrDraining and leaves g intact for the successor. A second adopter arriving
// after removal fails with os.ErrNotExist rather than resurrecting the
// directory.
func AdoptDead(ctx context.Context, cfg ScanConfig, g Generation, expected proc.Identity) error {
	done, err := cfg.Intake.Admit()
	if err != nil {
		return fmt.Errorf("drain: adopt %s: %w", g.Name(), err)
	}
	defer done()
	canonical := cfg.Canonical
	lock, err := proc.Flock(ctx, g.rootLock())
	if err != nil {
		return fmt.Errorf("drain: lock drain root for %s: %w", g.Name(), err)
	}
	defer lock.Release()
	rec, err := readOwnerFile(g.ownerPath())
	if err != nil {
		return fmt.Errorf("drain: read %s owner: %w", g.Name(), err)
	}
	owner := rec.identity()
	if owner != expected {
		return fmt.Errorf("drain: adopt %s: owner changed since scan: %w", g.Name(), errAdoptionRaced)
	}
	if assess(proberOrSys(cfg.prober), owner) != Dead {
		return fmt.Errorf("drain: adopt %s: %w", g.Name(), errAdoptionRaced)
	}
	g.inc = rec.Inc
	rows, err := g.journal().rowsUnlocked()
	if err != nil {
		return fmt.Errorf("drain: read %s journal: %w", g.Name(), err)
	}
	pending := make([]Row, 0, len(rows))
	for _, r := range rows {
		if r.State == RowPending {
			pending = append(pending, r)
		}
	}
	sort.Slice(pending, func(a, b int) bool { return pending[a].Key < pending[b].Key })
	if len(pending) > 0 {
		if err := canonical.adopt(ctx, pending); err != nil {
			return fmt.Errorf("drain: adopt %s: %w", g.Name(), err)
		}
	}
	current, err := canonical.Rows(ctx)
	if err != nil {
		return fmt.Errorf("drain: reread canonical after adopt %s: %w", g.Name(), err)
	}
	for _, r := range pending {
		if !provesAdoption(current[r.Key], r) {
			return fmt.Errorf("drain: adopt %s: row %q not durably fenced (canonical %+v, generation %+v); generation retained", g.Name(), r.Key, current[r.Key], r)
		}
	}
	if err := canonical.releaseTransition(ctx, g.Name(), owner); err != nil {
		return fmt.Errorf("drain: release transition %s: %w", g.Name(), err)
	}
	if err := g.removeUnlocked(); err != nil {
		return fmt.Errorf("drain: remove %s: %w", g.Name(), err)
	}
	return nil
}

// reclaimOwnerless removes a generation directory that has no owner record: a
// crashed partial removal, or a claim that died before writing its owner. The
// root lock serializes it against claims and adoptions, and the owner recheck
// under the lock catches a claim that completed since the enumeration. An
// ownerless directory named by a live-owner transition claim is a mid-setup
// generation whose owner write was lost undurably — its owner re-fences it on
// retry, so the reclaim leaves it alone.
func reclaimOwnerless(ctx context.Context, cfg ScanConfig, g Generation) (bool, error) {
	lock, err := proc.Flock(ctx, g.rootLock())
	if err != nil {
		return false, err
	}
	defer lock.Release()
	if _, err := g.ReadOwner(); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	active, ok, err := cfg.Canonical.activeTransition(ctx)
	if err != nil {
		return false, err
	}
	if ok && active.Generation == g.Name() && assess(proberOrSys(cfg.prober), active.Owner.identity()) == Alive {
		return false, nil
	}
	if err := g.removeUnlocked(); err != nil {
		return false, err
	}
	return true, nil
}

func provesAdoption(canonical, generation Row) bool {
	return canonical.Seq > generation.Seq
}
