package drain

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/yasyf/daemonkit/proc"
)

// ErrDraining refuses admission and registration once the drain flag is set.
var ErrDraining = errors.New("drain: draining")

// ErrDrainInProgress refuses a second drain transition while a claim is active.
var ErrDrainInProgress = errors.New("drain: transition already in progress")

// ErrSpawnParked refuses a successor spawn while the strike breaker is parked.
var ErrSpawnParked = errors.New("drain: successor spawn parked")

// Intake gates canonical admission.
type Intake struct {
	mu            sync.Mutex
	draining      bool
	transitioning bool
	inflight      int
	settled       chan struct{}
}

// Admit admits one unit of work, returning ErrDraining once the flag is set.
func (i *Intake) Admit() (done func(), err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.draining {
		return nil, ErrDraining
	}
	i.inflight++
	return func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		i.inflight--
		if i.inflight == 0 && i.settled != nil {
			close(i.settled)
			i.settled = nil
		}
	}, nil
}

// Close idempotently sets the drain flag.
func (i *Intake) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.draining = true
	i.transitioning = true
}

// BeginDrain compare-and-sets the transition attempt.
func (i *Intake) BeginDrain() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.transitioning {
		return ErrDrainInProgress
	}
	i.transitioning = true
	i.draining = true
	return nil
}

func (i *Intake) abortTransition(reopen bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.transitioning = false
	if reopen {
		i.draining = false
	}
}

// Draining reports whether the drain flag is set.
func (i *Intake) Draining() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.draining
}

// Settle blocks until every admitted unit reached its terminal outcome.
func (i *Intake) Settle(ctx context.Context) error {
	i.mu.Lock()
	if i.inflight == 0 {
		i.mu.Unlock()
		return nil
	}
	if i.settled == nil {
		i.settled = make(chan struct{})
	}
	ch := i.settled
	i.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// Register CAS-appends key to j as one admitted unit.
func (i *Intake) Register(ctx context.Context, j Journal, key Key) (Row, error) {
	done, err := i.Admit()
	if err != nil {
		return Row{}, err
	}
	defer done()
	return j.Bump(ctx, key, RowPending)
}

// Step is one transition step in the normative a..h order.
type Step int

const (
	// StepDrainFlag sets the draining flag (a).
	StepDrainFlag Step = iota + 1
	// StepCloseIntake stops and closes canonical intake and drain handlers (b).
	StepCloseIntake
	// StepSettle waits for admitted running and queued work to settle (c).
	StepSettle
	// StepSnapshot snapshots the generation journal (d).
	StepSnapshot
	// StepTruncate truncates the canonical ownership state (e).
	StepTruncate
	// StepBindListener binds the generation drain listener (f).
	StepBindListener
	// StepReleaseLock releases the canonical flock (g).
	StepReleaseLock
	// StepSpawn spawns the successor (h).
	StepSpawn
)

// TransitionConfig drives one drain transition; every field is required.
// Every callback can run more than once — a crash between a step's effect
// and its durable phase record replays it — so all callbacks must be
// idempotent, and a failed Transition retries with the SAME Generation.
type TransitionConfig struct {
	Intake            *Intake
	CloseIntake       func(ctx context.Context) error
	Canonical         Journal
	Generation        Generation
	Self              proc.Identity
	BindDrainListener func(ctx context.Context) error
	ReleaseLock       func() error
	SpawnSuccessor    func(ctx context.Context) error

	afterStep   func(Step) error
	midSnapshot func() error
}

func (cfg TransitionConfig) step(s Step) error {
	if cfg.afterStep == nil {
		return nil
	}
	return cfg.afterStep(s)
}

// Transition runs the normative drain transition a..h in order.
func Transition(ctx context.Context, cfg TransitionConfig) (err error) {
	gen, err := cfg.Generation.claimOwner(ctx, cfg.Self)
	if err != nil {
		return fmt.Errorf("drain: claim generation owner: %w", err)
	}
	if err := cfg.Intake.BeginDrain(); err != nil {
		return err
	}
	phase, err := cfg.Canonical.claimTransition(ctx, gen.Name(), cfg.Self)
	if err != nil {
		cfg.Intake.abortTransition(true)
		return fmt.Errorf("drain: claim active transition: %w", err)
	}
	defer func() {
		if err != nil {
			cfg.Intake.abortTransition(false)
		}
	}()
	if phase == 0 {
		complete, err := gen.journal().isComplete(ctx)
		if err != nil {
			return fmt.Errorf("drain: read generation journal: %w", err)
		}
		if complete {
			if err := cfg.Canonical.releaseTransition(context.WithoutCancel(ctx), gen.Name(), cfg.Self); err != nil {
				return fmt.Errorf("drain: release re-claimed transition: %w", err)
			}
			return nil
		}
	}
	advance := func(step Step) error {
		if err := cfg.Canonical.advanceTransition(context.WithoutCancel(ctx), gen.Name(), cfg.Self, step); err != nil {
			return fmt.Errorf("drain: record transition step %d: %w", step, err)
		}
		if step > phase {
			phase = step
		}
		return cfg.step(step)
	}
	// a..c replay on every attempt (admission may have reopened between
	// attempts); the durable phase record gates only the effectful d..h.
	if err := advance(StepDrainFlag); err != nil {
		return err
	}
	if err := cfg.CloseIntake(ctx); err != nil {
		return fmt.Errorf("drain: close intake: %w", err)
	}
	if err := advance(StepCloseIntake); err != nil {
		return err
	}
	if err := cfg.Intake.Settle(ctx); err != nil {
		return fmt.Errorf("drain: settle admitted work: %w", err)
	}
	if err := advance(StepSettle); err != nil {
		return err
	}
	if phase < StepSnapshot {
		if err := cfg.snapshot(ctx, gen); err != nil {
			return err
		}
		if err := advance(StepSnapshot); err != nil {
			return err
		}
	}
	if phase < StepTruncate {
		scope, err := gen.journal().Rows(ctx)
		if err != nil {
			return fmt.Errorf("drain: read generation snapshot: %w", err)
		}
		if err := cfg.Canonical.Truncate(ctx, scope); err != nil {
			return fmt.Errorf("drain: truncate canonical: %w", err)
		}
		if err := advance(StepTruncate); err != nil {
			return err
		}
	}
	if phase < StepBindListener {
		if err := cfg.BindDrainListener(ctx); err != nil {
			return fmt.Errorf("drain: bind drain listener: %w", err)
		}
		if err := advance(StepBindListener); err != nil {
			return err
		}
	}
	if phase < StepReleaseLock {
		if err := cfg.ReleaseLock(); err != nil {
			return fmt.Errorf("drain: release canonical lock: %w", err)
		}
		if err := advance(StepReleaseLock); err != nil {
			return err
		}
	}
	if phase < StepSpawn {
		if err := cfg.SpawnSuccessor(ctx); err != nil {
			return fmt.Errorf("drain: spawn successor: %w", err)
		}
		if err := advance(StepSpawn); err != nil {
			return err
		}
	}
	// The completion marker lands before the release, so a release whose rename landed but fsync failed reads as done on retry.
	if err := gen.journal().markComplete(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("drain: mark transition complete: %w", err)
	}
	if err := cfg.Canonical.releaseTransition(context.WithoutCancel(ctx), gen.Name(), cfg.Self); err != nil {
		return fmt.Errorf("drain: release active transition: %w", err)
	}
	return nil
}

func (cfg TransitionConfig) snapshot(ctx context.Context, gen Generation) error {
	rows, err := cfg.Canonical.Rows(ctx)
	if err != nil {
		return fmt.Errorf("drain: read canonical: %w", err)
	}
	ordered := make([]Row, 0, len(rows))
	for _, r := range rows {
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(a, b int) bool { return ordered[a].Key < ordered[b].Key })
	claim := func() error {
		if cfg.midSnapshot != nil {
			if err := cfg.midSnapshot(); err != nil {
				return err
			}
		}
		owner, err := gen.ReadOwner()
		if err != nil {
			return fmt.Errorf("drain: read generation owner: %w", err)
		}
		if owner != cfg.Self {
			return fmt.Errorf("drain: generation owner %+v does not match self %+v", owner, cfg.Self)
		}
		return nil
	}
	if err := gen.journal().claimSnapshot(ctx, ordered, claim); err != nil {
		return fmt.Errorf("drain: snapshot generation %s: %w", gen.Name(), err)
	}
	return nil
}

// OnSkew adapts Transition to daemon.SkewConfig.OnSkew.
func OnSkew(cfg TransitionConfig) func(context.Context) error {
	return func(ctx context.Context) error { return Transition(ctx, cfg) }
}
