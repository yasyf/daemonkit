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

// ErrDrainInProgress refuses a second drain transition while one already holds
// the drain flag, so concurrent transitions never snapshot the same canonical
// rows into two generations.
var ErrDrainInProgress = errors.New("drain: transition already in progress")

// Intake gates canonical admission: Admit tracks in-flight work, Close sets the
// drain flag refusing new work, Settle blocks until admitted work settles.
type Intake struct {
	mu       sync.Mutex
	draining bool
	inflight int
	settled  chan struct{}
}

// Admit admits one unit of work, returning ErrDraining once the flag is set.
// The caller invokes done exactly once at the unit's terminal outcome.
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

// Close idempotently sets the drain flag; admissions and registrations refuse
// from now on. Use BeginDrain where a second setter must be refused.
func (i *Intake) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.draining = true
}

// BeginDrain compare-and-sets the drain flag: the first caller sets it and wins,
// a caller that finds it already set refuses with ErrDrainInProgress. Admissions
// and registrations refuse once the flag is set. The flag is committed before
// the caller returns, so a concurrent transition observes it before snapshotting.
func (i *Intake) BeginDrain() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.draining {
		return ErrDrainInProgress
	}
	i.draining = true
	return nil
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

// Register CAS-appends key to j as one admitted unit; once the drain flag is
// set it refuses, so no handler can create an untracked row post-snapshot.
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
type TransitionConfig struct {
	// Intake is the canonical admission gate; Transition closes it (a) and
	// settles it (c).
	Intake *Intake
	// CloseIntake stops and closes canonical intake and drain handlers (b),
	// strictly before the snapshot.
	CloseIntake func(ctx context.Context) error
	// Canonical is the canonical ownership journal: snapshotted (d), then
	// truncated (e).
	Canonical Journal
	// Generation receives the owner record and row snapshot (d).
	Generation Generation
	// Self is the draining process's identity, recorded as generation owner.
	Self proc.Identity
	// BindDrainListener binds the generation drain listener (f).
	BindDrainListener func(ctx context.Context) error
	// ReleaseLock releases the canonical flock (g); wire proc.FlockHandle.Release.
	// A release failure fails the transition before the successor spawns, so no
	// successor comes up behind a canonical lock this process still holds.
	ReleaseLock func() error
	// SpawnSuccessor spawns the successor (h); wire proc.Spawn.EnsureRunning.
	SpawnSuccessor func(ctx context.Context) error

	afterStep   func(Step) error
	midSnapshot func() error
}

func (cfg TransitionConfig) step(s Step) error {
	if cfg.afterStep == nil {
		return nil
	}
	return cfg.afterStep(s)
}

// Transition runs the normative drain transition a..h in order. After it
// returns nil, the generation journal owns every row and the successor is
// spawned; the caller then drives Run until the journal drains to zero.
func Transition(ctx context.Context, cfg TransitionConfig) error {
	if err := cfg.Intake.BeginDrain(); err != nil {
		return err
	}
	if err := cfg.step(StepDrainFlag); err != nil {
		return err
	}
	if err := cfg.CloseIntake(ctx); err != nil {
		return fmt.Errorf("drain: close intake: %w", err)
	}
	if err := cfg.step(StepCloseIntake); err != nil {
		return err
	}
	if err := cfg.Intake.Settle(ctx); err != nil {
		return fmt.Errorf("drain: settle admitted work: %w", err)
	}
	if err := cfg.step(StepSettle); err != nil {
		return err
	}
	if err := cfg.snapshot(ctx); err != nil {
		return err
	}
	if err := cfg.step(StepSnapshot); err != nil {
		return err
	}
	if err := cfg.Canonical.Truncate(ctx); err != nil {
		return fmt.Errorf("drain: truncate canonical: %w", err)
	}
	if err := cfg.step(StepTruncate); err != nil {
		return err
	}
	if err := cfg.BindDrainListener(ctx); err != nil {
		return fmt.Errorf("drain: bind drain listener: %w", err)
	}
	if err := cfg.step(StepBindListener); err != nil {
		return err
	}
	if err := cfg.ReleaseLock(); err != nil {
		return fmt.Errorf("drain: release canonical lock: %w", err)
	}
	if err := cfg.step(StepReleaseLock); err != nil {
		return err
	}
	if err := cfg.SpawnSuccessor(ctx); err != nil {
		return fmt.Errorf("drain: spawn successor: %w", err)
	}
	return cfg.step(StepSpawn)
}

func (cfg TransitionConfig) snapshot(ctx context.Context) error {
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
		if err := cfg.Generation.WriteOwner(cfg.Self); err != nil {
			return fmt.Errorf("drain: record generation owner: %w", err)
		}
		return nil
	}
	if err := cfg.Generation.Journal().ClaimSnapshot(ctx, ordered, claim); err != nil {
		return fmt.Errorf("drain: snapshot generation %s: %w", cfg.Generation.Name(), err)
	}
	return nil
}

// OnSkew adapts Transition to daemon.SkewConfig.OnSkew, so a confirmed newer
// artifact triggers the drain transition.
func OnSkew(cfg TransitionConfig) func(context.Context) error {
	return func(ctx context.Context) error { return Transition(ctx, cfg) }
}
