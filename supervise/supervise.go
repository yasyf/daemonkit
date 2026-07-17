// Package supervise is consumer-agnostic in-process child supervision: a
// ticker-driven keep-alive loop that re-spawns a supervised child once its death
// is confirmed, under a restart budget. The consumer describes each child through
// the Subject seam — it names the subject, reports a cheap staleness signal,
// authoritatively probes process liveness, and provides the kill/respawn/park
// hooks — while this package owns the loop mechanics: nominate on staleness,
// corroborate with the probe, act under a per-subject lock, and rate-limit
// respawns with a sliding-window strike budget.
//
// The safety contract is fail-closed. A staleness signal only NOMINATES death; an
// authoritative Alive probe must corroborate before any action, and every probe
// error is read as not-confirmed-dead, so an ambiguous signal never kills or
// respawns a possibly-live child. Kill and respawn run under the same per-subject
// lock as the consumer's manual-kill path, and the subject's status is re-read
// under that lock immediately before acting, so a child killed or respawned by
// another path between nomination and action is detected, not double-actioned.
package supervise

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// Subject is one supervised child. The consumer names it, reports whether it
// looks stale (the cheap death-nomination), authoritatively probes whether its
// process is still alive (the corroborating gate), and provides the hooks the
// supervisor drives under the per-subject lock.
type Subject interface {
	// Name identifies the subject in errors and as the strike-budget key; it is
	// stable for the subject's lifetime.
	Name() string
	// Lock returns the per-subject lock shared with the consumer's manual-kill
	// path. It must return the same locker for the subject's identity across
	// every Subject value (back it with a KeyedMutex). The supervisor holds it
	// across the re-read, kill, and respawn so a concurrent manual kill is
	// serialized, never interleaved.
	Lock() sync.Locker
	// SpawnedAt is when the subject's current process was last (re)spawned; the
	// supervisor suppresses nomination until StartupGrace past it.
	SpawnedAt() time.Time
	// Active reports whether the subject is still in a supervisable lifecycle
	// state. It is re-read under the lock immediately before acting; a subject a
	// manual path has already killed or parked returns false and is skipped.
	Active(ctx context.Context) (bool, error)
	// Stale reports whether the subject's staleness signal has tripped — the
	// pre-filter that only NOMINATES death. An error folds to false: a failed
	// staleness read is "no signal", never an action.
	Stale(ctx context.Context) bool
	// Alive authoritatively probes the subject's process (kill(pid, 0) or a
	// process lookup with identity revalidation). Any error means
	// not-confirmed-dead, so the supervisor does nothing this tick.
	Alive(ctx context.Context) (bool, error)
	// Kill tears down any husk surviving a confirmed-dead process before the
	// respawn; it is called on an already-dead process, so it must be safe there.
	Kill(ctx context.Context) error
	// Respawn starts a fresh process for the subject.
	Respawn(ctx context.Context) error
	// Park gives up on the subject once its restart budget is exhausted: the
	// consumer records the terminal state and Active must then return false, so
	// the parked subject is not re-actioned on later ticks.
	Park(ctx context.Context) error
}

// Roster enumerates the subjects to supervise this tick. A failed enumeration is
// "no signal": the tick is aborted and retried next interval, never treated as
// "all subjects gone".
type Roster func(ctx context.Context) ([]Subject, error)

// Supervisor is a ticker-driven keep-alive loop over a Roster of Subjects. The
// zero value is not runnable: Interval, Limit, and Window are required.
type Supervisor struct {
	// Interval is the tick cadence.
	Interval time.Duration
	// StartupGrace suppresses nomination for a subject spawned within this window,
	// so a freshly (re)spawned child is never mistaken for a dead one. Zero
	// disables the grace.
	StartupGrace time.Duration
	// Limit is the number of respawns allowed within Window before a subject is
	// parked rather than respawned again.
	Limit int
	// Window is the sliding window the respawn strikes are counted within.
	Window time.Duration
	// OnError, when set, observes a tick's error (a roster failure or a subject
	// hook failure) instead of it being dropped. It never stops the loop.
	OnError func(error)

	clock   clock
	mu      sync.Mutex
	strikes map[string]*proc.Strikes
}

func (s *Supervisor) validate() error {
	if s.Interval <= 0 {
		return errors.New("supervise: Interval must be positive")
	}
	if s.Limit <= 0 {
		return errors.New("supervise: Limit must be positive")
	}
	if s.Window <= 0 {
		return errors.New("supervise: Window must be positive")
	}
	return nil
}

func (s *Supervisor) init() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.strikes == nil {
		s.strikes = make(map[string]*proc.Strikes)
	}
	if s.clock == nil {
		s.clock = realClock{}
	}
}

// Run drives the supervision loop until ctx is cancelled, ticking every Interval
// and returning nil on cancellation.
func (s *Supervisor) Run(ctx context.Context, roster Roster) error {
	if err := s.validate(); err != nil {
		return err
	}
	s.init()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.clock.After(s.Interval):
			if err := s.tick(ctx, roster); err != nil && s.OnError != nil {
				s.OnError(err)
			}
		}
	}
}

// tick reconciles every rostered subject once. A subject hook failure aborts the
// tick; the surviving subjects are retried next interval.
func (s *Supervisor) tick(ctx context.Context, roster Roster) error {
	s.init()
	subjects, err := roster(ctx)
	if err != nil {
		return fmt.Errorf("supervise: roster: %w", err)
	}
	s.pruneStrikes(subjects)
	for _, subj := range subjects {
		if err := s.reconcile(ctx, subj); err != nil {
			return err
		}
	}
	return nil
}

// pruneStrikes drops strike history for names the roster no longer surfaces;
// a name that leaves and returns starts with a fresh budget.
func (s *Supervisor) pruneStrikes(subjects []Subject) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.strikes) == 0 {
		return
	}
	names := make(map[string]struct{}, len(subjects))
	for _, subj := range subjects {
		names[subj.Name()] = struct{}{}
	}
	for name := range s.strikes {
		if _, ok := names[name]; !ok {
			delete(s.strikes, name)
		}
	}
}

// reconcile is the decision site for one subject: nominate on staleness (past the
// startup grace), then under the per-subject lock re-read status, corroborate
// death with the authoritative probe, and either respawn under budget or park.
func (s *Supervisor) reconcile(ctx context.Context, subj Subject) error {
	active, err := subj.Active(ctx)
	if err != nil {
		return fmt.Errorf("supervise %s: active: %w", subj.Name(), err)
	}
	if !active {
		return nil
	}
	if s.clock.Now().Sub(subj.SpawnedAt()) < s.StartupGrace {
		return nil
	}
	if !subj.Stale(ctx) {
		return nil
	}

	lock := subj.Lock()
	lock.Lock()
	defer lock.Unlock()

	active, err = subj.Active(ctx)
	if err != nil {
		return fmt.Errorf("supervise %s: active recheck: %w", subj.Name(), err)
	}
	if !active {
		return nil
	}
	alive, err := subj.Alive(ctx)
	if err != nil || alive {
		return nil
	}
	return s.respawnUnderBudget(ctx, subj)
}

// respawnUnderBudget kills the confirmed-dead subject's husk and respawns it while
// it stays under Limit respawns per Window, and parks it at or over budget. The
// caller holds the subject lock and has re-confirmed the process dead. The strike
// is recorded before the respawn, so a respawn that itself fails still counts
// toward the budget and a persistently-dying subject climbs to a park.
func (s *Supervisor) respawnUnderBudget(ctx context.Context, subj Subject) error {
	strikes := s.strikesFor(subj.Name())
	now := s.clock.Now()
	if strikes.Struck(now) {
		if err := subj.Park(ctx); err != nil {
			return fmt.Errorf("supervise %s: park: %w", subj.Name(), err)
		}
		return nil
	}
	strikes.Strike(now)
	if err := subj.Kill(ctx); err != nil {
		return fmt.Errorf("supervise %s: kill: %w", subj.Name(), err)
	}
	if err := subj.Respawn(ctx); err != nil {
		return fmt.Errorf("supervise %s: respawn: %w", subj.Name(), err)
	}
	return nil
}

func (s *Supervisor) strikesFor(name string) *proc.Strikes {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.strikes[name]
	if st == nil {
		st = &proc.Strikes{Limit: s.Limit, Window: s.Window}
		s.strikes[name] = st
	}
	return st
}
