package proc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

const observationPollInterval = 100 * time.Millisecond

// ScopeObservation describes the recorded leader and its live session members.
type ScopeObservation struct {
	Identity  Identity
	Session   int
	Members   []Identity
	Reap      Reap
	Excluded  bool
	Uncertain bool
}

// Observation includes all recorded scopes from one store generation.
type Observation struct {
	Generation uint64
	Scopes     []ScopeObservation
}

// Complete requires certainty and absence of every unexcluded live member.
func (o Observation) Complete() bool {
	for _, scope := range o.Scopes {
		if !scope.complete() {
			return false
		}
	}
	return o.Generation != 0
}

func (o ScopeObservation) complete() bool {
	if o.Uncertain {
		return false
	}
	for _, member := range o.Members {
		if !o.Excluded || !instance(o.Identity).matches(instance(member)) {
			return false
		}
	}
	return o.Reap != reapUndetermined || o.Excluded
}

// Observe covers recorded identities and their dedicated sessions. Descendants
// that escape with setsid require their own adopted record.
func (s *Store) Observe(ctx context.Context, excluded []*Child) (Observation, error) {
	result := Observation{Generation: s.generation}
	live, err := s.snapshot(ctx)
	if err != nil {
		return Observation{}, errors.Join(ErrUnsettled, err)
	}
	var errs []error
	for _, rec := range live {
		scope, err := s.observeScope(ctx, rec.id(), rec.Session)
		for _, child := range excluded {
			if child.store == s && child.id.matches(rec.id()) {
				scope.Excluded = true
				break
			}
		}
		result.Scopes = append(result.Scopes, scope)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return result, errors.Join(errs...)
}

func (s *Store) observeScope(ctx context.Context, id identity, session int) (ScopeObservation, error) {
	scope := ScopeObservation{Identity: Identity{PID: id.pid, Start: id.start, Boot: id.boot}, Session: session}
	err := s.inspectScope(ctx, &scope)
	if err != nil {
		scope.Uncertain = true
		scope.Reap = reapUndetermined
		return scope, errors.Join(ErrUnsettled, err)
	}
	return scope, nil
}

func (s *Store) inspectScope(ctx context.Context, scope *ScopeObservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return errors.New("proc: record store is closed")
	default:
	}
	reap, settled, err := observe(s.prober, scope.Identity)
	if err != nil {
		return err
	}
	if !settled {
		scope.Members = append(scope.Members, scope.Identity)
	}
	if scope.Session != 0 && reap != ReapCrossBoot {
		if scope.Session <= 1 || scope.Session != scope.Identity.PID {
			return errors.New("proc: session record has no durable dedicated-session identity")
		}
		members, err := s.verifiedMembers(ctx, scope.Session, scope.Identity.Boot)
		if err != nil {
			return err
		}
		for _, member := range members {
			id := Identity{PID: member.pid, Start: member.info.start, Boot: scope.Identity.Boot}
			if !settled && instance(id).matches(instance(scope.Identity)) {
				continue
			}
			scope.Members = append(scope.Members, id)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(scope.Members) == 0 {
		scope.Reap = reap
	}
	slices.SortFunc(scope.Members, func(a, b Identity) int { return a.PID - b.PID })
	return nil
}

// RetireQuiet removes only records whose exact scopes are observed quiet.
func (s *Store) RetireQuiet(ctx context.Context) error {
	observation, err := s.Observe(ctx, nil)
	if err != nil {
		return err
	}
	var errs []error
	for _, scope := range observation.Scopes {
		if !scope.complete() {
			errs = append(errs, fmt.Errorf("%w: %s remains live", ErrUnsettled, scope.Identity))
			continue
		}
		if err := s.retireQuietScope(ctx, instance(scope.Identity), scope.Session); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Store) retireQuietScope(ctx context.Context, id identity, session int) error {
	scope, err := s.observeScope(ctx, id, session)
	if err != nil {
		return err
	}
	if !scope.complete() {
		return fmt.Errorf("%w: %s remains live", ErrUnsettled, scope.Identity)
	}
	return s.retireBounded(ctx, id)
}

// Observe checks the child and its recorded session without signaling.
func (c *Child) Observe(ctx context.Context) (ScopeObservation, error) {
	return c.store.observeScope(ctx, c.id, c.session)
}

// WaitNatural requires a deadline and leaves the child untouched on expiry.
func (c *Child) WaitNatural(ctx context.Context) (Exit, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Exit{}, errors.New("proc: natural wait requires a context deadline")
	}
	select {
	case <-c.settled:
		if c.exit.Reap == reapUndetermined {
			return c.exit, ErrUnsettled
		}
		return c.exit, nil
	case <-ctx.Done():
		return Exit{}, errors.Join(ErrUnsettled, ctx.Err())
	}
}

func (s *Store) awaitNaturalScope(c *Child, clk clock) (Reap, bool) {
	for {
		scope, err := c.Observe(context.Background())
		if err == nil && scope.complete() {
			return scope.Reap, true
		}
		select {
		case deadline := <-c.demand:
			c.demanded = deadline
			return ReapAbsent, true
		case <-s.closed:
			return reapUndetermined, false
		case <-clk.After(observationPollInterval):
		}
	}
}

// Observe checks the adopted identity and recorded session without signaling.
func (a *Adopted) Observe(ctx context.Context) (ScopeObservation, error) {
	return a.store.observeScope(ctx, a.id, a.session)
}

// ObserveAndRetire requires a deadline and retains records until their scope is quiet.
func (a *Adopted) ObserveAndRetire(ctx context.Context) (Reap, error) {
	if _, ok := ctx.Deadline(); !ok {
		return reapUndetermined, errors.New("proc: observe and retire requires a context deadline")
	}
	clk := clockOrReal(a.store.clock)
	for {
		scope, err := a.Observe(ctx)
		if err != nil {
			return reapUndetermined, err
		}
		if scope.complete() {
			if err := a.store.retireQuietScope(ctx, a.id, a.session); err != nil {
				return reapUndetermined, err
			}
			return scope.Reap, nil
		}
		select {
		case <-ctx.Done():
			return reapUndetermined, errors.Join(ErrUnsettled, ctx.Err())
		case <-clk.After(observationPollInterval):
		}
	}
}
