package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

// ShutdownPolicy selects automatic settlement; explicit Stop remains separate.
type ShutdownPolicy uint8

const (
	TerminateOwned ShutdownPolicy = iota
	PreserveOwned
)

const naturalPollInterval = 100 * time.Millisecond

// RecordedScopeCoverage excludes descendants that escape a recorded session.
const RecordedScopeCoverage = "recorded process identities and dedicated sessions; escaped descendants require explicit adoption"

// ObservedProcess describes an observed instance without granting signal authority.
type ObservedProcess struct {
	PID   int
	Start uint64
	Boot  uint64
}

// ScopeObservation includes a recorded leader and its live session members.
type ScopeObservation struct {
	Leader    ObservedProcess
	Session   int
	Members   []ObservedProcess
	Reap      Reap
	Producer  bool
	Uncertain bool
}

// Quiet excludes only the exact producer leader, never another session member.
func (s ScopeObservation) Quiet() bool {
	if s.Uncertain {
		return false
	}
	for _, member := range s.Members {
		if !s.Producer || member != s.Leader {
			return false
		}
	}
	return s.Reap != ReapUndetermined || s.Producer
}

// OwnershipObservation is diagnostic data, not authority to retire ownership.
type OwnershipObservation struct {
	Generation uint64
	Coverage   string
	Pending    []string
	Scopes     []ScopeObservation
	Uncertain  bool
}

// Complete reports whether this observation found no unexcluded live work.
func (o OwnershipObservation) Complete() bool {
	if o.Generation == 0 || o.Uncertain || len(o.Pending) != 0 {
		return false
	}
	return !slices.ContainsFunc(o.Scopes, func(s ScopeObservation) bool { return !s.Quiet() })
}

// NaturalProof belongs to one held admission lease and ownership generation.
type NaturalProof struct {
	drain      *NaturalDrain
	generation uint64
	admissions uint64
	serial     uint64
}

// NaturalDrain holds ownership admission while the caller keeps producers paused.
type NaturalDrain struct {
	owner      *Owned
	producers  []*Child
	generation uint64
	admissions uint64
	serial     uint64
	committed  bool
}

// Observe reads recorded scopes and pending registrations without signaling.
func (o *Owned) Observe(ctx context.Context) (OwnershipObservation, error) {
	return o.observe(ctx, nil)
}

func (o *Owned) observe(ctx context.Context, drain *NaturalDrain) (OwnershipObservation, error) {
	o.mu.Lock()
	if drain != nil && (o.drain != drain || drain.committed) {
		o.mu.Unlock()
		return OwnershipObservation{Uncertain: true}, ErrDrainBusy
	}
	admissions := o.admissions
	pending := make([]string, 0, len(o.starting))
	for res := range o.starting {
		pending = append(pending, res.verb)
	}
	var excluded []*proc.Child
	if drain != nil {
		for _, child := range drain.producers {
			excluded = append(excluded, child.child)
		}
	}
	o.mu.Unlock()
	slices.Sort(pending)
	observed, err := o.store.Observe(ctx, excluded)
	result := OwnershipObservation{
		Generation: observed.Generation,
		Coverage:   RecordedScopeCoverage,
		Pending:    pending,
		Scopes:     make([]ScopeObservation, len(observed.Scopes)),
		Uncertain:  err != nil,
	}
	for i, scope := range observed.Scopes {
		result.Scopes[i] = scopeObservation(scope)
	}
	o.mu.Lock()
	changed := o.admissions != admissions || (drain != nil && (o.drain != drain || drain.committed))
	o.mu.Unlock()
	if changed {
		result.Uncertain = true
		return result, errors.Join(err, ErrDrainBusy)
	}
	return result, err
}

func observedProcess(id proc.Identity) ObservedProcess {
	return ObservedProcess{PID: id.PID, Start: id.Start, Boot: id.Boot}
}

func scopeObservation(scope proc.ScopeObservation) ScopeObservation {
	result := ScopeObservation{
		Leader:    observedProcess(scope.Identity),
		Session:   scope.Session,
		Reap:      Reap(scope.Reap),
		Producer:  scope.Excluded,
		Uncertain: scope.Uncertain,
		Members:   make([]ObservedProcess, len(scope.Members)),
	}
	for i, member := range scope.Members {
		result.Members[i] = observedProcess(member)
	}
	return result
}

// BeginNaturalDrain closes admissions; Wait includes reservations already admitted.
// The caller must pause application producers before beginning this lease.
func (o *Owned) BeginNaturalDrain(ctx context.Context, producers []*Child) (*NaturalDrain, error) {
	if ctx.Err() != nil {
		return nil, naturalWaitError(ctx)
	}
	if err := budgeted(ctx, "BeginNaturalDrain"); err != nil {
		return nil, err
	}
	if o.store.Policy() != proc.PreserveOwned {
		return nil, errors.New("daemonkit: natural drain requires PreserveOwned")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.drain != nil {
		return nil, ErrDrainBusy
	}
	for _, producer := range producers {
		if _, owned := o.children[producer]; !owned {
			return nil, fmt.Errorf("daemonkit: producer is not a live child of this ownership scope: %w", ErrDrainBusy)
		}
	}
	drain := &NaturalDrain{
		owner:      o,
		producers:  append([]*Child(nil), producers...),
		generation: o.store.Generation(),
		admissions: o.admissions,
	}
	o.drain = drain
	return drain, nil
}

// BeginNaturalDrain leases the daemon's ownership admissions without signaling.
func (x Ctx) BeginNaturalDrain(ctx context.Context, producers []*Child) (*NaturalDrain, error) {
	if x.owner == nil {
		return nil, errZeroCtx
	}
	return x.owner.BeginNaturalDrain(ctx, producers)
}

// Observe reads the daemon's recorded ownership scopes without signaling.
func (x Ctx) Observe(ctx context.Context) (OwnershipObservation, error) {
	if x.owner == nil {
		return OwnershipObservation{}, errZeroCtx
	}
	return x.owner.Observe(ctx)
}

// Wait retains its lease on timeout; resume producers before calling Abort.
func (d *NaturalDrain) Wait(ctx context.Context) (NaturalProof, error) {
	if _, ok := ctx.Deadline(); !ok {
		return NaturalProof{}, errors.New("daemonkit: natural wait requires a context deadline")
	}
	ticker := time.NewTicker(naturalPollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return NaturalProof{}, naturalWaitError(ctx)
		}
		observed, err := d.owner.observe(ctx, d)
		if errors.Is(err, ErrDrainBusy) {
			return NaturalProof{}, err
		}
		if err == nil && observed.Complete() {
			if ctx.Err() != nil {
				return NaturalProof{}, naturalWaitError(ctx)
			}
			d.owner.mu.Lock()
			if d.owner.drain != d || d.committed || len(d.owner.starting) != 0 {
				d.owner.mu.Unlock()
				return NaturalProof{}, ErrDrainBusy
			}
			d.serial++
			proof := NaturalProof{drain: d, generation: d.generation, admissions: d.admissions, serial: d.serial}
			d.owner.mu.Unlock()
			return proof, nil
		}
		select {
		case <-ctx.Done():
			return NaturalProof{}, naturalWaitError(ctx)
		case <-ticker.C:
		}
	}
}

func naturalWaitError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: owned work did not become quiet", ErrDrainPreparationTimeout)
	}
	return fmt.Errorf("%w: natural wait canceled", ErrDrainBusy)
}

// Commit consumes the lease's proof without signaling or retiring a record.
// Producers must remain paused; shutdown continues with observational settlement.
func (d *NaturalDrain) Commit(proof NaturalProof) error {
	d.owner.mu.Lock()
	defer d.owner.mu.Unlock()
	if d.owner.drain != d || d.committed || proof.drain != d || proof.serial == 0 || proof.serial != d.serial || proof.generation != d.generation || proof.admissions != d.admissions || d.owner.admissions != d.admissions || len(d.owner.starting) != 0 {
		return ErrDrainBusy
	}
	d.committed = true
	d.owner.closed = true
	return nil
}

// Abort reopens admissions only after the caller has resumed its producers.
func (d *NaturalDrain) Abort() error {
	d.owner.mu.Lock()
	defer d.owner.mu.Unlock()
	if d.owner.drain != d || d.committed || d.owner.closed {
		return ErrDrainBusy
	}
	d.owner.drain = nil
	return nil
}

func (o *Owned) settleNatural(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("daemonkit: settling an ownership scope requires a context deadline")
	}
	o.mu.Lock()
	if o.drain != nil && !o.drain.committed {
		o.mu.Unlock()
		return ErrDrainBusy
	}
	o.closed = true
	starting := make([]*reservation, 0, len(o.starting))
	for res := range o.starting {
		starting = append(starting, res)
	}
	o.mu.Unlock()
	for _, res := range starting {
		select {
		case <-res.done:
		case <-ctx.Done():
			return fmt.Errorf("daemonkit: %s still registering: %w", res.verb, errors.Join(ErrUnsettled, ctx.Err()))
		}
	}
	o.mu.Lock()
	children := make([]*Child, 0, len(o.children))
	for child := range o.children {
		children = append(children, child)
	}
	o.mu.Unlock()
	for _, child := range children {
		if _, err := child.WaitNatural(ctx); err != nil {
			return fmt.Errorf("daemonkit: child %d did not settle naturally: %w", child.PID(), err)
		}
	}
	ticker := time.NewTicker(naturalPollInterval)
	defer ticker.Stop()
	for {
		err := o.store.RetireQuiet(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnsettled, ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

// Observe reads the child's exact leader and recorded session without signaling.
func (c *Child) Observe(ctx context.Context) (ScopeObservation, error) {
	scope, err := c.child.Observe(ctx)
	return scopeObservation(scope), err
}

// WaitNatural never sends a signal or publishes an unproven terminal on timeout.
func (c *Child) WaitNatural(ctx context.Context) (Exit, error) {
	exit, err := c.child.WaitNatural(ctx)
	return exitOf(exit), err
}

// Observe reads the adopted identity and recorded session without signaling.
func (t *Tracked) Observe(ctx context.Context) (ScopeObservation, error) {
	scope, err := t.adopted.Observe(ctx)
	return scopeObservation(scope), err
}

// ObserveAndRetire waits for exact scope absence before releasing its record.
func (t *Tracked) ObserveAndRetire(ctx context.Context) (Reap, error) {
	reap, err := t.adopted.ObserveAndRetire(ctx)
	if err != nil {
		return Reap(reap), err
	}
	t.retire()
	return Reap(reap), nil
}
