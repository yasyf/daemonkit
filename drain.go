package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

// ErrMaintenance rejects ordinary work while a preserving drain is reversible.
var ErrMaintenance = wire.ErrMaintenance

// DrainPreparation retains the product's admission barriers through commitment.
// Abort must resume producers before reopening ownership admissions. A failed
// Abort leaves the daemon unavailable. Both methods must honor their budget.
type DrainPreparation interface {
	Commit(Budget) error
	Abort(Budget) error
}

// DrainPreparer pauses producers and proves their owned scopes quiet without
// cancelling activation or requests. An error with a non-nil preparation asks
// the runtime to abort it; a nil preparation with an error promises that no
// product admission barrier remains held.
type DrainPreparer interface {
	PrepareDrain(Budget) (DrainPreparation, error)
}

// MaintenanceHandler serves only continuation, evidence, cleanup, or status
// requests while ordinary product dispatch is paused. It must not admit new
// producers or create untracked children. Products without it reject all work.
type MaintenanceHandler interface {
	HandleMaintenance(context.Context, Request) (Reply, error)
}

type drainAttempt struct {
	done   chan struct{}
	budget Budget
	err    error
}

func (r *serveRuntime) Drain(ctx context.Context) error {
	if r.policy != PreserveOwned {
		r.publish(wire.PhaseDraining)
		r.signal()
		return nil
	}
	if ctx.Err() != nil {
		return ErrDrainPreparationTimeout
	}
	r.mu.Lock()
	if r.snapshot.Phase == wire.PhaseDraining {
		r.mu.Unlock()
		return nil
	}
	if r.snapshot.Phase == wire.PhaseFailed {
		r.mu.Unlock()
		return fmt.Errorf("%w: runtime is unavailable", ErrDrainBusy)
	}
	attempt := r.preparing
	if attempt == nil {
		if r.product == nil {
			r.mu.Unlock()
			return fmt.Errorf("%w: activation has not produced a drain preparer", ErrDrainBusy)
		}
		preparer, ok := r.product.(DrainPreparer)
		if !ok {
			r.mu.Unlock()
			return fmt.Errorf("%w: preserving product must implement DrainPreparer", ErrDrainBusy)
		}
		attempt = &drainAttempt{done: make(chan struct{}), budget: r.grace.mint("prepare")}
		r.preparing = attempt
		r.publishLocked(wire.PhaseMaintenance)
		go r.prepareDrain(attempt, preparer)
	}
	r.mu.Unlock()
	wait, cancel := attempt.budget.Context(ctx)
	defer cancel()
	select {
	case <-attempt.done:
		return attempt.err
	case <-wait.Done():
		return fmt.Errorf("%w: preparation remains runtime-owned until its budget expires", ErrDrainPreparationTimeout)
	}
}

func (r *serveRuntime) prepareDrain(attempt *drainAttempt, preparer DrainPreparer) {
	work := attempt.budget.Share("proof", 0.75)
	prepared, err := preparer.PrepareDrain(work)
	missing := err == nil && prepared == nil
	if missing {
		err = errors.New("product returned no drain preparation")
	}
	if work.Left() == 0 {
		err = ErrDrainPreparationTimeout
	}
	if err == nil {
		err = prepared.Commit(work)
		if work.Left() == 0 && err == nil {
			err = ErrDrainPreparationTimeout
		}
	}
	if err == nil {
		r.mu.Lock()
		r.publishLocked(wire.PhaseDraining)
		r.signal()
		close(attempt.done)
		r.mu.Unlock()
		return
	}
	refusal := ErrDrainBusy
	if errors.Is(err, ErrDrainPreparationTimeout) || errors.Is(err, context.DeadlineExceeded) {
		refusal = ErrDrainPreparationTimeout
	}
	attempt.err = fmt.Errorf("%w: %v", refusal, err)
	var aborted error
	if missing {
		aborted = err
	}
	if prepared != nil {
		rollback := Budget{deadline: attempt.budget.deadline, path: "prepare-abort"}
		aborted = prepared.Abort(rollback)
		if rollback.Left() == 0 && aborted == nil {
			aborted = errors.New("product resume exceeded its budget")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if aborted == nil {
		r.preparing = nil
		r.publishLocked(wire.PhaseReady)
	} else {
		attempt.err = fmt.Errorf("%w: product resume failed: %v", refusal, aborted)
	}
	close(attempt.done)
}

func (r *serveRuntime) triggerDrain() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.grace))
	defer cancel()
	if err := r.Drain(ctx); err != nil {
		slog.Warn("daemonkit: shutdown preparation refused; runtime retained", "err", err)
	}
}

func drainRefused(err error) bool {
	return errors.Is(err, ErrDrainBusy) || errors.Is(err, ErrDrainPreparationTimeout)
}
