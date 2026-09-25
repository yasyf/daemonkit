package daemonkit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

type preparationProduct struct {
	prepare func(Budget) (DrainPreparation, error)
	calls   atomic.Int32
	drained atomic.Int32
	closed  atomic.Int32
}

func (p *preparationProduct) Handle(context.Context, Request) (Reply, error) { return Reply{}, nil }
func (p *preparationProduct) Drain(Budget) error { p.drained.Add(1); return nil }

func (p *preparationProduct) Close(Budget) error { p.closed.Add(1); return nil }

func (p *preparationProduct) PrepareDrain(b Budget) (DrainPreparation, error) {
	p.calls.Add(1)
	return p.prepare(b)
}

type preparationFixture struct {
	commit func(Budget) error
	abort  func(Budget) error
}

func (p preparationFixture) Commit(b Budget) error { return p.commit(b) }
func (p preparationFixture) Abort(b Budget) error  { return p.abort(b) }

func preservingRuntime(t *testing.T, product Product) *serveRuntime {
	t.Helper()
	r := newServeRuntime(1024)
	r.policy = PreserveOwned
	r.grace = Grace(time.Second)
	if err := r.ready(product); err != nil {
		t.Fatal(err)
	}
	return r
}

func drainContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func requireNotStopped(t *testing.T, r *serveRuntime) {
	t.Helper()
	select {
	case <-r.stopped.Done():
		t.Fatal("preparation cancelled activation")
	default:
	}
}

func TestDrainPreparationBusyResumesBeforeRefusing(t *testing.T) {
	resumed := false
	product := &preparationProduct{prepare: func(Budget) (DrainPreparation, error) {
		return preparationFixture{commit: func(Budget) error { t.Error("busy preparation committed"); return nil }, abort: func(Budget) error { resumed = true; return nil }}, ErrDrainBusy
	}}
	r := preservingRuntime(t, product)
	if err := r.Drain(drainContext(t)); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("Drain=%v", err)
	}
	if !resumed || r.Phase().Phase != wire.PhaseReady {
		t.Fatalf("resume=%v phase=%v", resumed, r.Phase())
	}
	requireNotStopped(t, r)
	if product.drained.Load() != 0 || product.closed.Load() != 0 {
		t.Fatal("product shutdown ran before commitment")
	}
}

func TestDrainPreparationTimeoutRetainsMaintenanceUntilResume(t *testing.T) {
	aborting := make(chan struct{})
	resume := make(chan struct{})
	product := &preparationProduct{prepare: func(b Budget) (DrainPreparation, error) {
		ctx, cancel := b.Context(context.Background())
		defer cancel()
		<-ctx.Done()
		return preparationFixture{commit: func(Budget) error { t.Error("expired preparation committed"); return nil }, abort: func(Budget) error { close(aborting); <-resume; return nil }}, ctx.Err()
	}}
	r := preservingRuntime(t, product)
	r.grace = Grace(500 * time.Millisecond)
	result := make(chan error, 1)
	ctx := drainContext(t)
	go func() { result <- r.Drain(ctx) }()
	<-aborting
	if r.Phase().Phase != wire.PhaseMaintenance {
		t.Fatalf("phase=%v", r.Phase())
	}
	close(resume)
	if err := <-result; !errors.Is(err, ErrDrainPreparationTimeout) {
		t.Fatalf("Drain=%v", err)
	}
	if r.Phase().Phase != wire.PhaseReady {
		t.Fatalf("resumed phase=%v", r.Phase())
	}
	requireNotStopped(t, r)
}

func TestDrainPreparationFailedResumeNeverPublishesReady(t *testing.T) {
	product := &preparationProduct{prepare: func(Budget) (DrainPreparation, error) {
		return preparationFixture{commit: func(Budget) error { return nil }, abort: func(Budget) error { return errors.New("resume failed") }}, ErrDrainBusy
	}}
	r := preservingRuntime(t, product)
	for range 2 {
		if err := r.Drain(drainContext(t)); !errors.Is(err, ErrDrainBusy) {
			t.Fatalf("Drain=%v", err)
		}
	}
	if product.calls.Load() != 1 || r.Phase().Phase != wire.PhaseMaintenance {
		t.Fatalf("calls=%d phase=%v", product.calls.Load(), r.Phase())
	}
	requireNotStopped(t, r)
}

func TestDrainPreparationJoinsAndSurvivesRequesterCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	product := &preparationProduct{prepare: func(Budget) (DrainPreparation, error) {
		close(entered)
		<-release
		return preparationFixture{commit: func(Budget) error { return nil }, abort: func(Budget) error { t.Error("successful preparation aborted"); return nil }}, nil
	}}
	r := preservingRuntime(t, product)
	first, cancel := context.WithCancel(drainContext(t))
	firstResult := make(chan error, 1)
	go func() { firstResult <- r.Drain(first) }()
	<-entered
	cancel()
	if err := <-firstResult; !errors.Is(err, ErrDrainPreparationTimeout) {
		t.Fatalf("cancelled requester=%v", err)
	}
	requireNotStopped(t, r)
	second := make(chan error, 1)
	secondCtx := drainContext(t)
	go func() { second <- r.Drain(secondCtx) }()
	close(release)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if product.calls.Load() != 1 || r.Phase().Phase != wire.PhaseDraining {
		t.Fatalf("calls=%d phase=%v", product.calls.Load(), r.Phase())
	}
	select {
	case <-r.stopped.Done():
	default:
		t.Fatal("committed preparation did not begin shutdown")
	}
}

func TestPreservingActivationFailureRefusesBeforeShutdown(t *testing.T) {
	r := newServeRuntime(1024)
	r.policy = PreserveOwned
	r.grace = Grace(time.Second)
	if err := r.Drain(drainContext(t)); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("starting Drain=%v", err)
	}
	r.fail()
	if err := r.Drain(drainContext(t)); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("failed Drain=%v", err)
	}
	if r.Phase().Phase != wire.PhaseFailed {
		t.Fatal("failed startup became ready")
	}
	requireNotStopped(t, r)
}
