package daemonkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

type refusingDrainRuntime struct {
	*wiretest.StubRuntime
	refuse func(context.Context) error
}

func (r refusingDrainRuntime) Drain(ctx context.Context) error { return r.refuse(ctx) }

func TestControlPreservationRefusalNeverObservesHostExit(t *testing.T) {
	for _, tt := range []struct {
		name    string
		refusal error
	}{
		{"busy", ErrDrainBusy}, {"timeout", ErrDrainPreparationTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt := refusingDrainRuntime{wiretest.NewStubRuntime(), func(context.Context) error { return tt.refusal }}
			serving := wire.Serving{PID: 4242, Build: "b1", Generation: 7, PreserveOwned: true}
			session := startControlServer(t, rt, serving)
			observed := 0
			control := &Control{session: session, pinned: proc.Identity{PID: 4242, Start: 100, Boot: 200}, generation: 7, preserveOwned: true, requirePreservation: true, observe: func(proc.Identity) (proc.Reap, bool, error) { observed++; return proc.ReapAbsent, true, nil }}
			_, err := control.Drain(drainContext(t), Expect{Build: "b1", Generation: 7})
			if !errors.Is(err, tt.refusal) || errors.Is(err, ErrUnsettled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Drain=%v", err)
			}
			if observed != 0 || rt.Phase().Phase != wire.PhaseReady {
				t.Fatalf("observed=%d phase=%v", observed, rt.Phase())
			}
		})
	}
}

func TestControlPendingPreparationTimeoutCannotBecomeUnsettled(t *testing.T) {
	rt := refusingDrainRuntime{wiretest.NewStubRuntime(), func(ctx context.Context) error { <-ctx.Done(); return ErrDrainPreparationTimeout }}
	serving := wire.Serving{PID: 4242, Build: "b1", Generation: 7, PreserveOwned: true}
	session := startControlServer(t, rt, serving)
	control := &Control{session: session, pinned: proc.Identity{PID: 4242, Start: 100, Boot: 200}, generation: 7, preserveOwned: true, observe: func(proc.Identity) (proc.Reap, bool, error) {
		t.Error("uncertain commitment observed exit")
		return proc.ReapAbsent, true, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := control.Drain(ctx, Expect{})
	if !errors.Is(err, ErrDrainPreparationTimeout) || errors.Is(err, ErrUnsettled) {
		t.Fatalf("Drain=%v", err)
	}
}

func TestControlPreservationRequiresSupportedIncumbent(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	session := startControlServer(t, rt, wire.Serving{PID: 4242, Build: "old", Generation: 7})
	control := &Control{session: session, pinned: proc.Identity{PID: 4242, Start: 100, Boot: 200}, generation: 7, preserveOwned: true, requirePreservation: true}
	_, err := control.Drain(drainContext(t), Expect{})
	if !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("Drain=%v", err)
	}
	select {
	case <-rt.Drained:
		t.Fatal("unsupported incumbent received drain")
	default:
	}
}
