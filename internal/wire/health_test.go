package wire_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

func dialControl(t *testing.T, sock string) *wire.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Authorize: wiretest.AuthorizeTestServer, Lane: wire.LaneControl,
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = client.Abort(nil) })
	return client
}

func TestHealthVerbBothLanesAllPhases(t *testing.T) {
	serving := wire.Serving{
		PID:        4242,
		Build:      "build-digest",
		Generation: 7,
		Detail:     func() []byte { return []byte("product-detail") },
	}
	tests := []struct {
		name  string
		lane  wire.Lane
		phase wire.Phase
	}{
		{"control while starting", wire.LaneControl, wire.PhaseStarting},
		{"control while ready", wire.LaneControl, wire.PhaseReady},
		{"control while draining", wire.LaneControl, wire.PhaseDraining},
		{"business while starting", wire.LaneBusiness, wire.PhaseStarting},
		{"business while ready", wire.LaneBusiness, wire.PhaseReady},
		{"business while draining", wire.LaneBusiness, wire.PhaseDraining},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := wiretest.NewStubRuntime()
			sock, _ := startServer(t, rt, wire.Config{Serving: serving})
			var client *wire.Client
			if tt.lane == wire.LaneControl {
				client = dialControl(t, sock)
			} else {
				client = dialBusiness(t, sock)
			}
			// The draining case must answer on the established session, so the
			// phase moves only after attach.
			rt.SetPhase(tt.phase, nil)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			report, err := client.Health(ctx)
			if err != nil {
				t.Fatalf("Health() = %v", err)
			}
			want := wire.HealthReport{
				Phase:      tt.phase,
				Protocol:   wire.ProtocolVersion,
				Generation: serving.Generation,
				PID:        serving.PID,
				Build:      serving.Build,
				Detail:     []byte("product-detail"),
			}
			if report.Phase != want.Phase || report.Protocol != want.Protocol ||
				report.Generation != want.Generation || report.PID != want.PID ||
				report.Build != want.Build || !bytes.Equal(report.Detail, want.Detail) {
				t.Fatalf("Health() = %+v, want %+v", report, want)
			}
		})
	}
}

func TestHealthVerbAnswersBelowPhaseGate(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetPhase(wire.PhaseStarting, nil)
	sock, _ := startServer(t, rt, wire.Config{Serving: wire.Serving{PID: 1, Generation: 1}})
	client := dialBusiness(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Call(ctx, "test.echo.v1", nil)
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if rejection := result.Rejection(); rejection == nil {
		t.Fatal("business dispatch while starting was not rejected")
	}
	if _, err := client.Health(ctx); err != nil {
		t.Fatalf("Health() during starting = %v, want an answer below the gate", err)
	}
}

// pastConveyedDeadline advertises a deadline the serving side is already past
// while the caller's own context stays live, so the verb answers with that
// side's verdict rather than racing the caller's.
type pastConveyedDeadline struct{ context.Context }

func (pastConveyedDeadline) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }

func TestHealthVerbCarriesTheServingSidesTerminalVerdict(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{Serving: wire.Serving{PID: 1, Generation: 1}})
	client := dialControl(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Health(pastConveyedDeadline{Context: ctx})

	var terminal *wire.TerminalError
	if !errors.As(err, &terminal) {
		t.Fatalf("Health() = %v, want a *wire.TerminalError", err)
	}
	if terminal.Code != wire.ResponseCodeDeadlineExceeded {
		t.Errorf("TerminalError.Code = %q, want %q", terminal.Code, wire.ResponseCodeDeadlineExceeded)
	}
	if terminal.Message != "context deadline exceeded" {
		t.Errorf("TerminalError.Message = %q, want %q", terminal.Message, "context deadline exceeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(%v, context.DeadlineExceeded) = false, want true", err)
	}
}
