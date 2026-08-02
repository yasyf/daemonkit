package daemonkit

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

func TestAssemblePin(t *testing.T) {
	pinned := proc.Identity{PID: 4242, Start: 100, Boot: 200}
	probeOK := func(pid int) (proc.Identity, error) {
		if pid != 4242 {
			t.Fatalf("probe pid = %d, want 4242", pid)
		}
		return pinned, nil
	}
	healthOK := func() (wire.HealthReport, error) {
		return wire.HealthReport{PID: 4242, Generation: 7, Build: "b1"}, nil
	}
	probeFailure := errors.New("probe failed")
	healthFailure := errors.New("health failed")
	tests := []struct {
		name    string
		self    int
		peerPID int
		probe   func(int) (proc.Identity, error)
		health  func() (wire.HealthReport, error)
		wantErr string
	}{
		{
			name: "refuses own pid", self: 4242, peerPID: 4242,
			probe:   func(int) (proc.Identity, error) { t.Fatal("probed self"); return proc.Identity{}, nil },
			health:  healthOK,
			wantErr: "refusing to pin own process",
		},
		{
			name: "refuses probe failure", self: 1, peerPID: 4242,
			probe:   func(int) (proc.Identity, error) { return proc.Identity{}, probeFailure },
			health:  healthOK,
			wantErr: "probe peer 4242",
		},
		{
			name: "refuses health failure", self: 1, peerPID: 4242,
			probe:   probeOK,
			health:  func() (wire.HealthReport, error) { return wire.HealthReport{}, healthFailure },
			wantErr: "health self-attestation",
		},
		{
			name: "refuses self-attestation mismatch", self: 1, peerPID: 4242,
			probe:   probeOK,
			health:  func() (wire.HealthReport, error) { return wire.HealthReport{PID: 9999, Generation: 7}, nil },
			wantErr: "peer self-attests pid 9999, socket observed 4242",
		},
		{
			name: "pins on agreement", self: 1, peerPID: 4242,
			probe:  probeOK,
			health: healthOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, report, err := assemblePin(tt.self, tt.peerPID, tt.probe, tt.health)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("assemblePin() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("assemblePin() = %v", err)
			}
			if id != pinned {
				t.Fatalf("pinned = %+v, want %+v", id, pinned)
			}
			if report.Generation != 7 {
				t.Fatalf("generation = %d, want 7", report.Generation)
			}
		})
	}
}

func TestExpectMismatch(t *testing.T) {
	tests := []struct {
		name       string
		expect     Expect
		build      string
		generation uint64
		want       bool
	}{
		{"zero expect is unconditional", Expect{}, "b1", 7, false},
		{"matching build", Expect{Build: "b1"}, "b1", 7, false},
		{"mismatched build", Expect{Build: "b2"}, "b1", 7, true},
		{"matching generation", Expect{Generation: 7}, "b1", 7, false},
		{"mismatched generation", Expect{Generation: 8}, "b1", 7, true},
		{"both match", Expect{Build: "b1", Generation: 7}, "b1", 7, false},
		{"build matches generation does not", Expect{Build: "b1", Generation: 8}, "b1", 7, true},
		{"generation matches build does not", Expect{Build: "b2", Generation: 7}, "b1", 7, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.expect.mismatch(tt.build, tt.generation); got != tt.want {
				t.Fatalf("mismatch(%q, %d) = %t, want %t", tt.build, tt.generation, got, tt.want)
			}
		})
	}
}

func TestReapMirrorsProc(t *testing.T) {
	tests := []struct {
		name string
		proc proc.Reap
		want Reap
	}{
		{"absent", proc.ReapAbsent, ReapAbsent},
		{"cross boot", proc.ReapCrossBoot, ReapCrossBoot},
		{"reused", proc.ReapReused, ReapReused},
		{"terminated", proc.ReapTerminated, ReapTerminated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Reap(tt.proc); got != tt.want {
				t.Fatalf("Reap(proc.%d) = %d, want %d", tt.proc, got, tt.want)
			}
		})
	}
}

func startControlServer(t *testing.T, rt wire.Runtime, serving wire.Serving) *wire.Client {
	t.Helper()
	server, err := wire.NewServer(rt, wire.Config{Schemas: wire.Schemas{"test.v1"}, Serving: serving})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sock := filepath.Join(wiretest.SocketDir(t), "srv")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	ctx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	session, err := wire.NewClient(ctx, wire.ClientConfig{Dial: wire.UnixDialer(sock), Authorize: wiretest.AuthorizeTestServer, Lane: wire.LaneControl})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = session.Abort(nil) })
	return session
}

func TestControlDrain(t *testing.T) {
	serving := wire.Serving{PID: 4242, Build: "b1", Generation: 7}
	pinned := proc.Identity{PID: 4242, Start: 100, Boot: 200}
	absent := func(id proc.Identity) (proc.Reap, bool, error) {
		if id != pinned {
			t.Fatalf("observed %+v, want the pinned identity", id)
		}
		return proc.ReapAbsent, true, nil
	}
	live := func(proc.Identity) (proc.Reap, bool, error) { return 0, false, nil }
	tests := []struct {
		name         string
		generation   uint64
		expect       Expect
		observe      func(proc.Identity) (proc.Reap, bool, error)
		wantReap     Reap
		wantErr      error
		wantErrText  string
		wantDispatch bool
	}{
		{
			name:       "wrong incumbent refuses before dispatch",
			generation: 7, expect: Expect{Build: "b2"},
			observe: absent, wantErr: ErrWrongIncumbent,
		},
		{
			name:       "wrong generation refuses before dispatch",
			generation: 7, expect: Expect{Generation: 8},
			observe: absent, wantErr: ErrWrongIncumbent,
		},
		{
			name:       "moved pin refuses before dispatch",
			generation: 8, expect: Expect{},
			observe: absent, wantErrText: "pinned incumbent moved",
		},
		{
			name:       "drains and observes absence",
			generation: 7, expect: Expect{Build: "b1", Generation: 7},
			observe: absent, wantReap: ReapAbsent, wantDispatch: true,
		},
		{
			name:       "unsettled joins ctx error",
			generation: 7, expect: Expect{},
			observe: live, wantErr: ErrUnsettled, wantDispatch: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := wiretest.NewStubRuntime()
			session := startControlServer(t, rt, serving)
			control := &Control{session: session, pinned: pinned, generation: tt.generation, observe: tt.observe}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			stopped, err := control.Drain(ctx, tt.expect)

			dispatched := true
			select {
			case <-rt.Drained:
			default:
				dispatched = false
			}
			if dispatched != tt.wantDispatch {
				t.Fatalf("drain verb dispatched = %t, want %t", dispatched, tt.wantDispatch)
			}
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Drain() error = %v, want %v", err, tt.wantErr)
				}
				if errors.Is(tt.wantErr, ErrUnsettled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Drain() error = %v, want joined ctx.Err()", err)
				}
			case tt.wantErrText != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErrText) {
					t.Fatalf("Drain() error = %v, want %q", err, tt.wantErrText)
				}
			default:
				if err != nil {
					t.Fatalf("Drain() = %v", err)
				}
				if stopped.Reap != tt.wantReap {
					t.Fatalf("Reap = %d, want %d", stopped.Reap, tt.wantReap)
				}
				want := Health{Phase: PhaseReady, Protocol: wire.ProtocolVersion, Generation: 7, PID: 4242, Build: "b1"}
				if got := stopped.Before; got.Phase != want.Phase || got.PID != want.PID ||
					got.Generation != want.Generation || got.Build != want.Build || got.Protocol != want.Protocol {
					t.Fatalf("Before = %+v, want %+v", stopped.Before, want)
				}
			}
		})
	}
}

func TestControlHealthEnforcesThePin(t *testing.T) {
	serving := wire.Serving{PID: 4242, Build: "b1", Generation: 7}
	tests := []struct {
		name       string
		pinned     proc.Identity
		generation uint64
		wantErr    string
	}{
		{name: "pinned incumbent", pinned: proc.Identity{PID: 4242}, generation: 7},
		{
			name: "moved pid", pinned: proc.Identity{PID: 9999}, generation: 7,
			wantErr: "pinned incumbent moved",
		},
		{
			name: "moved generation", pinned: proc.Identity{PID: 4242}, generation: 8,
			wantErr: "pinned incumbent moved",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := startControlServer(t, wiretest.NewStubRuntime(), serving)
			control := &Control{session: session, pinned: tt.pinned, generation: tt.generation}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			health, err := control.Health(ctx)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Health() = %+v, %v, want %q", health, err, tt.wantErr)
				}
				if health.PID != 0 || health.Generation != 0 || health.Build != "" {
					t.Fatalf("Health() = %+v, want no observation past the refusal", health)
				}
				return
			}
			if err != nil {
				t.Fatalf("Health() = %v", err)
			}
			want := Health{Phase: PhaseReady, Protocol: wire.ProtocolVersion, Generation: 7, PID: 4242, Build: "b1"}
			if health.Phase != want.Phase || health.PID != want.PID || health.Generation != want.Generation ||
				health.Build != want.Build || health.Protocol != want.Protocol {
				t.Fatalf("Health() = %+v, want %+v", health, want)
			}
		})
	}
}

func TestControlCloseRequiresDeadline(t *testing.T) {
	session := startControlServer(t, wiretest.NewStubRuntime(), wire.Serving{PID: 4242, Generation: 7})
	control := &Control{session: session, pinned: proc.Identity{PID: 4242}, generation: 7}
	if err := control.Close(context.Background()); err == nil {
		t.Fatal("Close() without a deadline succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := control.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}
