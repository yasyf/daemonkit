package daemonkit

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

func TestAssemblePin(t *testing.T) {
	self := os.Getpid()
	other := os.Getppid()
	const unheld = 1 << 30
	tests := []struct {
		name       string
		self       int
		peerPID    int
		servingPID int
		cancelled  bool
		wantErr    string
	}{
		{
			name: "refuses own pid", self: self, peerPID: self, servingPID: self,
			wantErr: "refusing to pin own process",
		},
		{
			name: "refuses probe failure", self: self, peerPID: unheld, servingPID: unheld,
			wantErr: "probe peer " + strconv.Itoa(unheld),
		},
		{
			name: "refuses health failure", self: other, peerPID: self, servingPID: self, cancelled: true,
			wantErr: "health self-attestation",
		},
		{
			name: "refuses self-attestation mismatch", self: other, peerPID: self, servingPID: unheld,
			wantErr: "peer self-attests pid " + strconv.Itoa(unheld) + ", socket observed " + strconv.Itoa(self),
		},
		{name: "pins on agreement", self: other, peerPID: self, servingPID: self},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serving := wire.Serving{PID: tt.servingPID, Build: "b1", Generation: 7}
			session := startSessionServer(t, wiretest.NewStubRuntime(), serving)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if tt.cancelled {
				cancel()
			}
			id, report, err := assemblePin(ctx, tt.self, tt.peerPID, session)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("assemblePin() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("assemblePin() = %v", err)
			}
			want, err := proc.ProbeIdentity(tt.peerPID)
			if err != nil {
				t.Fatalf("ProbeIdentity(%d) = %v", tt.peerPID, err)
			}
			if id != want {
				t.Fatalf("pinned = %+v, want %+v", id, want)
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

func startSessionServer(t *testing.T, rt wire.Runtime, serving wire.Serving) *wire.Client {
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
	session, err := wire.NewClient(ctx, wire.ClientConfig{Dial: wire.UnixDialer(sock)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = session.Abort(nil) })
	return session
}
