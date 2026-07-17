package daemon

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

// TestTakeoverSameOrNewerExitsSelf: a same-version or newer incumbent makes the
// successor exit without a single shutdown, signal, or handoff. Ties never evict.
func TestTakeoverSameOrNewerExitsSelf(t *testing.T) {
	tests := []struct {
		name      string
		self      string
		incumbent string
	}{
		{"same version", "1.2.3", "1.2.3"},
		{"incumbent newer", "1.0.0", "2.0.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{{h: Health{Version: tt.incumbent, PID: 100}}}}
			sig := &fakeSignaler{}
			cfg := TakeoverConfig{Self: tt.self, Peer: peer, clock: newAutoClock(), prober: &fakeProber{results: []proberResult{{}}}, signaler: sig}

			got, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got != ExitSelf {
				t.Errorf("outcome = %s, want exit-self", got)
			}
			if sd, ho := peer.counts(); sd != 0 || ho != 0 {
				t.Errorf("calls: shutdowns=%d handoffs=%d, want 0/0", sd, ho)
			}
			if calls := sig.calls(); len(calls) != 0 {
				t.Errorf("signals = %v, want none", calls)
			}
		})
	}
}

// TestTakeoverNoIncumbentBinds: an unreachable socket means nothing to take over,
// so the caller may bind; no eviction is attempted.
func TestTakeoverNoIncumbentBinds(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{err: errors.New("connection refused")}}}
	sig := &fakeSignaler{}
	cfg := TakeoverConfig{Self: "2.0.0", Peer: peer, clock: newAutoClock(), signaler: sig}

	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != Bind {
		t.Errorf("outcome = %s, want bind", got)
	}
	if sd, ho := peer.counts(); sd != 0 || ho != 0 {
		t.Errorf("calls: shutdowns=%d handoffs=%d, want 0/0", sd, ho)
	}
	if calls := sig.calls(); len(calls) != 0 {
		t.Errorf("signals = %v, want none", calls)
	}
}

// TestTakeoverHandoffPathNoSignals: a strictly-older incumbent advertising the
// handoff feature is asked to hand off, then the successor waits for release and
// binds — never a shutdown or a signal.
func TestTakeoverHandoffPathNoSignals(t *testing.T) {
	peer := &fakePeer{health: []healthResult{
		{h: Health{Version: "1.0.0", PID: 100, Features: []string{FeatureHandoff}}},
		{err: errors.New("released")}, // release-wait sees the socket gone
	}}
	sig := &fakeSignaler{}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, WaitMode: SocketRelease,
		clock: newAutoClock(), prober: &fakeProber{results: []proberResult{{}}}, signaler: sig,
	}

	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != Bind {
		t.Errorf("outcome = %s, want bind", got)
	}
	if sd, ho := peer.counts(); sd != 0 || ho != 1 {
		t.Errorf("calls: shutdowns=%d handoffs=%d, want 0/1", sd, ho)
	}
	if calls := sig.calls(); len(calls) != 0 {
		t.Errorf("signals = %v, want none on the handoff path", calls)
	}
}

// TestTakeoverRequestDaemonKillLadder exercises the Shutdown -> grace ->
// PID-revalidated SIGKILL ladder and every branch that must NOT kill.
func TestTakeoverRequestDaemonKillLadder(t *testing.T) {
	const start = "111.222"
	tests := []struct {
		name        string
		afterGrace  healthResult // Health re-read after the grace window
		reprobe     proberResult // probe of the victim after grace
		signalErr   error
		wantSignals []signalRec
	}{
		{
			name:        "same instance persists: SIGKILL, ESRCH is success",
			afterGrace:  healthResult{h: Health{Version: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			signalErr:   syscall.ESRCH,
			wantSignals: []signalRec{{100, syscall.SIGKILL}},
		},
		{
			name:        "same instance persists: SIGKILL delivered",
			afterGrace:  healthResult{h: Health{Version: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			signalErr:   nil,
			wantSignals: []signalRec{{100, syscall.SIGKILL}},
		},
		{
			name:        "pid reused during grace: no kill",
			afterGrace:  healthResult{h: Health{Version: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: "999.000"}},
			wantSignals: nil,
		},
		{
			name:        "socket released during grace: no kill",
			afterGrace:  healthResult{err: errors.New("released")},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			wantSignals: nil,
		},
		{
			name:        "different owner answers: no kill",
			afterGrace:  healthResult{h: Health{Version: "1.0.0", PID: 200}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			wantSignals: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{
				{h: Health{Version: "1.0.0", PID: 100}}, // initial probe
				tt.afterGrace,
			}}
			prober := &fakeProber{results: []proberResult{
				{id: proc.Identity{PID: 100, StartTime: start}}, // pre-shutdown probe
				tt.reprobe, // post-grace revalidation
			}}
			sig := &fakeSignaler{err: tt.signalErr}
			cfg := TakeoverConfig{
				Self: "2.0.0", Peer: peer, Contract: RequestDaemon,
				clock: newAutoClock(), prober: prober, signaler: sig,
			}

			got, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got != Bind {
				t.Errorf("outcome = %s, want bind", got)
			}
			if sd, _ := peer.counts(); sd != 1 {
				t.Errorf("shutdowns = %d, want 1", sd)
			}
			if calls := sig.calls(); !equalSignals(calls, tt.wantSignals) {
				t.Errorf("signals = %v, want %v", calls, tt.wantSignals)
			}
		})
	}
}

// TestTakeoverRequestDaemonAlreadyGone: an incumbent that vanished before its
// pre-shutdown probe is never shut down or signaled; the caller just binds.
func TestTakeoverRequestDaemonAlreadyGone(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Version: "1.0.0", PID: 100}}}}
	prober := &fakeProber{results: []proberResult{{err: proc.ErrNoProcess}}}
	sig := &fakeSignaler{}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: RequestDaemon,
		clock: newAutoClock(), prober: prober, signaler: sig,
	}

	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != Bind {
		t.Errorf("outcome = %s, want bind", got)
	}
	if sd, _ := peer.counts(); sd != 0 {
		t.Errorf("shutdowns = %d, want 0 (already gone)", sd)
	}
	if calls := sig.calls(); len(calls) != 0 {
		t.Errorf("signals = %v, want none", calls)
	}
}

// TestTakeoverRefusesSelfAndInit: a RequestDaemon eviction refuses to target
// pid<=1 or the successor's own pid, before any shutdown.
func TestTakeoverRefusesSelfAndInit(t *testing.T) {
	tests := []struct {
		name string
		pid  int
	}{
		{"init", 1},
		{"pid zero", 0},
		{"self", os.Getpid()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{{h: Health{Version: "1.0.0", PID: tt.pid}}}}
			sig := &fakeSignaler{}
			cfg := TakeoverConfig{
				Self: "2.0.0", Peer: peer, Contract: RequestDaemon,
				clock: newAutoClock(), prober: &fakeProber{results: []proberResult{{}}}, signaler: sig,
			}

			_, err := Run(context.Background(), cfg)
			if !errors.Is(err, ErrRefuseVictim) {
				t.Fatalf("err = %v, want ErrRefuseVictim", err)
			}
			if sd, _ := peer.counts(); sd != 0 {
				t.Errorf("shutdowns = %d, want 0 (refused before shutdown)", sd)
			}
			if calls := sig.calls(); len(calls) != 0 {
				t.Errorf("signals = %v, want none", calls)
			}
		})
	}
}

// TestTakeoverResourceOwnerDefers: an older ResourceOwner is never killed for
// being older — busy with no proof, or proof of "still alive", both defer.
func TestTakeoverResourceOwnerDefers(t *testing.T) {
	tests := []struct {
		name          string
		busy          bool
		confirmedDead func(context.Context, Health) (bool, error)
	}{
		{"busy, no death proof", true, nil},
		{"idle, no death proof", false, nil},
		{"proof says still alive", true, func(context.Context, Health) (bool, error) { return false, nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{{h: Health{Version: "1.0.0", PID: 100, Busy: tt.busy}}}}
			sig := &fakeSignaler{}
			cfg := TakeoverConfig{
				Self: "2.0.0", Peer: peer, Contract: ResourceOwner, ConfirmedDead: tt.confirmedDead,
				clock: newAutoClock(), prober: &fakeProber{results: []proberResult{{}}}, signaler: sig,
			}

			got, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got != Defer {
				t.Errorf("outcome = %s, want defer", got)
			}
			if sd, _ := peer.counts(); sd != 0 {
				t.Errorf("shutdowns = %d, want 0 (never shut a ResourceOwner)", sd)
			}
			if calls := sig.calls(); len(calls) != 0 {
				t.Errorf("signals = %v, want none without death proof", calls)
			}
		})
	}
}

// TestTakeoverResourceOwnerForcesOnDeathProof: proof of death lets the takeover
// force in, revalidating {pid,start_time} before the SIGKILL.
func TestTakeoverResourceOwnerForcesOnDeathProof(t *testing.T) {
	const start = "111.222"
	peer := &fakePeer{health: []healthResult{
		{h: Health{Version: "1.0.0", PID: 100}},
		{h: Health{Version: "1.0.0", PID: 100}}, // still answers; revalidation matches
	}}
	prober := &fakeProber{results: []proberResult{
		{id: proc.Identity{StartTime: start}},
		{id: proc.Identity{StartTime: start}},
	}}
	sig := &fakeSignaler{err: syscall.ESRCH}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: ResourceOwner,
		ConfirmedDead: func(context.Context, Health) (bool, error) { return true, nil },
		clock:         newAutoClock(), prober: prober, signaler: sig,
	}

	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != Bind {
		t.Errorf("outcome = %s, want bind", got)
	}
	if want := []signalRec{{100, syscall.SIGKILL}}; !equalSignals(sig.calls(), want) {
		t.Errorf("signals = %v, want %v", sig.calls(), want)
	}
}

func equalSignals(got, want []signalRec) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
