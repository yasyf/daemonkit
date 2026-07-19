package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

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
			peer := &fakePeer{health: []healthResult{{h: Health{Build: tt.incumbent, PID: 100}}}}
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

func TestTakeoverNoIncumbentBinds(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{err: fmt.Errorf("dial lifecycle: %w", ErrNoPeer)}}}
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

func TestTakeoverHealthFailuresNeverAuthorizeBindOrRelease(t *testing.T) {
	transportErr := errors.New("lifecycle trust failure")
	tests := []struct {
		name   string
		peer   *fakePeer
		config TakeoverConfig
	}{
		{
			name: "initial health failure",
			peer: &fakePeer{health: []healthResult{{err: transportErr}}},
			config: TakeoverConfig{
				Self: "2.0.0", Contract: ResourceOwner, WaitMode: SocketRelease,
			},
		},
		{
			name: "socket release health failure",
			peer: &fakePeer{health: []healthResult{
				{h: Health{Build: "1.0.0", PID: 100}},
				{err: transportErr},
			}},
			config: TakeoverConfig{
				Self: "2.0.0", Contract: ResourceOwner, WaitMode: SocketRelease,
			},
		},
		{
			name: "request daemon owner revalidation failure",
			peer: &fakePeer{health: []healthResult{
				{h: Health{Build: "1.0.0", PID: 100}},
				{err: transportErr},
			}},
			config: TakeoverConfig{
				Self: "2.0.0", Contract: RequestDaemon,
				prober: &fakeProber{results: []proberResult{{id: proc.Identity{PID: 100, StartTime: "1.2"}}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.config.Peer = tt.peer
			tt.config.clock = newAutoClock()
			got, err := Run(context.Background(), tt.config)
			if !errors.Is(err, transportErr) {
				t.Fatalf("Run err = %v, want transport failure", err)
			}
			if got != 0 {
				t.Fatalf("outcome = %s, want zero", got)
			}
		})
	}
}

func TestTakeoverRequestDaemonProbeFailureNeverAuthorizesBind(t *testing.T) {
	probeErr := errors.New("process table unavailable")
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "1.0.0", PID: 100}},
		{h: Health{Build: "1.0.0", PID: 100}},
	}}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: RequestDaemon,
		clock: newAutoClock(),
		prober: &fakeProber{results: []proberResult{
			{id: proc.Identity{PID: 100, StartTime: "1.2"}},
			{err: probeErr},
		}},
	}

	got, err := Run(context.Background(), cfg)
	if !errors.Is(err, probeErr) {
		t.Fatalf("Run err = %v, want probe failure", err)
	}
	if got != 0 {
		t.Fatalf("outcome = %s, want zero", got)
	}
}

func TestTakeoverRefusesProtocolMismatch(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{
		Build: "1.0.0", Protocol: 1, PID: 100,
	}}}}
	cfg := TakeoverConfig{Self: "2.0.0", Protocol: 2, Peer: peer}

	got, err := Run(context.Background(), cfg)
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("Run err = %v, want ErrProtocolMismatch", err)
	}
	if got != 0 {
		t.Fatalf("outcome = %s, want zero", got)
	}
	if shutdowns, handoffs := peer.counts(); shutdowns != 0 || handoffs != 0 {
		t.Fatalf("calls: shutdowns=%d handoffs=%d, want 0/0", shutdowns, handoffs)
	}
}

func TestTakeoverResourceOwnerHandoffNoSignals(t *testing.T) {
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "1.0.0", PID: 100}},
		{err: ErrNoPeer},
	}}
	sig := &fakeSignaler{}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: ResourceOwner, WaitMode: SocketRelease,
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

func TestTakeoverHandoffWaitModes(t *testing.T) {
	tests := []struct {
		name  string
		mode  WaitMode
		check func(*testing.T, TakeoverConfig, *fakePeer, *blockedProcess)
	}{
		{
			name: "pid exit waits for predecessor",
			mode: PIDExit,
			check: func(t *testing.T, cfg TakeoverConfig, peer *fakePeer, predecessor *blockedProcess) {
				prober := newGatedProcessProber()
				t.Cleanup(prober.unblock)
				cfg.prober = prober
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				result := make(chan takeoverResult, 1)
				go func() {
					outcome, err := Run(ctx, cfg)
					result <- takeoverResult{outcome: outcome, err: err}
				}()

				select {
				case <-prober.entered:
				case got := <-result:
					t.Fatalf("Run returned before waiting on live pid: outcome=%s err=%v", got.outcome, got.err)
				case <-time.After(5 * time.Second):
					t.Fatal("Run did not continue polling the live predecessor")
				}
				assertProcessAlive(t, predecessor.pid())
				select {
				case got := <-result:
					t.Fatalf("Run returned while PID probe was blocked: outcome=%s err=%v", got.outcome, got.err)
				default:
				}

				predecessor.exit(t)
				if _, err := proc.Probe(predecessor.pid()); !errors.Is(err, proc.ErrNoProcess) {
					t.Fatalf("Probe(exited pid) error = %v, want ErrNoProcess", err)
				}
				prober.unblock()
				select {
				case got := <-result:
					if got.err != nil {
						t.Fatalf("Run: %v", got.err)
					}
					if got.outcome != Bind {
						t.Errorf("outcome = %s, want bind", got.outcome)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Run did not return after predecessor exited")
				}
				if prober.calls != 3 {
					t.Errorf("PID probes = %d, want 3", prober.calls)
				}
				if calls := peer.healthCalls(); calls != 1 {
					t.Errorf("Health calls = %d, want 1", calls)
				}
			},
		},
		{
			name: "socket release does not wait for predecessor",
			mode: SocketRelease,
			check: func(t *testing.T, cfg TakeoverConfig, peer *fakePeer, predecessor *blockedProcess) {
				cfg.prober = &fakeProber{results: []proberResult{{err: errors.New("unexpected PID probe")}}}
				got, err := Run(context.Background(), cfg)
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if got != Bind {
					t.Errorf("outcome = %s, want bind", got)
				}
				assertProcessAlive(t, predecessor.pid())
				if calls := peer.healthCalls(); calls != 2 {
					t.Errorf("Health calls = %d, want 2", calls)
				}
				predecessor.exit(t)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predecessor := startBlockedProcess(t)
			peer := &fakePeer{health: []healthResult{
				{h: Health{Build: "1.0.0", PID: predecessor.pid()}},
				{err: ErrNoPeer},
			}}
			cfg := TakeoverConfig{
				Self: "2.0.0", Peer: peer, Contract: ResourceOwner, WaitMode: tt.mode,
				WaitTimeout: 5 * time.Second, clock: newAutoClock(),
			}

			tt.check(t, cfg, peer, predecessor)
			if shutdowns, handoffs := peer.counts(); shutdowns != 0 || handoffs != 1 {
				t.Errorf("calls: shutdowns=%d handoffs=%d, want 0/1", shutdowns, handoffs)
			}
		})
	}
}

func TestTakeoverRequestDaemonKillLadder(t *testing.T) {
	const start = "111.222"
	tests := []struct {
		name        string
		afterGrace  healthResult
		reprobe     proberResult
		signalErr   error
		wantSignals []signalRec
	}{
		{
			name:        "same instance persists: SIGKILL, ESRCH is success",
			afterGrace:  healthResult{h: Health{Build: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			signalErr:   syscall.ESRCH,
			wantSignals: []signalRec{{100, syscall.SIGKILL}},
		},
		{
			name:        "same instance persists: SIGKILL delivered",
			afterGrace:  healthResult{h: Health{Build: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			signalErr:   nil,
			wantSignals: []signalRec{{100, syscall.SIGKILL}},
		},
		{
			name:        "pid reused during grace: no kill",
			afterGrace:  healthResult{h: Health{Build: "1.0.0", PID: 100}},
			reprobe:     proberResult{id: proc.Identity{StartTime: "999.000"}},
			wantSignals: nil,
		},
		{
			name:        "socket released during grace: no kill",
			afterGrace:  healthResult{err: ErrNoPeer},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			wantSignals: nil,
		},
		{
			name:        "different owner answers: no kill",
			afterGrace:  healthResult{h: Health{Build: "1.0.0", PID: 200}},
			reprobe:     proberResult{id: proc.Identity{StartTime: start}},
			wantSignals: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{
				{h: Health{Build: "1.0.0", PID: 100}},
				tt.afterGrace,
			}}
			prober := &fakeProber{results: []proberResult{
				{id: proc.Identity{PID: 100, StartTime: start}},
				tt.reprobe,
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

func TestTakeoverRequestDaemonPIDExitWaitsAfterSIGKILL(t *testing.T) {
	const start = "111.222"
	tests := []struct {
		name  string
		after proberResult
	}{
		{name: "identity disappears", after: proberResult{err: proc.ErrNoProcess}},
		{name: "pid is reused", after: proberResult{id: proc.Identity{PID: 100, StartTime: "999.000"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := newSettlementGateProber(start, tt.after)
			peer := &fakePeer{health: []healthResult{
				{h: Health{Build: "1.0.0", PID: 100}},
				{h: Health{Build: "1.0.0", PID: 100}},
			}}
			sig := &fakeSignaler{}
			cfg := TakeoverConfig{
				Self: "2.0.0", Peer: peer, Contract: RequestDaemon, WaitMode: PIDExit,
				WaitTimeout: time.Second, clock: newAutoClock(), prober: prober, signaler: sig,
			}

			result := make(chan takeoverResult, 1)
			go func() {
				outcome, err := Run(context.Background(), cfg)
				result <- takeoverResult{outcome: outcome, err: err}
			}()
			<-prober.entered
			if calls := sig.calls(); !equalSignals(calls, []signalRec{{100, syscall.SIGKILL}}) {
				t.Fatalf("signals = %v, want SIGKILL", calls)
			}
			select {
			case got := <-result:
				t.Fatalf("Run returned before exit proof: outcome=%s err=%v", got.outcome, got.err)
			default:
			}
			close(prober.release)
			got := <-result
			if got.err != nil || got.outcome != Bind {
				t.Fatalf("Run = (%s, %v), want bind", got.outcome, got.err)
			}
		})
	}
}

func TestTakeoverRequestDaemonPIDExitTimesOutWithoutProof(t *testing.T) {
	const start = "111.222"
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "1.0.0", PID: 100}},
		{h: Health{Build: "1.0.0", PID: 100}},
	}}
	prober := &fakeProber{results: []proberResult{
		{id: proc.Identity{PID: 100, StartTime: start}},
		{id: proc.Identity{PID: 100, StartTime: start}},
		{id: proc.Identity{PID: 100, StartTime: start}},
	}}
	sig := &fakeSignaler{}
	outcome, err := Run(context.Background(), TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: RequestDaemon, WaitMode: PIDExit,
		WaitTimeout: 200 * time.Millisecond, clock: newAutoClock(), prober: prober, signaler: sig,
	})
	if !errors.Is(err, ErrReleaseTimeout) {
		t.Fatalf("Run = (%s, %v), want ErrReleaseTimeout", outcome, err)
	}
	if outcome == Bind {
		t.Fatal("takeover authorized bind without exit proof")
	}
}

func TestTakeoverRequestDaemonPIDExitReapsChild(t *testing.T) {
	predecessor := startBlockedProcess(t)
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "1.0.0", PID: predecessor.pid()}},
		{h: Health{Build: "1.0.0", PID: predecessor.pid()}},
	}}
	outcome, err := Run(context.Background(), TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: RequestDaemon, WaitMode: PIDExit,
		Grace: time.Millisecond, WaitTimeout: 5 * time.Second,
	})
	if err != nil || outcome != Bind {
		t.Fatalf("Run = (%s, %v), want bind", outcome, err)
	}
	if _, err := proc.Probe(predecessor.pid()); !errors.Is(err, proc.ErrNoProcess) {
		t.Fatalf("Probe(reaped child) = %v, want ErrNoProcess", err)
	}
}

func TestTakeoverRequestDaemonAlreadyGone(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Build: "1.0.0", PID: 100}}}}
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
			peer := &fakePeer{health: []healthResult{{h: Health{Build: "1.0.0", PID: tt.pid}}}}
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

func TestTakeoverResourceOwnerHandoffsWhileBusy(t *testing.T) {
	peer := &fakePeer{health: []healthResult{
		{h: Health{Build: "1.0.0", PID: 100, Busy: true}},
		{err: ErrNoPeer},
	}}
	sig := &fakeSignaler{}
	cfg := TakeoverConfig{
		Self: "2.0.0", Peer: peer, Contract: ResourceOwner, WaitMode: SocketRelease,
		clock: newAutoClock(), signaler: sig,
	}

	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != Bind {
		t.Fatalf("outcome = %s, want bind", got)
	}
	if shutdowns, handoffs := peer.counts(); shutdowns != 0 || handoffs != 1 {
		t.Fatalf("calls: shutdowns=%d handoffs=%d, want 0/1", shutdowns, handoffs)
	}
	if calls := sig.calls(); len(calls) != 0 {
		t.Fatalf("signals = %v, want none", calls)
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

type blockedProcess struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func startBlockedProcess(t *testing.T) *blockedProcess {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "read _")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("create predecessor stdin: %v", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		t.Fatalf("start predecessor: %v", err)
	}
	p := &blockedProcess{cmd: cmd, stdin: stdin}
	t.Cleanup(func() {
		_ = p.stdin.Close()
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
	})
	return p
}

func (p *blockedProcess) pid() int {
	return p.cmd.Process.Pid
}

func (p *blockedProcess) exit(t *testing.T) {
	t.Helper()
	if _, err := io.WriteString(p.stdin, "\n"); err != nil {
		t.Fatalf("release predecessor: %v", err)
	}
	if err := p.stdin.Close(); err != nil {
		t.Fatalf("close predecessor stdin: %v", err)
	}
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("wait for predecessor: %v", err)
	}
}

func assertProcessAlive(t *testing.T, pid int) {
	t.Helper()
	id, err := proc.Probe(pid)
	if err != nil {
		t.Fatalf("Probe(live pid): %v", err)
	}
	if id.PID != pid {
		t.Errorf("Probe(live pid).PID = %d, want %d", id.PID, pid)
	}
}

type takeoverResult struct {
	outcome Outcome
	err     error
}

type gatedProcessProber struct {
	calls     int
	entered   chan struct{}
	release   chan struct{}
	unblocked bool
}

type settlementGateProber struct {
	calls   int
	start   string
	after   proberResult
	entered chan struct{}
	release chan struct{}
}

func newSettlementGateProber(start string, after proberResult) *settlementGateProber {
	return &settlementGateProber{
		start: start, after: after, entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (p *settlementGateProber) probe(pid int) (proc.Identity, error) {
	p.calls++
	if p.calls == 3 {
		close(p.entered)
		<-p.release
		return p.after.id, p.after.err
	}
	return proc.Identity{PID: pid, StartTime: p.start}, nil
}

func newGatedProcessProber() *gatedProcessProber {
	return &gatedProcessProber{entered: make(chan struct{}), release: make(chan struct{})}
}

func (p *gatedProcessProber) probe(pid int) (proc.Identity, error) {
	p.calls++
	if p.calls == 3 {
		close(p.entered)
		<-p.release
	}
	return proc.Probe(pid)
}

func (p *gatedProcessProber) unblock() {
	if p.unblocked {
		return
	}
	p.unblocked = true
	close(p.release)
}
