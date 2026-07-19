package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type runtimeEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *runtimeEvents) add(event string) {
	e.mu.Lock()
	e.events = append(e.events, event)
	e.mu.Unlock()
}

func (e *runtimeEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func (e *runtimeEvents) index(event string) int {
	for i, got := range e.snapshot() {
		if got == event {
			return i
		}
	}
	return -1
}

func (e *runtimeEvents) wait(t *testing.T, event string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for e.index(event) < 0 {
		if time.Now().After(deadline) {
			t.Fatalf("event %q did not occur: %v", event, e.snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

type runtimeAdmission struct {
	events *runtimeEvents

	mu        sync.Mutex
	draining  bool
	inflight  int
	settled   chan struct{}
	settleErr error
}

func (a *runtimeAdmission) Admit() (func(), error) {
	return a.admit(false)
}

func (a *runtimeAdmission) AdmitLifecycle() (func(), error) {
	return a.admit(true)
}

func (a *runtimeAdmission) admit(lifecycle bool) (func(), error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.draining && !lifecycle {
		return nil, errors.New("draining")
	}
	a.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.inflight--
			if a.inflight == 0 && a.settled != nil {
				close(a.settled)
				a.settled = nil
			}
			a.mu.Unlock()
		})
	}, nil
}

func (a *runtimeAdmission) Close() {
	a.events.add("admission-close")
	a.mu.Lock()
	a.draining = true
	a.mu.Unlock()
}

func (a *runtimeAdmission) Draining() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.draining
}

func (a *runtimeAdmission) Settle(ctx context.Context) error {
	a.events.add("admission-settle")
	a.mu.Lock()
	if a.inflight == 0 {
		err := a.settleErr
		a.mu.Unlock()
		return err
	}
	if a.settled == nil {
		a.settled = make(chan struct{})
	}
	settled := a.settled
	err := a.settleErr
	a.mu.Unlock()
	select {
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	case <-settled:
		return err
	}
}

type runtimeServer struct {
	events   *runtimeEvents
	started  chan struct{}
	serveErr error
	closeErr error

	mu       sync.Mutex
	listener net.Listener
}

func newRuntimeServer(events *runtimeEvents) *runtimeServer {
	return &runtimeServer{events: events, started: make(chan struct{})}
}

func (s *runtimeServer) Serve(
	ctx context.Context,
	listener net.Listener,
	_, _ func() (func(), error),
) error {
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	s.events.add("serve")
	close(s.started)
	if s.serveErr != nil {
		return s.serveErr
	}
	<-ctx.Done()
	s.events.add("serve-exit")
	return ctx.Err()
}

func (s *runtimeServer) CloseIntake() error {
	s.events.add("server-close-intake")
	s.mu.Lock()
	listener := s.listener
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	return s.closeErr
}

type runtimeWorkers struct {
	events      *runtimeEvents
	waitErr     error
	waitGate    <-chan struct{}
	waitContext bool
}

func (w *runtimeWorkers) Close()  { w.events.add("workers-close") }
func (w *runtimeWorkers) Cancel() { w.events.add("workers-cancel") }
func (w *runtimeWorkers) Wait(ctx context.Context) error {
	w.events.add("workers-wait")
	if w.waitContext {
		<-ctx.Done()
	}
	if w.waitGate != nil {
		<-w.waitGate
	}
	return errors.Join(w.waitErr, ctx.Err())
}

type runtimeCloser struct {
	events *runtimeEvents
	event  string
	err    error
}

func (c *runtimeCloser) Close() error {
	c.events.add(c.event)
	return c.err
}

func runtimeTestConfig(t *testing.T, peer Peer) (RuntimeConfig, *runtimeEvents, *runtimeServer, *runtimeAdmission, *runtimeWorkers) {
	t.Helper()
	dir, err := os.MkdirTemp("", "dkr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	events := &runtimeEvents{}
	server := newRuntimeServer(events)
	admission := &runtimeAdmission{events: events}
	workers := &runtimeWorkers{events: events}
	return RuntimeConfig{
		Socket:    filepath.Join(dir, "s"),
		Build:     "v2.0.0",
		Protocol:  2,
		Peer:      peer,
		Contract:  RequestDaemon,
		WaitMode:  SocketRelease,
		Admission: admission,
		Server:    server,
		Workers:   workers,
		State:     &runtimeCloser{events: events, event: "state-close"},
		Resources: &runtimeCloser{events: events, event: "resources-close"},
		Activate: func(context.Context) error {
			events.add("activate")
			return nil
		},
		Signals: make(chan os.Signal),
	}, events, server, admission, workers
}

func absentRuntimePeer() Peer {
	return &fakePeer{health: []healthResult{{err: ErrNoPeer}}}
}

func startRuntime(ctx context.Context, t *testing.T, runtime *Runtime, server *runtimeServer) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-server.started:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not start session server")
	}
	return done
}

func waitRuntime(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
		return nil
	}
}

func TestRuntimeShutdownOrderAndDoubleRunClose(t *testing.T) {
	cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := waitRuntime(t, runDone); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrRuntimeStarted) {
		t.Fatalf("second Run = %v, want ErrRuntimeStarted", err)
	}
	want := []string{
		"activate",
		"serve",
		"admission-close",
		"server-close-intake",
		"admission-settle",
		"workers-close",
		"workers-wait",
		"serve-exit",
		"state-close",
		"resources-close",
	}
	if got := events.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeHealthUsesExactIdentityAndDrainState(t *testing.T) {
	cfg, _, _, admission, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Busy = func() bool { return true }
	cfg.HealthState = func() State { return StateDegraded }
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}

	health, err := runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Build != cfg.Build || health.Protocol != cfg.Protocol {
		t.Fatalf("identity = {%q %d}, want {%q %d}", health.Build, health.Protocol, cfg.Build, cfg.Protocol)
	}
	if health.State != StateDegraded || !health.Busy || health.Draining {
		t.Fatalf("health = %+v", health)
	}
	admission.Close()
	health, err = runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !health.Draining {
		t.Fatalf("health after close = %+v, want draining", health)
	}
}

func TestRuntimeSettlesAdmissionBeforeClosingWorkers(t *testing.T) {
	cfg, events, server, admission, _ := runtimeTestConfig(t, absentRuntimePeer())
	doneRequest, err := admission.Admit()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	closeDone := make(chan error, 1)
	go func() { closeDone <- runtime.Close(context.Background()) }()
	events.wait(t, "admission-settle")
	if got := events.index("workers-close"); got >= 0 {
		t.Fatalf("workers closed before admitted request settled: %v", events.snapshot())
	}
	doneRequest()
	if err := <-closeDone; err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := waitRuntime(t, runDone); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if events.index("admission-settle") >= events.index("workers-close") {
		t.Fatalf("settlement order = %v", events.snapshot())
	}
}

func TestRuntimeTakeoverVersionOrdering(t *testing.T) {
	tests := []struct {
		name        string
		peerVersion string
		serve       bool
	}{
		{name: "same exits", peerVersion: "v2.0.0"},
		{name: "newer exits", peerVersion: "v3.0.0"},
		{name: "older hands off", peerVersion: "v1.0.0", serve: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := &fakePeer{health: []healthResult{{h: Health{
				Build: tt.peerVersion, Protocol: 2, PID: 222,
			}}}}
			if tt.serve {
				peer.health = append(peer.health, healthResult{err: ErrNoPeer})
			}
			cfg, events, server, _, _ := runtimeTestConfig(t, peer)
			if tt.serve {
				cfg.Contract = ResourceOwner
				cfg.Handoff = func(context.Context) error { return nil }
			}
			runtime, err := NewRuntime(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !tt.serve {
				if err := runtime.Run(context.Background()); err != nil {
					t.Fatalf("Run = %v", err)
				}
				select {
				case <-server.started:
					t.Fatal("same-or-newer peer must prevent serving")
				default:
				}
				if events.index("activate") >= 0 {
					t.Fatalf("same-or-newer peer activated shared state: %v", events.snapshot())
				}
				return
			}
			runDone := startRuntime(context.Background(), t, runtime, server)
			if err := runtime.Close(context.Background()); err != nil {
				t.Fatalf("Close = %v", err)
			}
			if err := waitRuntime(t, runDone); err != nil {
				t.Fatalf("Run = %v", err)
			}
			_, handoffs := peer.counts()
			if handoffs != 1 {
				t.Fatalf("handoffs = %d, want 1", handoffs)
			}
		})
	}
}

func TestRuntimeActivatesAfterListenerOwnershipBeforeServing(t *testing.T) {
	cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Activate = func(context.Context) error {
		info, err := os.Lstat(cfg.Socket)
		if err != nil {
			return fmt.Errorf("activation listener: %w", err)
		}
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("activation ran without listener ownership")
		}
		lock, lockErr := (proc.FileLockSpec{
			Path: cfg.Socket + ".lock", Mode: proc.FileLockExclusive, Deadline: time.Second,
		}).TryAcquire()
		if lock != nil {
			_ = lock.Close()
		}
		if !errors.Is(lockErr, proc.ErrLockBusy) {
			return fmt.Errorf("activation listener lock was not held: %w", lockErr)
		}
		select {
		case <-server.started:
			return errors.New("session server started before activation")
		default:
		}
		events.add("activate")
		return nil
	}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := waitRuntime(t, runDone); err != nil {
		t.Fatal(err)
	}
	if events.index("activate") < 0 || events.index("activate") >= events.index("serve") {
		t.Fatalf("activation order = %v", events.snapshot())
	}
}

func TestRuntimeActivationFailureNeverServesAndClosesOwnedState(t *testing.T) {
	want := errors.New("activate failed")
	cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Activate = func(context.Context) error {
		events.add("activate")
		return want
	}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run = %v, want activation failure", err)
	}
	select {
	case <-server.started:
		t.Fatal("session server started after activation failure")
	default:
	}
	wantEvents := []string{
		"activate", "admission-close", "server-close-intake", "admission-settle",
		"workers-close", "workers-wait", "state-close", "resources-close",
	}
	if got := events.snapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("activation failure events = %v, want %v", got, wantEvents)
	}
	if _, err := os.Lstat(cfg.Socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activation failure retained listener socket: %v", err)
	}
}

func TestRuntimeActivationPanicNeverServesAndClosesOwnedState(t *testing.T) {
	cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Activate = func(context.Context) error {
		events.add("activate")
		panic("activation exploded")
	}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "activation exploded") {
		t.Fatalf("Run = %v, want recovered activation panic", err)
	}
	select {
	case <-server.started:
		t.Fatal("session server started after activation panic")
	default:
	}
	wantEvents := []string{
		"activate", "admission-close", "server-close-intake", "admission-settle",
		"workers-close", "workers-wait", "state-close", "resources-close",
	}
	if got := events.snapshot(); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("activation panic events = %v, want %v", got, wantEvents)
	}
}

func TestRuntimeInterruptionsCancelAndJoinActivationBeforeCleanup(t *testing.T) {
	tests := []struct {
		name    string
		trigger func(*Runtime, context.CancelFunc, chan<- os.Signal) error
		wantErr error
	}{
		{
			name: "shutdown",
			trigger: func(runtime *Runtime, _ context.CancelFunc, _ chan<- os.Signal) error {
				return runtime.Shutdown(context.Background())
			},
		},
		{
			name: "parent context",
			trigger: func(_ *Runtime, cancel context.CancelFunc, _ chan<- os.Signal) error {
				cancel()
				return nil
			},
			wantErr: context.Canceled,
		},
		{
			name: "signal",
			trigger: func(_ *Runtime, _ context.CancelFunc, signals chan<- os.Signal) error {
				signals <- syscall.SIGTERM
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
			entered := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			cfg.Activate = func(ctx context.Context) error {
				events.add("activate")
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				return ctx.Err()
			}
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			signals := make(chan os.Signal, 1)
			cfg.Signals = signals
			runtime, err := NewRuntime(cfg)
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- runtime.Run(parent) }()
			<-entered
			if err := test.trigger(runtime, cancelParent, signals); err != nil {
				t.Fatal(err)
			}
			<-canceled
			if events.index("workers-close") >= 0 || events.index("state-close") >= 0 {
				t.Fatalf("cleanup started before activation joined: %v", events.snapshot())
			}
			select {
			case <-server.started:
				t.Fatal("session server started during interrupted activation")
			default:
			}
			close(release)
			err = waitRuntime(t, runDone)
			if test.wantErr == nil && err != nil {
				t.Fatalf("Run = %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Run = %v, want %v", err, test.wantErr)
			}
			if events.index("activate") >= events.index("workers-close") ||
				events.index("workers-close") >= events.index("state-close") {
				t.Fatalf("interrupted activation cleanup order = %v", events.snapshot())
			}
		})
	}
}

func TestRuntimeRejectsProtocolMismatch(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Build: "v1.0.0", Protocol: 1, PID: 222}}}}
	cfg, events, _, _, _ := runtimeTestConfig(t, peer)
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Run(context.Background())
	if !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("Run = %v, want ErrProtocolMismatch", err)
	}
	want := []string{"workers-close", "workers-wait", "state-close", "resources-close"}
	if got := events.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestRuntimeDrainsBusyResourceOwner(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{
		Build: "v1.0.0", Protocol: 2, PID: 222, Busy: true,
	}}, {err: ErrNoPeer}}}
	cfg, _, server, _, _ := runtimeTestConfig(t, peer)
	cfg.Contract = ResourceOwner
	cfg.Handoff = func(context.Context) error { return nil }
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := waitRuntime(t, runDone); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if shutdowns, handoffs := peer.counts(); shutdowns != 0 || handoffs != 1 {
		t.Fatalf("calls: shutdowns=%d handoffs=%d, want 0/1", shutdowns, handoffs)
	}
}

func TestRuntimeResourceHandoffOrder(t *testing.T) {
	cfg, events, server, admission, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Contract = ResourceOwner
	cfg.Handoff = func(context.Context) error {
		events.add("handoff")
		return nil
	}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	if err := runtime.Handoff(context.Background()); err != nil {
		t.Fatalf("Handoff = %v", err)
	}
	if err := waitRuntime(t, runDone); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if !admission.Draining() {
		t.Fatal("handoff did not close admission")
	}
	for before, after := range map[string]string{
		"admission-settle": "workers-close",
		"workers-wait":     "handoff",
		"handoff":          "serve-exit",
		"serve-exit":       "state-close",
		"state-close":      "resources-close",
	} {
		if events.index(before) < 0 || events.index(before) >= events.index(after) {
			t.Fatalf("%s must precede %s: %v", before, after, events.snapshot())
		}
	}
}

func TestRuntimeContextAndSignalShutdown(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		cfg, _, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
		runtime, err := NewRuntime(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		runDone := startRuntime(ctx, t, runtime, server)
		cancel()
		if err := waitRuntime(t, runDone); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	})
	t.Run("signal", func(t *testing.T) {
		cfg, _, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
		signals := make(chan os.Signal, 1)
		cfg.Signals = signals
		runtime, err := NewRuntime(cfg)
		if err != nil {
			t.Fatal(err)
		}
		runDone := startRuntime(context.Background(), t, runtime, server)
		signals <- syscall.SIGTERM
		if err := waitRuntime(t, runDone); err != nil {
			t.Fatalf("Run = %v", err)
		}
	})
}

func TestRuntimeWaitsForWorkerSettlementPastDeadline(t *testing.T) {
	cfg, events, server, _, workers := runtimeTestConfig(t, absentRuntimePeer())
	release := make(chan struct{})
	workers.waitGate = release
	workers.waitContext = true
	cfg.ShutdownTimeout = 25 * time.Millisecond
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := startRuntime(context.Background(), t, runtime, server)
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	events.wait(t, "workers-wait")
	events.wait(t, "workers-cancel")
	select {
	case err := <-runDone:
		t.Fatalf("Run returned before worker settlement: %v", err)
	default:
	}
	if events.index("state-close") >= 0 || events.index("resources-close") >= 0 {
		t.Fatalf("resources closed before worker settlement: %v", events.snapshot())
	}
	close(release)
	if err := waitRuntime(t, runDone); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want deadline after settlement", err)
	}
	got := events.snapshot()
	cancels := 0
	for _, event := range got {
		if event == "workers-cancel" {
			cancels++
		}
	}
	if cancels != 1 {
		t.Fatalf("worker cancellations = %d, want 1: %v", cancels, got)
	}
	if events.index("workers-wait") >= events.index("workers-cancel") ||
		events.index("workers-cancel") >= events.index("state-close") {
		t.Fatalf("deadline settlement order = %v", got)
	}
}

func TestRuntimeCloseUnstartedWaitsForWorkerSettlementPastDeadline(t *testing.T) {
	peer := &fakePeer{health: []healthResult{{h: Health{Build: "v1.0.0", Protocol: 1, PID: 222}}}}
	cfg, events, _, _, workers := runtimeTestConfig(t, peer)
	release := make(chan struct{})
	workers.waitGate = release
	workers.waitContext = true
	cfg.ShutdownTimeout = 25 * time.Millisecond
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	events.wait(t, "workers-wait")
	events.wait(t, "workers-cancel")
	select {
	case err := <-runDone:
		t.Fatalf("Run returned before unstarted worker settlement: %v", err)
	default:
	}
	if events.index("state-close") >= 0 || events.index("resources-close") >= 0 {
		t.Fatalf("resources closed before unstarted worker settlement: %v", events.snapshot())
	}
	close(release)
	err = waitRuntime(t, runDone)
	for _, want := range []error{ErrProtocolMismatch, context.DeadlineExceeded} {
		if !errors.Is(err, want) {
			t.Fatalf("Run = %v, missing %v", err, want)
		}
	}
	got := events.snapshot()
	cancels := 0
	for _, event := range got {
		if event == "workers-cancel" {
			cancels++
		}
	}
	if cancels != 1 {
		t.Fatalf("worker cancellations = %d, want 1: %v", cancels, got)
	}
	if events.index("workers-wait") >= events.index("workers-cancel") ||
		events.index("workers-cancel") >= events.index("state-close") {
		t.Fatalf("unstarted deadline settlement order = %v", got)
	}
}

func TestRuntimePropagatesFailuresAndClosesResourcesLast(t *testing.T) {
	serveFailure := errors.New("serve failed")
	stateFailure := errors.New("state close failed")
	resourceFailure := errors.New("resource close failed")
	cfg, events, server, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	server.serveErr = serveFailure
	cfg.State = &runtimeCloser{events: events, event: "state-close", err: stateFailure}
	cfg.Resources = &runtimeCloser{events: events, event: "resources-close", err: resourceFailure}
	runtime, err := NewRuntime(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = runtime.Run(context.Background())
	for _, want := range []error{serveFailure, stateFailure, resourceFailure} {
		if !errors.Is(err, want) {
			t.Errorf("Run = %v, missing %v", err, want)
		}
	}
	got := events.snapshot()
	if len(got) == 0 || got[len(got)-1] != "resources-close" {
		t.Fatalf("resources were not closed last: %v", got)
	}
}

func TestRuntimeCrashOrderContinuesCleanup(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(RuntimeConfig, *runtimeServer, *runtimeAdmission, *runtimeWorkers, *runtimeEvents, error) RuntimeConfig
		handoff bool
	}{
		{
			name: "close intake",
			prepare: func(cfg RuntimeConfig, server *runtimeServer, _ *runtimeAdmission, _ *runtimeWorkers, _ *runtimeEvents, boom error) RuntimeConfig {
				server.closeErr = boom
				return cfg
			},
		},
		{
			name: "settle admission",
			prepare: func(cfg RuntimeConfig, _ *runtimeServer, admission *runtimeAdmission, _ *runtimeWorkers, _ *runtimeEvents, boom error) RuntimeConfig {
				admission.settleErr = boom
				return cfg
			},
		},
		{
			name: "settle workers",
			prepare: func(cfg RuntimeConfig, _ *runtimeServer, _ *runtimeAdmission, workers *runtimeWorkers, _ *runtimeEvents, boom error) RuntimeConfig {
				workers.waitErr = boom
				return cfg
			},
		},
		{
			name: "handoff",
			prepare: func(cfg RuntimeConfig, _ *runtimeServer, _ *runtimeAdmission, _ *runtimeWorkers, _ *runtimeEvents, boom error) RuntimeConfig {
				cfg.Contract = ResourceOwner
				cfg.Handoff = func(context.Context) error { return boom }
				return cfg
			},
			handoff: true,
		},
		{
			name: "close state",
			prepare: func(cfg RuntimeConfig, _ *runtimeServer, _ *runtimeAdmission, _ *runtimeWorkers, events *runtimeEvents, boom error) RuntimeConfig {
				cfg.State = &runtimeCloser{events: events, event: "state-close", err: boom}
				return cfg
			},
		},
		{
			name: "close resources",
			prepare: func(cfg RuntimeConfig, _ *runtimeServer, _ *runtimeAdmission, _ *runtimeWorkers, events *runtimeEvents, boom error) RuntimeConfig {
				cfg.Resources = &runtimeCloser{events: events, event: "resources-close", err: boom}
				return cfg
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			boom := errors.New(tt.name + " failed")
			cfg, events, server, admission, workers := runtimeTestConfig(t, absentRuntimePeer())
			cfg = tt.prepare(cfg, server, admission, workers, events, boom)
			runtime, err := NewRuntime(cfg)
			if err != nil {
				t.Fatal(err)
			}
			runDone := startRuntime(context.Background(), t, runtime, server)
			if tt.handoff {
				if err := runtime.Handoff(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err := runtime.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			err = waitRuntime(t, runDone)
			if !errors.Is(err, boom) {
				t.Fatalf("Run = %v, want %v", err, boom)
			}
			got := events.snapshot()
			if len(got) == 0 || got[len(got)-1] != "resources-close" {
				t.Fatalf("resources were not closed last: %v", got)
			}
			for _, required := range []string{"workers-wait", "state-close", "resources-close"} {
				if events.index(required) < 0 {
					t.Errorf("cleanup omitted %q: %v", required, got)
				}
			}
			if events.index("workers-cancel") >= 0 {
				t.Errorf("settled workers were canceled: %v", got)
			}
		})
	}
}

func TestRuntimeConfigRequiresExactHandoffContract(t *testing.T) {
	cfg, _, _, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Handoff = func(context.Context) error { return nil }
	if _, err := NewRuntime(cfg); err == nil {
		t.Fatal("request runtime accepted a handoff callback")
	}
	cfg.Contract = ResourceOwner
	cfg.Handoff = nil
	if _, err := NewRuntime(cfg); err == nil {
		t.Fatal("resource-owner runtime accepted no handoff callback")
	}
}

func TestRuntimeConfigRequiresActivation(t *testing.T) {
	cfg, _, _, _, _ := runtimeTestConfig(t, absentRuntimePeer())
	cfg.Activate = nil
	if _, err := NewRuntime(cfg); err == nil {
		t.Fatal("runtime accepted no activation callback")
	}
}
