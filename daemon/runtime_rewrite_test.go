package daemon

import (
	"context"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

const runtimeTestTimeout = 3 * time.Second

type runtimeTestServer struct {
	mu sync.Mutex

	admit          runtimeauth.Admission
	admitProtected runtimeauth.Admission
	onServe        func(net.Listener) error
	ignoreContext  bool
	returnGate     <-chan struct{}

	terminalInput     chan error
	terminalPublished chan struct{}
	force             chan struct{}
	forceOnce         sync.Once
	terminalOnce      sync.Once
	closeCalls        atomic.Int32
}

func newRuntimeTestServer() *runtimeTestServer {
	return &runtimeTestServer{
		terminalInput:     make(chan error, 1),
		terminalPublished: make(chan struct{}),
		force:             make(chan struct{}),
	}
}

func (s *runtimeTestServer) ServeRuntime(
	ctx context.Context,
	listener net.Listener,
	_ any,
	_ *worker.RuntimeClaim,
	admit runtimeauth.Admission,
	admitProtected runtimeauth.Admission,
	_ runtimeauth.PeerFence,
	serverExit runtimeauth.ServerExit,
	started chan<- error,
) error {
	s.mu.Lock()
	s.admit = admit
	s.admitProtected = admitProtected
	hook := s.onServe
	ignoreContext := s.ignoreContext
	returnGate := s.returnGate
	s.mu.Unlock()

	if hook != nil {
		if err := hook(listener); err != nil {
			started <- err
			return err
		}
	}
	started <- nil

	var cause error
	if ignoreContext {
		select {
		case cause = <-s.terminalInput:
		case <-s.force:
			cause = context.Canceled
		}
	} else {
		select {
		case cause = <-s.terminalInput:
		case <-ctx.Done():
			cause = ctx.Err()
		case <-s.force:
			cause = context.Canceled
		}
	}
	result := serverExit(cause)
	s.terminalOnce.Do(func() { close(s.terminalPublished) })
	if returnGate != nil {
		select {
		case <-returnGate:
		case <-s.force:
		}
	}
	return result
}

func (s *runtimeTestServer) CloseRuntimeIntake() error {
	s.closeCalls.Add(1)
	return nil
}

func (s *runtimeTestServer) admissions(t *testing.T) (runtimeauth.Admission, runtimeauth.Admission) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.admit == nil || s.admitProtected == nil {
		t.Fatal("runtime server admissions were not installed")
	}
	return s.admit, s.admitProtected
}

func (s *runtimeTestServer) trigger(err error) { s.terminalInput <- err }

func (s *runtimeTestServer) stop() { s.forceOnce.Do(func() { close(s.force) }) }

type runtimeOrderingStore struct {
	proc.Store
	socket string
	loads  atomic.Int32
}

func (s *runtimeOrderingStore) Load(ctx context.Context) ([]proc.Record, error) {
	if _, err := os.Stat(s.socket); err == nil {
		return nil, errors.New("recovery ran after listener acquisition")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	s.loads.Add(1)
	return s.Store.Load(ctx)
}

type runtimeFailRemoveStore struct {
	proc.Store
	fail atomic.Bool
}

func (s *runtimeFailRemoveStore) Remove(ctx context.Context, records []proc.Record) error {
	if s.fail.Load() {
		return errors.New("test durable remove failure")
	}
	return s.Store.Remove(ctx, records)
}

type runtimeTestRig struct {
	runtime  *Runtime
	slot     *PublicationSlot[string]
	server   *runtimeTestServer
	workers  *worker.Pool
	children *proc.Manager
	socket   string
}

func runtimeTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "daemonkit-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func runtimeTestTrustPolicy(t *testing.T) trust.TrustPolicy {
	t.Helper()
	stopRole := trust.PeerRole("runtime-test-stop")
	receiptRole := trust.PeerRole("runtime-test-receipt")
	readinessRole := trust.PeerRole("runtime-test-readiness")
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
		Roles: map[trust.PeerRole]trust.Requirement{
			stopRole:      {TeamID: "ABCDE12345", SigningIdentifier: "com.example.runtime-test.stop"},
			receiptRole:   {TeamID: "ABCDE12345", SigningIdentifier: "com.example.runtime-test.receipt"},
			readinessRole: {TeamID: "ABCDE12345", SigningIdentifier: "com.example.runtime-test.readiness"},
		},
		StopRoles: []trust.PeerRole{stopRole}, ReceiptRoles: []trust.PeerRole{receiptRole},
		ReadinessRoles: []trust.PeerRole{readinessRole},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func newRuntimeTestRig(
	t *testing.T,
	server *runtimeTestServer,
	shutdown time.Duration,
	workerStore proc.Store,
	childStore proc.Store,
) *runtimeTestRig {
	t.Helper()
	dir := runtimeTestDir(t)
	if server == nil {
		server = newRuntimeTestServer()
	}
	if workerStore == nil {
		workerStore = &proc.FileStore{Path: filepath.Join(dir, "workers.db")}
	}
	if childStore == nil {
		childStore = &proc.FileStore{Path: filepath.Join(dir, "children.db")}
	}
	ownerGeneration, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workerReaper := &proc.Reaper{
		Store: workerStore, Generation: ownerGeneration, Grace: 10 * time.Millisecond, Settlement: time.Second,
	}
	childReaper := &proc.Reaper{
		Store: childStore, Generation: ownerGeneration, Grace: 10 * time.Millisecond, Settlement: time.Second,
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 8, QueueCapacity: 8, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 4096, MaxStdoutBytes: 4096, MaxStderrBytes: 4096,
	}, workerReaper)
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(8, childReaper)
	if err != nil {
		t.Fatal(err)
	}
	if shutdown == 0 {
		shutdown = time.Second
	}
	runtime, err := newRuntime(RuntimeConfig{
		Socket: filepath.Join(dir, "daemon.sock"), RuntimeBuild: "runtime-test", RuntimeProtocol: 1,
		Workers: workers, Children: children, ShutdownTimeout: shutdown,
	}, runtimeTestTrustPolicy(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime.server = server
	rig := &runtimeTestRig{
		runtime: runtime, server: server, workers: workers, children: children,
		socket: runtime.cfg.Socket,
	}
	rig.slot = NewPublicationSlot[string](runtime)
	t.Cleanup(func() {
		server.stop()
		ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
		defer cancel()
		_ = runtime.Close(ctx)
		releaseRuntimeTestRetained(runtime)
	})
	return rig
}

func (r *runtimeTestRig) begin(t *testing.T) Activation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	activation, err := r.runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return activation
}

func (r *runtimeTestRig) ready(t *testing.T, value string) Activation {
	t.Helper()
	activation := r.begin(t)
	publication, err := r.slot.Stage(activation, value)
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	return activation
}

func closeRuntimeTest(t *testing.T, runtime *Runtime) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	return runtime.Close(ctx)
}

func waitRuntimeTest(t *testing.T, runtime *Runtime) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	return runtime.Wait(ctx)
}

func releaseRuntimeTestRetained(runtime *Runtime) {
	runtime.mu.Lock()
	listener, lock := runtime.retainedListener, runtime.retainedLock
	runtime.retainedListener, runtime.retainedLock = nil, nil
	runtime.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	if lock != nil {
		_ = lock.Close()
	}
}

func assertRuntimeTestRetained(t *testing.T, runtime *Runtime) {
	t.Helper()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.retainedListener == nil || runtime.retainedLock == nil {
		t.Fatalf("runtime did not retain listener ownership: listener=%v lock=%v", runtime.retainedListener, runtime.retainedLock)
	}
}

func waitRuntimeTestFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(runtimeTestTimeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRuntimeBeginRecoversEveryProcessOwnerBeforeListener(t *testing.T) {
	dir := runtimeTestDir(t)
	socket := filepath.Join(dir, "daemon.sock")
	workerBase := &proc.FileStore{Path: filepath.Join(dir, "workers.db")}
	childBase := &proc.FileStore{Path: filepath.Join(dir, "children.db")}
	workerStore := &runtimeOrderingStore{Store: workerBase, socket: socket}
	childStore := &runtimeOrderingStore{Store: childBase, socket: socket}
	server := newRuntimeTestServer()
	server.onServe = func(net.Listener) error {
		if workerStore.loads.Load() != 2 || childStore.loads.Load() != 1 {
			return errors.New("listener served before every process owner recovered")
		}
		return nil
	}

	ownerGeneration, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workerReaper := &proc.Reaper{Store: workerStore, Generation: ownerGeneration}
	childReaper := &proc.Reaper{Store: childStore, Generation: ownerGeneration}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 4096, MaxStdoutBytes: 4096, MaxStderrBytes: 4096,
	}, workerReaper)
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, childReaper)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := newRuntime(RuntimeConfig{
		Socket: socket, RuntimeBuild: "runtime-test", RuntimeProtocol: 1,
		Workers: workers, Children: children, ShutdownTimeout: time.Second,
	}, runtimeTestTrustPolicy(t))
	if err != nil {
		t.Fatal(err)
	}
	runtime.server = server
	slot := NewPublicationSlot[string](runtime)
	t.Cleanup(func() {
		server.stop()
		releaseRuntimeTestRetained(runtime)
	})

	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	activation, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	if err := closeRuntimeTest(t, runtime); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRecoveryCapabilityGatesReadinessAndIsOneShot(t *testing.T) {
	rig := newRuntimeTestRig(t, nil, 0, nil, nil)
	activation := rig.begin(t)
	recoveryID, err := proc.ParseRecoveryID("consumer.barrier.v1")
	if err != nil {
		t.Fatal(err)
	}
	capability, err := activation.RecoveryCapability(recoveryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := capability.Receipt().Validate(); err != nil {
		t.Fatal(err)
	}
	if capability.Receipt().Current() != rig.runtime.processGeneration {
		t.Fatalf("current generation = %q", capability.Receipt().Current())
	}
	if _, err := activation.RecoveryCapability(recoveryID); err == nil {
		t.Fatal("duplicate recovery capability issued")
	}
	publication, err := rig.slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err == nil {
		t.Fatal("readiness committed with unconsumed recovery capability")
	}
	if err := capability.Consume(); err != nil {
		t.Fatal(err)
	}
	if err := capability.Consume(); err == nil {
		t.Fatal("recovery capability consumed twice")
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	if _, err := activation.RecoveryCapability(proc.RecoveryTrustID); !errors.Is(err, ErrPublicationStale) {
		t.Fatalf("ready recovery capability = %v", err)
	}
}

func TestRuntimePublicationAdmissionAndControlIndependence(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	activation := trig.begin(t)
	publication, err := trig.slot.Stage(activation, "published")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := trig.slot.Load(); ok {
		t.Fatal("staged value was visible before CommitReady")
	}
	admit, admitProtected := trig.server.admissions(t)
	if _, done, err := admit(); !errors.Is(err, ErrRuntimeNotReady) || done != nil {
		t.Fatalf("ordinary admission while Starting = done %v err %v", done != nil, err)
	}
	if _, done, err := admitProtected(); err != nil || done == nil {
		t.Fatalf("protected admission while Starting = done %v err %v", done != nil, err)
	} else {
		done()
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	if value, ok := trig.slot.Load(); !ok || value != "published" {
		t.Fatalf("published value = %q, %v", value, ok)
	}

	releases := make([]func(), 0, 32)
	var pinned Publication
	for range 32 {
		value, done, err := admit()
		if err != nil || done == nil {
			t.Fatalf("ordinary admission = done %v err %v", done != nil, err)
		}
		candidate, ok := value.(Publication)
		if !ok {
			t.Fatalf("ordinary admission value has type %T", value)
		}
		pinned = candidate
		releases = append(releases, done)
	}
	if _, done, err := admitProtected(); err != nil || done == nil {
		t.Fatalf("protected admission under ordinary load = done %v err %v", done != nil, err)
	} else {
		done()
	}
	if err := trig.runtime.Drain(); err != nil {
		t.Fatal(err)
	}
	if _, ok := trig.slot.Load(); ok {
		t.Fatal("un-pinned publication remained visible after Drain")
	}
	if value, ok := trig.slot.LoadPinned(pinned); !ok || value != "published" {
		t.Fatalf("pinned publication during Drain = %q, %v", value, ok)
	}
	for _, release := range releases {
		release()
	}
	if _, ok := trig.slot.LoadPinned(pinned); ok {
		t.Fatal("released publication pin remained live")
	}
	if _, done, err := admitProtected(); !errors.Is(err, ErrDraining) || done != nil {
		t.Fatalf("protected admission while Draining = done %v err %v", done != nil, err)
	}
	if err := closeRuntimeTest(t, trig.runtime); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeServerTerminalPublishesBeforeServeEOF(t *testing.T) {
	server := newRuntimeTestServer()
	returnGate := make(chan struct{})
	server.returnGate = returnGate
	trig := newRuntimeTestRig(t, server, 0, nil, nil)
	trig.ready(t, "ready")
	boom := errors.New("test session server failure")
	server.trigger(boom)
	select {
	case <-server.terminalPublished:
	case <-time.After(runtimeTestTimeout):
		t.Fatal("server terminal callback did not publish")
	}
	if progress := trig.runtime.Lifecycle().Snapshot(); progress.State != LifecycleFailed {
		t.Fatalf("lifecycle before Serve return = %+v", progress)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := trig.runtime.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait before Serve return = %v", err)
	}
	close(returnGate)
	if err := waitRuntimeTest(t, trig.runtime); !errors.Is(err, boom) {
		t.Fatalf("Wait after Serve return = %v, want server failure", err)
	}
}

func TestRuntimeStopAndServerFailureHaveOneTerminalResult(t *testing.T) {
	for _, serverFirst := range []bool{false, true} {
		name := "stop-first"
		if serverFirst {
			name = "server-first"
		}
		t.Run(name, func(t *testing.T) {
			server := newRuntimeTestServer()
			server.ignoreContext = true
			trig := newRuntimeTestRig(t, server, 0, nil, nil)
			trig.ready(t, "ready")
			boom := errors.New("test terminal race failure")
			if serverFirst {
				server.trigger(boom)
				select {
				case <-server.terminalPublished:
				case <-time.After(runtimeTestTimeout):
					t.Fatal("server terminal callback did not publish")
				}
				_ = trig.runtime.Shutdown(context.Background())
			} else {
				if err := trig.runtime.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				server.trigger(boom)
			}
			if err := waitRuntimeTest(t, trig.runtime); !errors.Is(err, boom) {
				t.Fatalf("Wait = %v, want terminal server failure", err)
			}
		})
	}
}

func TestRuntimeFailAndGracefulTerminalResults(t *testing.T) {
	t.Run("Starting stop is not ready", func(t *testing.T) {
		trig := newRuntimeTestRig(t, nil, 0, nil, nil)
		trig.begin(t)
		if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrRuntimeNotReady) {
			t.Fatalf("Close while Starting = %v", err)
		}
	})
	t.Run("Ready stop is graceful", func(t *testing.T) {
		trig := newRuntimeTestRig(t, nil, 0, nil, nil)
		trig.ready(t, "ready")
		if err := closeRuntimeTest(t, trig.runtime); err != nil {
			t.Fatalf("Close while Ready = %v", err)
		}
	})
	t.Run("activation failure wins stop", func(t *testing.T) {
		trig := newRuntimeTestRig(t, nil, 0, nil, nil)
		activation := trig.begin(t)
		boom := errors.New("test activation failure")
		if err := activation.Fail(boom); err != nil {
			t.Fatal(err)
		}
		_ = trig.runtime.Shutdown(context.Background())
		if err := waitRuntimeTest(t, trig.runtime); !errors.Is(err, boom) {
			t.Fatalf("Wait = %v, want activation failure", err)
		}
	})
}

func TestRuntimeLifecycleSequenceEdges(t *testing.T) {
	t.Run("Max-2 commits Ready and reserves Drain", func(t *testing.T) {
		trig := newRuntimeTestRig(t, nil, 0, nil, nil)
		activation := trig.begin(t)
		publication, err := trig.slot.Stage(activation, "ready")
		if err != nil {
			t.Fatal(err)
		}
		trig.runtime.lifecycle.mu.Lock()
		trig.runtime.lifecycle.progress.Sequence = math.MaxUint64 - 2
		trig.runtime.lifecycle.mu.Unlock()
		if err := activation.CommitReady(publication); err != nil {
			t.Fatal(err)
		}
		if err := closeRuntimeTest(t, trig.runtime); err != nil {
			t.Fatal(err)
		}
		progress := trig.runtime.Lifecycle().Snapshot()
		if progress.Sequence != math.MaxUint64 || progress.State != LifecycleDraining {
			t.Fatalf("terminal progress = %+v", progress)
		}
	})
	t.Run("Max-1 Commit fatal still makes Close join", func(t *testing.T) {
		server := newRuntimeTestServer()
		returnGate := make(chan struct{})
		server.returnGate = returnGate
		trig := newRuntimeTestRig(t, server, 0, nil, nil)
		activation := trig.begin(t)
		publication, err := trig.slot.Stage(activation, "never-visible")
		if err != nil {
			t.Fatal(err)
		}
		trig.runtime.lifecycle.mu.Lock()
		trig.runtime.lifecycle.progress.Sequence = math.MaxUint64 - 1
		trig.runtime.lifecycle.mu.Unlock()
		if err := activation.CommitReady(publication); !errors.Is(err, ErrSequenceExhausted) {
			t.Fatalf("CommitReady = %v", err)
		}
		select {
		case <-server.terminalPublished:
		case <-time.After(runtimeTestTimeout):
			t.Fatal("fatal runtime did not terminalize server")
		}
		closeResult := make(chan error, 1)
		go func() { closeResult <- trig.runtime.Close(context.Background()) }()
		select {
		case err := <-closeResult:
			t.Fatalf("Close returned before Serve joined: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		close(returnGate)
		select {
		case err := <-closeResult:
			if !errors.Is(err, ErrSequenceExhausted) {
				t.Fatalf("Close = %v", err)
			}
		case <-time.After(runtimeTestTimeout):
			t.Fatal("Close did not join fatal runtime")
		}
	})
	t.Run("Max failure preserves fatal cause", func(t *testing.T) {
		trig := newRuntimeTestRig(t, nil, 0, nil, nil)
		activation := trig.begin(t)
		trig.runtime.lifecycle.mu.Lock()
		trig.runtime.lifecycle.progress.Sequence = math.MaxUint64
		trig.runtime.lifecycle.mu.Unlock()
		if err := activation.Fail(errors.New("semantic failure")); !errors.Is(err, ErrSequenceExhausted) {
			t.Fatalf("Fail = %v", err)
		}
		if err := waitRuntimeTest(t, trig.runtime); !errors.Is(err, ErrSequenceExhausted) {
			t.Fatalf("Wait = %v", err)
		}
	})
}

func TestRuntimeRetainsOwnershipOnIncompleteSessionSettlement(t *testing.T) {
	server := newRuntimeTestServer()
	server.ignoreContext = true
	trig := newRuntimeTestRig(t, server, 25*time.Millisecond, nil, nil)
	trig.ready(t, "ready")
	if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("Close = %v, want incomplete shutdown", err)
	}
	assertRuntimeTestRetained(t, trig.runtime)
}

func TestRuntimeRetainsOwnershipOnIncompleteWorkerSettlement(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 25*time.Millisecond, nil, nil)
	trig.ready(t, "ready")
	marker := filepath.Join(t.TempDir(), "worker-started")
	workerResult := make(chan error, 1)
	go func() {
		_, err := trig.workers.Run(context.Background(), worker.CommandRequest{
			Path: "/bin/sh", Dir: "/bin",
			Args:         []string{"-c", `trap '' TERM; : > "$1"; while :; do /bin/sleep 1; done`, "worker", marker},
			TotalTimeout: runtimeTestTimeout,
		})
		workerResult <- err
	}()
	waitRuntimeTestFile(t, marker)
	if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("Close = %v, want incomplete worker shutdown", err)
	}
	assertRuntimeTestRetained(t, trig.runtime)
	select {
	case <-workerResult:
	case <-time.After(runtimeTestTimeout):
		t.Fatal("worker did not settle after retained shutdown")
	}
}

func TestRuntimeRetainsOwnershipOnIncompleteChildSettlement(t *testing.T) {
	base := &proc.FileStore{Path: filepath.Join(t.TempDir(), "children.db")}
	store := &runtimeFailRemoveStore{Store: base}
	trig := newRuntimeTestRig(t, nil, time.Second, nil, store)
	trig.ready(t, "ready")
	var signature proc.SignatureDigest
	signature[0] = 1
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryID:        proc.RecoveryTaskID,
		Executable:        "/bin/sh",
		Args:              []string{"-c", "exec /bin/sleep 60"},
		Stdin:             proc.StdioNull,
		Stdout:            proc.StdioNull,
		Stderr:            proc.StdioNull,
		ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := trig.children.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.fail.Store(true)
	if err := closeRuntimeTest(t, trig.runtime); !errors.Is(err, ErrShutdownIncomplete) {
		t.Fatalf("Close = %v, want incomplete child shutdown", err)
	}
	assertRuntimeTestRetained(t, trig.runtime)
	if trig.children.Active() != 1 {
		t.Fatalf("active children = %d, want retained child", trig.children.Active())
	}
	select {
	case <-child.Done():
		t.Fatal("child completion published despite durable remove failure")
	default:
	}
	store.fail.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTestTimeout)
	defer cancel()
	if err := child.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeHealthCopiesStatusAndTracksActivity(t *testing.T) {
	trig := newRuntimeTestRig(t, nil, 0, nil, nil)
	activation := trig.ready(t, "ready")
	reporter := activation.StatusReporter()
	detail := []byte("degraded")
	if err := reporter.Update(HealthStatus{State: StateDegraded, Detail: detail}); err != nil {
		t.Fatal(err)
	}
	detail[0] = 'X'
	lease, err := reporter.BeginActivity()
	if err != nil {
		t.Fatal(err)
	}
	health, err := trig.runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != StateDegraded || string(health.Detail) != "degraded" || !health.Busy || !health.Ready {
		t.Fatalf("health with activity = %+v", health)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	health, err = trig.runtime.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Busy {
		t.Fatalf("health remained busy after release: %+v", health)
	}
	if err := closeRuntimeTest(t, trig.runtime); err != nil {
		t.Fatal(err)
	}
}
