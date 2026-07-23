package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	peeridentity "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

type childFenceFixture struct {
	runtime *Runtime
	manager *proc.Manager
	child   *proc.PreparedChild
	receipt proc.ProcessReceipt
	fence   *ChildFence
}

func newChildFenceFixture(t *testing.T) *childFenceFixture {
	t.Helper()
	requirement := func(identifier string) trust.Requirement {
		return trust.Requirement{TeamID: "DAEMONKITTEST", SigningIdentifier: identifier}
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
		Roles: map[trust.PeerRole]trust.Requirement{
			"stop": requirement("com.yasyf.daemonkit.test.stop"), "lifecycle": requirement("com.yasyf.daemonkit.test.lifecycle"),
			"product": requirement("com.yasyf.daemonkit.test.product"),
		},
		StopRoles: []trust.PeerRole{"stop"}, ReceiptRoles: []trust.PeerRole{"lifecycle"}, ReadinessRoles: []trust.PeerRole{"lifecycle"},
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, ok := policy.SignatureDigest("product")
	if !ok {
		t.Fatal("product role has no signature digest")
	}
	manager, err := proc.NewManager(1, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(t.TempDir(), "children.db")}, Generation: "child-fence-test",
		Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	request, err := proc.NewSpawnRequest(proc.SpawnConfig{
		RecoveryClass: proc.RecoveryTask, Executable: "/bin/sh", Args: []string{"-c", "trap '' TERM; sleep 60"},
		Stdin: proc.StdioNull, Stdout: proc.StdioNull, Stderr: proc.StdioNull,
		RequiresPeerFence: true, ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	child, receipt, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := newLifecycle()
	lifecycle.progress = LifecycleProgress{Sequence: 2, State: LifecycleReady, Detail: []byte{}}
	runtime := &Runtime{
		cfg: RuntimeConfig{Children: manager, ShutdownTimeout: 25 * time.Millisecond}, lifecycle: lifecycle,
		controllerGeneration: 1, serverLive: true, childFences: make(map[childFenceKey]*childFenceState), trustPolicy: policy,
		stop:               make(chan struct{}, 1),
		childFenceTimeout:  25 * time.Millisecond,
		childFenceVerifier: func(ctx context.Context, _ peeridentity.Identity, _ trust.Requirement) error { return ctx.Err() },
	}
	fence, err := runtime.ReadyOnlyListener().ArmChild(receipt)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &childFenceFixture{runtime: runtime, manager: manager, child: child, receipt: receipt, fence: fence}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = child.Stop(ctx)
		_ = manager.Shutdown(ctx)
	})
	return fixture
}

func waitChildFenceDispatch(t *testing.T, fixture *childFenceFixture) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fixture.runtime.mu.Lock()
		dispatching := fixture.fence.state.dispatching
		fixture.runtime.mu.Unlock()
		if dispatching {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("child fence did not begin dispatch")
}

func TestChildFenceMismatchDoesNotReenterRuntimeLock(t *testing.T) {
	key := childFenceKey{pid: 42, start: "start", boot: "boot", generation: "generation"}
	state := &childFenceState{executable: "/expected", authDone: make(chan struct{})}
	runtime := &Runtime{lifecycle: newLifecycle(), childFences: map[childFenceKey]*childFenceState{key: state}}
	done := make(chan error, 1)
	go func() {
		_, err := runtime.matchChildFence(context.Background(), peeridentity.Identity{
			PID: 42, StartTime: "start", Boot: "boot", Executable: "/unexpected",
		})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrFenceMismatch) {
			t.Fatalf("mismatch error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("mismatch recursively acquired the runtime lock")
	}
}

func TestChildFenceBackgroundStartHasRuntimeBound(t *testing.T) {
	fixture := newChildFenceFixture(t)
	done := make(chan error, 1)
	go func() { _, err := fixture.fence.Start(context.Background(), fixture.child); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unobserved child fence succeeded")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Background Start exceeded the runtime-owned authentication bound")
	}
}

func TestChildFenceAdmissionRollbackCannotAuthenticateStart(t *testing.T) {
	fixture := newChildFenceFixture(t)
	fixture.runtime.childFenceVerifier = func(context.Context, peeridentity.Identity, trust.Requirement) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := fixture.fence.Start(ctx, fixture.child); done <- err }()
	waitChildFenceDispatch(t, fixture)
	identity := fixture.receipt.ProcessIdentity()
	permit, err := fixture.runtime.matchChildFence(ctx, peeridentity.Identity{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Comm: identity.Comm,
		Executable: fixture.receipt.ExpectedExecutable(),
	})
	if err != nil || permit == nil {
		t.Fatalf("provisional match = %v, %v", permit, err)
	}
	permit.Rollback()
	if err := <-done; err == nil {
		t.Fatal("rolled-back pre-session authentication succeeded")
	}
}

func TestChildFenceMismatchSettlementFailureIsRuntimeFatal(t *testing.T) {
	fixture := newChildFenceFixture(t)
	startDone := make(chan error, 1)
	go func() { _, err := fixture.fence.Start(context.Background(), fixture.child); startDone <- err }()
	waitChildFenceDispatch(t, fixture)
	identity := fixture.receipt.ProcessIdentity()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fixture.runtime.matchChildFence(ctx, peeridentity.Identity{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Comm: identity.Comm,
		Executable: fixture.receipt.ExpectedExecutable(),
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, proc.ErrChildSettlementIncomplete) {
		t.Fatalf("mismatch settlement error = %v", err)
	}
	if fatal := fixture.runtime.lifecycle.fatalError(); fatal == nil || !errors.Is(fatal, proc.ErrChildSettlementIncomplete) {
		t.Fatalf("runtime fatal = %v, want retained settlement failure", fatal)
	}
	if fixture.manager.Active() != 1 {
		t.Fatalf("unsettled child ownership = %d, want 1", fixture.manager.Active())
	}
	select {
	case err := <-startDone:
		if err == nil {
			t.Fatal("Start succeeded after fatal mismatch")
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not observe fatal mismatch")
	}
}
