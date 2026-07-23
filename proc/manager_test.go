package proc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newManagerTest(t *testing.T, capacity int) (*Manager, *memStore) {
	t.Helper()
	store := &memStore{}
	manager, err := NewManager(capacity, &Reaper{
		Store: store, Generation: "manager-test", Grace: 10 * time.Millisecond, Settlement: time.Second,
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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := manager.Shutdown(ctx); err != nil {
			t.Errorf("shutdown manager: %v", err)
		}
	})
	return manager, store
}

func managerTestRequest(t *testing.T, script string, modes ...StdioMode) SpawnRequest {
	t.Helper()
	stdio := []StdioMode{StdioNull, StdioNull, StdioNull}
	copy(stdio, modes)
	var signature SignatureDigest
	signature[0] = 1
	request, err := NewSpawnRequest(SpawnConfig{
		RecoveryClass: RecoveryTask, Executable: "/bin/sh", Args: []string{"-c", script},
		Stdin: stdio[0], Stdout: stdio[1], Stderr: stdio[2], ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func waitManagerChild(t *testing.T, child *PreparedChild) ProcessExit {
	t.Helper()
	select {
	case <-child.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("prepared child did not settle")
	}
	exit, ok := child.Exit()
	if !ok {
		t.Fatal("settled child has no exit")
	}
	return exit
}

func newUnrecoveredManagerTest(t *testing.T, store Store) *Manager {
	t.Helper()
	manager, err := NewManager(1, &Reaper{
		Store: store, Generation: "manager-test", Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	return manager
}

func TestManagerPrepareRequiresCompletedRecovery(t *testing.T) {
	store := &memStore{}
	manager := newUnrecoveredManagerTest(t, store)
	child, receipt, err := manager.Prepare(context.Background(), managerTestRequest(t, "exit 0"))
	if err == nil || child != nil || receipt.Prepared() {
		t.Fatalf("Prepare before Recover = child %v receipt prepared=%v error %v", child, receipt.Prepared(), err)
	}
	if manager.Active() != 0 || len(manager.limit) != 0 || store.len() != 0 {
		t.Fatalf("rejected Prepare retained active=%d slots=%d records=%d", manager.Active(), len(manager.limit), store.len())
	}
	if err := manager.Recover(context.Background()); err != nil {
		t.Fatalf("Recover after rejected Prepare: %v", err)
	}
	child, _, err = manager.Prepare(context.Background(), managerTestRequest(t, "exit 0"))
	if err != nil {
		t.Fatalf("Prepare after Recover: %v", err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitManagerChild(t, child)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

type blockingLoadStore struct {
	*memStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *blockingLoadStore) Load(ctx context.Context) ([]Record, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
		return s.memStore.Load(ctx)
	}
}

func newBlockedRecoveryTest(t *testing.T) (*Manager, *blockingLoadStore, <-chan error) {
	t.Helper()
	store := &blockingLoadStore{
		memStore: &memStore{}, entered: make(chan struct{}), release: make(chan struct{}),
	}
	manager := newUnrecoveredManagerTest(t, store)
	done := make(chan error, 1)
	go func() { done <- manager.Recover(context.Background()) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("Recover did not enter the blocking store")
	}
	return manager, store, done
}

func TestManagerBlockedRecoveryRejectsPrepareAndRelease(t *testing.T) {
	manager, store, recoverDone := newBlockedRecoveryTest(t)
	child, receipt, err := manager.Prepare(context.Background(), managerTestRequest(t, "exit 0"))
	if err == nil || child != nil || receipt.Prepared() {
		t.Fatalf("Prepare during Recover = child %v receipt prepared=%v error %v", child, receipt.Prepared(), err)
	}
	if err := manager.ReleaseRuntime(); err == nil {
		t.Fatal("ReleaseRuntime succeeded during Recover")
	}
	close(store.release)
	if err := <-recoverDone; err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := manager.ReleaseRuntime(); err != nil {
		t.Fatalf("ReleaseRuntime after Recover: %v", err)
	}
}

func TestManagerShutdownWaitsForBlockedRecovery(t *testing.T) {
	manager, store, recoverDone := newBlockedRecoveryTest(t)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before Recover settled: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(store.release)
	if err := <-recoverDone; err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if child, receipt, err := manager.Prepare(context.Background(), managerTestRequest(t, "exit 0")); err == nil || child != nil || receipt.Prepared() {
		t.Fatalf("Prepare after Shutdown = child %v receipt prepared=%v error %v", child, receipt.Prepared(), err)
	}
}

func TestManagerActiveAndCapacityTransitionTogether(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	manager.mu.Lock()
	prepareDone := make(chan struct {
		child *PreparedChild
		err   error
	}, 1)
	go func() {
		child, _, err := manager.Prepare(context.Background(), managerTestRequest(t, "exit 0"))
		prepareDone <- struct {
			child *PreparedChild
			err   error
		}{child: child, err: err}
	}()
	deadline := time.Now().Add(100 * time.Millisecond)
	for len(manager.limit) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if used := len(manager.limit); used != 0 {
		manager.mu.Unlock()
		t.Fatalf("capacity became visible before active ownership: slots=%d", used)
	}
	manager.mu.Unlock()
	prepared := <-prepareDone
	if prepared.err != nil {
		t.Fatalf("Prepare: %v", prepared.err)
	}
	if active, slots := manager.Active(), len(manager.limit); active != 1 || slots != 1 {
		t.Fatalf("published child accounting active=%d slots=%d", active, slots)
	}
	if err := prepared.child.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitManagerChild(t, prepared.child)
	if active, slots := manager.Active(), len(manager.limit); active != 0 || slots != 0 {
		t.Fatalf("settled child accounting active=%d slots=%d", active, slots)
	}
}

func TestManagerPrepareRecordsBeforeDispatch(t *testing.T) {
	manager, store := newManagerTest(t, 1)
	marker := filepath.Join(t.TempDir(), "executed")
	request := managerTestRequest(t, "printf yes > \"$1\"", StdioNull, StdioNull, StdioNull)
	request.args = append(request.args, "manager-test", marker)
	child, receipt, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target executed before Start: %v", err)
	}
	records, err := store.Load(context.Background())
	if err != nil || len(records) != 1 {
		t.Fatalf("durable records before Start = %d, %v", len(records), err)
	}
	receiptSignature, signatureOK := receipt.ExpectedSignature()
	if receipt.ProcessIdentity().PID != records[0].PID || receipt.ExpectedExecutable() != "/bin/sh" || !signatureOK || receiptSignature == (SignatureDigest{}) {
		t.Fatalf("receipt = %+v, record = %+v", receipt, records[0])
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	exit := waitManagerChild(t, child)
	if exit.Code != 0 || exit.Stopped || exit.Error != "" {
		t.Fatalf("exit = %+v", exit)
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "yes" {
		t.Fatalf("marker = %q, %v", data, err)
	}
	records, err = store.Load(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatalf("durable records after settlement = %d, %v", len(records), err)
	}
}

func TestManagerStopWinsDispatchRace(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	marker := filepath.Join(t.TempDir(), "executed")
	request := managerTestRequest(t, "printf yes > \"$1\"")
	request.args = append(request.args, "manager-test", marker)
	child, _, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := child.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); !errors.Is(err, ErrChildStopped) {
		t.Fatalf("Start after Stop = %v, want ErrChildStopped", err)
	}
	exit := waitManagerChild(t, child)
	if !exit.Stopped || exit.Error != "" {
		t.Fatalf("exit = %+v", exit)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stopped target executed: %v", err)
	}
}

func TestManagerStartExactlyOnceAndOwnsPipes(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	child, _, err := manager.Prepare(context.Background(), managerTestRequest(
		t, `IFS= read -r line; printf 'out:%s' "$line"; printf err >&2`, StdioPipe, StdioPipe, StdioPipe,
	))
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := child.TakeStdin()
	if err != nil {
		t.Fatal(err)
	}
	stdoutPipe, err := child.TakeStdout()
	if err != nil {
		t.Fatal(err)
	}
	stderrPipe, err := child.TakeStderr()
	if err != nil {
		t.Fatal(err)
	}
	defer stdoutPipe.Close()
	defer stderrPipe.Close()
	if _, err := child.TakeStdout(); !errors.Is(err, ErrPipeUnavailable) {
		t.Fatalf("second TakeStdout = %v", err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); !errors.Is(err, ErrChildStarted) {
		t.Fatalf("second Start = %v, want ErrChildStarted", err)
	}
	if _, err := io.WriteString(stdin, "value\n"); err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	stdout, err := io.ReadAll(stdoutPipe)
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(stderrPipe)
	if err != nil {
		t.Fatal(err)
	}
	exit := waitManagerChild(t, child)
	if exit.Code != 0 || string(stdout) != "out:value" || string(stderr) != "err" {
		t.Fatalf("exit=%+v stdout=%q stderr=%q", exit, stdout, stderr)
	}
}

func TestManagerCapacityAndRequestImmutability(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	args := []string{"-c", "exit 0"}
	env := []string{"VALUE=before"}
	var signature SignatureDigest
	signature[0] = 1
	request, err := NewSpawnRequest(SpawnConfig{
		RecoveryClass: RecoveryTask, Executable: "/bin/sh", Args: args, Env: env,
		Stdin: StdioNull, Stdout: StdioNull, Stderr: StdioNull, ExpectedSignature: &signature,
	})
	if err != nil {
		t.Fatal(err)
	}
	args[0], env[0] = "changed", "VALUE=changed"
	if request.args[0] != "-c" || request.env[0] != "VALUE=before" {
		t.Fatalf("request was not deeply copied: args=%v env=%v", request.args, request.env)
	}
	first, _, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := manager.Prepare(ctx, request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capacity wait = %v, want deadline", err)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type failRemoveStore struct {
	*memStore
	muFail sync.Mutex
	fail   bool
}

func (s *failRemoveStore) Remove(ctx context.Context, records []Record) error {
	s.muFail.Lock()
	fail := s.fail
	s.muFail.Unlock()
	if fail {
		return errors.New("remove failed")
	}
	return s.memStore.Remove(ctx, records)
}

func TestManagerRetainsOwnershipUntilDurableSettlement(t *testing.T) {
	store := &failRemoveStore{memStore: &memStore{}, fail: true}
	manager, err := NewManager(1, &Reaper{
		Store: store, Generation: "manager-test", Grace: 10 * time.Millisecond, Settlement: time.Second,
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
	child, _, err := manager.Prepare(context.Background(), managerTestRequest(t, "sleep 60"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = child.Stop(ctx)
	cancel()
	if !errors.Is(err, ErrChildSettlementIncomplete) || manager.Active() != 1 {
		t.Fatalf("first Stop = %v, active=%d", err, manager.Active())
	}
	select {
	case <-child.Done():
		t.Fatal("child settled despite durable remove failure")
	default:
	}
	store.muFail.Lock()
	store.fail = false
	store.muFail.Unlock()
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := child.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.Active() != 0 {
		t.Fatalf("active after retry = %d", manager.Active())
	}
}

func TestNewSpawnRequestRejectsAmbientAndMalformedInput(t *testing.T) {
	var signature SignatureDigest
	signature[0] = 1
	base := SpawnConfig{
		RecoveryClass: RecoveryTask, Executable: "/bin/sh", Stdin: StdioNull, Stdout: StdioNull,
		Stderr: StdioNull, ExpectedSignature: &signature,
	}
	for _, mutate := range []func(*SpawnConfig){
		func(c *SpawnConfig) { c.Executable = "bin/sh" },
		func(c *SpawnConfig) { c.Executable = "/bin/sh\x00bad" },
		func(c *SpawnConfig) { c.Dir = "relative" },
		func(c *SpawnConfig) { c.Dir = "/tmp\x00bad" },
		func(c *SpawnConfig) { c.Args = []string{"bad\x00arg"} },
		func(c *SpawnConfig) { c.Env = []string{"PATH=/other"} },
		func(c *SpawnConfig) { c.Env = []string{"LANG=C"} },
		func(c *SpawnConfig) { c.Env = []string{"BROKEN"} },
		func(c *SpawnConfig) { zero := SignatureDigest{}; c.ExpectedSignature = &zero },
	} {
		config := base
		mutate(&config)
		if _, err := NewSpawnRequest(config); err == nil || strings.TrimSpace(err.Error()) == "" {
			t.Fatalf("malformed config accepted: %+v", config)
		}
	}
}

func TestSpawnRequestOptionalSignatureAndPeerFence(t *testing.T) {
	base := SpawnConfig{
		RecoveryClass: RecoveryTask, Executable: "/bin/sh",
		Stdin: StdioNull, Stdout: StdioNull, Stderr: StdioNull,
	}
	request, err := NewSpawnRequest(base)
	if err != nil {
		t.Fatalf("unsigned one-shot request: %v", err)
	}
	if request.hasSignature || request.requiresFence {
		t.Fatalf("unsigned request = %+v", request)
	}
	base.RequiresPeerFence = true
	if _, err := NewSpawnRequest(base); err == nil {
		t.Fatal("peer-fenced request without signature succeeded")
	}
	var signature SignatureDigest
	signature[0] = 1
	base.ExpectedSignature = &signature
	first, err := NewSpawnRequest(base)
	if err != nil {
		t.Fatal(err)
	}
	signature[0] = 2
	if first.signature[0] != 1 || !first.hasSignature || first.digest == (SpawnRequestDigest{}) {
		t.Fatalf("request did not deep-copy signature: %+v", first)
	}
}

func TestManagerStopRetryAfterTerminationCommitOnlyJoinsWait(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	manager.limit <- struct{}{}
	child := &PreparedChild{
		manager: manager, state: preparedChildStopping, terminated: true,
		observed: make(chan struct{}), done: make(chan struct{}),
	}
	manager.mu.Lock()
	manager.children[child] = struct{}{}
	manager.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	err := child.Stop(ctx)
	cancel()
	if !errors.Is(err, ErrChildSettlementIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Stop = %v", err)
	}
	child.mu.Lock()
	child.waitErr = errors.New("terminated")
	close(child.observed)
	child.mu.Unlock()
	if err := child.Stop(context.Background()); err != nil {
		t.Fatalf("retry Stop = %v", err)
	}
	if exit := waitManagerChild(t, child); !exit.Stopped || exit.Error != "" {
		t.Fatalf("exit = %+v", exit)
	}
}

func TestManagerCanceledStartSettlesWithoutManualStop(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	child, _, err := manager.Prepare(context.Background(), managerTestRequest(t, "sleep 60"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := child.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want context.Canceled", err)
	}
	exit := waitManagerChild(t, child)
	if !exit.Stopped || manager.Active() != 0 {
		t.Fatalf("exit=%+v active=%d", exit, manager.Active())
	}
}

func TestManagerNaturalLeaderExitSettlesDescendants(t *testing.T) {
	manager, store := newManagerTest(t, 1)
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	request := managerTestRequest(t, `sleep 60 & printf %s $! > "$1"; exit 0`)
	request.args = append(request.args, "manager-test", pidFile)
	child, _, err := manager.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	exit := waitManagerChild(t, child)
	if exit.Code != 0 || exit.Stopped || exit.Error != "" {
		t.Fatalf("exit = %+v", exit)
	}
	records, err := store.Load(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatalf("records after natural group settlement = %d, %v", len(records), err)
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for syscall.Kill(pid, 0) == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant pid %d remains: %v", pid, err)
	}
}

func TestManagerStopEscalatesAfterFixedGrace(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	child, _, err := manager.Prepare(context.Background(), managerTestRequest(
		t, `trap '' TERM; printf ready; while :; do sleep 1; done`, StdioNull, StdioPipe, StdioNull,
	))
	if err != nil {
		t.Fatal(err)
	}
	stdoutPipe, err := child.TakeStdout()
	if err != nil {
		t.Fatal(err)
	}
	defer stdoutPipe.Close()
	if err := child.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, len("ready"))
	if _, err := io.ReadFull(stdoutPipe, ready); err != nil || string(ready) != "ready" {
		t.Fatalf("ready = %q, %v", ready, err)
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := child.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed < 400*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("TERM/KILL grace elapsed = %s", elapsed)
	}
}

type blockingAddStore struct {
	*memStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *blockingAddStore) Add(ctx context.Context, record Record) error {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return s.memStore.Add(ctx, record)
	}
}

func TestManagerShutdownBoundsInflightPrepareAndRejectsPublication(t *testing.T) {
	store := &blockingAddStore{memStore: &memStore{}, entered: make(chan struct{}), release: make(chan struct{})}
	manager, err := NewManager(1, &Reaper{
		Store: store, Generation: "manager-test", Grace: 10 * time.Millisecond, Settlement: time.Second,
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
	prepareDone := make(chan error, 1)
	request := managerTestRequest(t, "sleep 60")
	go func() {
		_, _, err := manager.Prepare(context.Background(), request)
		prepareDone <- err
	}()
	<-store.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = manager.Shutdown(ctx)
	cancel()
	if !errors.Is(err, ErrChildSettlementIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Shutdown = %v", err)
	}
	close(store.release)
	if err := <-prepareDone; !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Prepare after shutdown began = %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.Active() != 0 {
		t.Fatalf("active = %d", manager.Active())
	}
}

func TestManagerRecoverSettlesPriorGenerationBeforeUse(t *testing.T) {
	store := &memStore{}
	command := exec.Command("/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	oldReaper := &Reaper{Store: store, Generation: "old", Grace: 10 * time.Millisecond, Settlement: time.Second}
	if _, err := oldReaper.TrackGroup(context.Background(), command.Process.Pid, RecoveryTask); err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(1, &Reaper{
		Store: store, Generation: "new", Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waited:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	records, err := store.Load(context.Background())
	if err != nil || len(records) != 0 {
		t.Fatalf("records after recovery = %d, %v", len(records), err)
	}
}

func TestManagerUntrackedCleanupFailureIsBoundedAndRetained(t *testing.T) {
	killFailure := errors.New("kill failed")
	prober := &fakeProber{
		boot: testBoot,
		info: procInfo{startTime: "start", comm: "wrapper"},
	}
	signaler := &termThenKillFailureSignaler{killErr: killFailure}
	manager, err := NewManager(1, &Reaper{
		Store: &memStore{}, Generation: "manager-test", Grace: 10 * time.Millisecond,
		Settlement: time.Second, prober: prober, signaler: signaler,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ClaimRuntime(); err != nil {
		t.Fatal(err)
	}
	manager.limit <- struct{}{}
	waited := make(chan error, 1)
	child := &untrackedChild{
		manager: manager, identity: Identity{PID: 42, StartTime: "start", Boot: testBoot, Comm: "wrapper"},
		waited: waited, observed: make(chan struct{}), done: make(chan struct{}),
	}
	manager.mu.Lock()
	manager.untracked[child] = struct{}{}
	manager.mu.Unlock()
	go child.observe()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	started := time.Now()
	err = child.Stop(ctx)
	cancel()
	if !errors.Is(err, ErrChildSettlementIncomplete) || !errors.Is(err, killFailure) {
		t.Fatalf("Stop = %v", err)
	}
	if calls := signaler.calls(); len(calls) != 2 || calls[0] != (signalCall{pid: 42, sig: syscall.SIGTERM}) ||
		calls[1] != (signalCall{pid: 42, sig: syscall.SIGKILL}) {
		t.Fatalf("signals = %+v, want TERM then KILL", calls)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded failed cleanup took %s", elapsed)
	}
	if manager.Active() != 1 {
		t.Fatalf("active after failed termination = %d", manager.Active())
	}
	waited <- nil
	select {
	case <-child.done:
	case <-time.After(time.Second):
		t.Fatal("naturally reaped untracked child did not settle")
	}
	if manager.Active() != 0 {
		t.Fatalf("active after observed reap = %d", manager.Active())
	}
}

type termThenKillFailureSignaler struct {
	mu      sync.Mutex
	sent    []signalCall
	killErr error
}

func (s *termThenKillFailureSignaler) signal(pid int, sig syscall.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, signalCall{pid: pid, sig: sig})
	if sig == syscall.SIGKILL {
		return s.killErr
	}
	return nil
}

func (s *termThenKillFailureSignaler) calls() []signalCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signalCall(nil), s.sent...)
}

func TestManagerUntrackedCleanupNeverWaitsPastCallerBound(t *testing.T) {
	manager, _ := newManagerTest(t, 1)
	manager.limit <- struct{}{}
	waited := make(chan error, 1)
	child := &untrackedChild{
		manager: manager, waited: waited, observed: make(chan struct{}), done: make(chan struct{}),
	}
	manager.mu.Lock()
	manager.untracked[child] = struct{}{}
	manager.mu.Unlock()
	go child.observe()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err := child.Stop(ctx)
	cancel()
	if !errors.Is(err, ErrChildSettlementIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v", err)
	}
	if time.Since(started) > time.Second || manager.Active() != 1 {
		t.Fatalf("bounded Stop elapsed=%s active=%d", time.Since(started), manager.Active())
	}
	// Complete the synthetic child so the test-owned manager can shut down.
	waited <- nil
	<-child.done
}
