package supervise

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

const managedProcessMarkerEnv = "DAEMONKIT_MANAGED_PROCESS_MARKER"

func TestManagedProcessHelper(_ *testing.T) {
	marker := os.Getenv(managedProcessMarkerEnv)
	if marker == "" {
		return
	}
	if err := os.WriteFile(marker, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		panic(err)
	}
	signal.Ignore(syscall.SIGTERM)
	for {
		time.Sleep(time.Hour)
	}
}

func managedProcessSpec(t *testing.T, marker string) ProcessSpec {
	t.Helper()
	return ProcessSpec{
		RecoveryClass: proc.RecoveryTask,
		Path:          os.Args[0],
		Args:          []string{"-test.run=^TestManagedProcessHelper$"},
		Env:           append(os.Environ(), managedProcessMarkerEnv+"="+marker),
	}
}

func TestManagedProcessCannotExecOrBecomeReadyBeforeDurableRecord(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	release := make(chan struct{})
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	registry.trackRelease = release
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	ready := make(chan struct{})
	recorded := make(chan proc.Record, 1)
	spec := managedProcessSpec(t, marker)
	spec.Recorded = func(_ context.Context, record proc.Record) error {
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			return errors.New("child executed before recorded hook")
		}
		recorded <- record
		return nil
	}
	spec.Ready = func(context.Context, proc.Record) error {
		close(ready)
		if registry.recordCount() != 1 {
			return errors.New("readiness ran before durable tracking")
		}
		return nil
	}
	started := make(chan struct {
		process *Process
		err     error
	}, 1)
	go func() {
		process, startErr := pool.Start(context.Background(), spec)
		started <- struct {
			process *Process
			err     error
		}{process: process, err: startErr}
	}()
	pid := <-registry.trackStarted
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child executed before durable tracking: %v", err)
	}
	select {
	case <-ready:
		t.Fatal("readiness ran before durable tracking")
	default:
	}
	close(release)
	result := <-started
	if result.err != nil {
		t.Fatalf("Start: %v", result.err)
	}
	if result.process.Record().PID != pid {
		t.Fatalf("Record PID = %d, want %d", result.process.Record().PID, pid)
	}
	if record := <-recorded; record != result.process.Record() {
		t.Fatalf("Recorded record = %#v, want %#v", record, result.process.Record())
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("readiness did not run after durable tracking")
	}
	_ = readPIDFile(t, marker)
	if err := result.process.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertPIDGone(t, pid)
}

func TestManagedProcessRecordedRefusalPreventsExecAndRemovesRecord(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	spec := managedProcessSpec(t, marker)
	refused := errors.New("refused record")
	spec.Recorded = func(context.Context, proc.Record) error { return refused }
	_, err = pool.Start(context.Background(), spec)
	if !errors.Is(err, refused) {
		t.Fatalf("Start error = %v, want %v", err, refused)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child executed after recorded refusal: %v", err)
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want rejected record removed", got)
	}
}

func TestManagedProcessRejectsRecordWithoutBootBeforeExec(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	registry.recordBoot = ""
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	_, err = pool.Start(context.Background(), managedProcessSpec(t, marker))
	if !errors.Is(err, proc.ErrInvalidRecord) {
		t.Fatalf("Start error = %v, want ErrInvalidRecord", err)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child executed with incomplete durable identity: %v", err)
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want invalid record removed", got)
	}
}

func TestManagedProcessReadinessTimeoutKillsReapsAndUntracks(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	spec := managedProcessSpec(t, marker)
	spec.ReadinessTimeout = 50 * time.Millisecond
	spec.Ready = func(ctx context.Context, _ proc.Record) error {
		<-ctx.Done()
		return ctx.Err()
	}
	_, err = pool.Start(context.Background(), spec)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error = %v, want readiness deadline", err)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedProcessRejectsWrongReadyPeerAndCleansChild(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	spec := managedProcessSpec(t, marker)
	spec.Ready = func(_ context.Context, record proc.Record) error {
		peer := wire.Peer{PID: record.PID, StartTime: record.StartTime + "-reused"}
		if !peer.MatchesProcess(record) {
			return errors.New("ready peer does not match managed process")
		}
		return nil
	}
	_, err = pool.Start(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "ready peer does not match") {
		t.Fatalf("Start error = %v, want wrong-peer rejection", err)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedProcessStopEscalatesAndReapsExactChild(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	var (
		signalsMu sync.Mutex
		signals   []syscall.Signal
	)
	pool.signal = func(pid int, signal syscall.Signal) error {
		signalsMu.Lock()
		signals = append(signals, signal)
		signalsMu.Unlock()
		return syscall.Kill(pid, signal)
	}
	process, err := pool.Start(context.Background(), managedProcessSpec(t, marker))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := readPIDFile(t, marker)
	if pid != process.Record().PID {
		t.Fatalf("child PID = %d, record PID = %d", pid, process.Record().PID)
	}
	if err := process.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := process.Wait(context.Background()); !errors.Is(err, ErrProcessStopped) {
		t.Fatalf("Wait error = %v, want ErrProcessStopped", err)
	}
	signalsMu.Lock()
	gotSignals := append([]syscall.Signal(nil), signals...)
	signalsMu.Unlock()
	if len(gotSignals) != 2 || gotSignals[0] != syscall.SIGTERM || gotSignals[1] != syscall.SIGKILL {
		t.Fatalf("signals = %v, want [terminated killed]", gotSignals)
	}
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedProcessStartupContextCancellationAfterReadyDoesNotStopProcess(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	process, err := pool.Start(ctx, managedProcessSpec(t, marker))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := readPIDFile(t, marker)
	cancel()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelWait()
	if err := process.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait after startup cancellation = %v, want running process", err)
	}
	if got := registry.recordCount(); got != 1 {
		t.Fatalf("durable records after startup cancellation = %d, want 1", got)
	}
	pool.Cancel()
	if err := pool.Wait(context.Background()); err != nil {
		t.Fatalf("pool Wait: %v", err)
	}
	if err := process.Wait(context.Background()); !errors.Is(err, ErrProcessStopped) {
		t.Fatalf("Wait after pool cancellation = %v, want ErrProcessStopped", err)
	}
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedProcessStartupCancellationBeforeReadinessStopsAndReaps(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	registry := newFakeRegistry()
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	ready := make(chan struct{})
	spec := managedProcessSpec(t, marker)
	spec.Ready = func(ctx context.Context, _ proc.Record) error {
		close(ready)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() {
		_, startErr := pool.Start(ctx, spec)
		started <- startErr
	}()
	pid := <-registry.trackStarted
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("readiness did not start")
	}
	cancel()
	if err := <-started; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want context.Canceled", err)
	}
	assertPIDGone(t, pid)
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want 0", got)
	}
}

func TestManagedProcessRecordIsRecoveredByNextGeneration(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.json")}
	oldReaper := &proc.Reaper{Store: store, Generation: "old-generation"}
	pool, err := NewPool(1, oldReaper)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	process, err := pool.Start(context.Background(), managedProcessSpec(t, marker))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	pid := readPIDFile(t, marker)
	newReaper := &proc.Reaper{
		Store: store, Generation: "new-generation", Grace: 50 * time.Millisecond, Settlement: time.Second,
	}
	if err := newReaper.Reap(context.Background()); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if err := process.Wait(context.Background()); err == nil {
		t.Fatal("Wait succeeded after next-generation reaping")
	}
	assertPIDGone(t, pid)
	records, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("durable records = %v, want none", records)
	}
}
