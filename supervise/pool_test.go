package supervise

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type fakeRegistry struct {
	mu sync.Mutex

	trackStarted chan int
	trackRelease <-chan struct{}
	trackErr     error
	untrackErr   error
	owns         bool
	ownsErr      error
	ownsResults  []ownsResult
	ownsCalls    int
	ownsStarted  chan struct{}
	ownsRelease  <-chan struct{}
	reapErr      error
	recordBoot   string
	records      map[int]proc.Record
}

type ownsResult struct {
	owns bool
	err  error
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{owns: true, recordBoot: "test-boot", records: make(map[int]proc.Record)}
}

func (f *fakeRegistry) TrackGroup(ctx context.Context, pid int) (proc.Record, error) {
	if f.trackStarted != nil {
		select {
		case f.trackStarted <- pid:
		case <-ctx.Done():
			return proc.Record{}, ctx.Err()
		}
	}
	if f.trackRelease != nil {
		select {
		case <-f.trackRelease:
		case <-ctx.Done():
			return proc.Record{}, ctx.Err()
		}
	}
	if f.trackErr != nil {
		return proc.Record{}, f.trackErr
	}
	rec := proc.Record{
		PID:          pid,
		StartTime:    "test-start",
		Boot:         f.recordBoot,
		Comm:         "worker",
		Generation:   "test-generation",
		ProcessGroup: true,
		SessionID:    pid,
	}
	f.mu.Lock()
	f.records[pid] = rec
	f.mu.Unlock()
	return rec, nil
}

func (f *fakeRegistry) Untrack(_ context.Context, rec proc.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.records, rec.PID)
	return f.untrackErr
}

func (f *fakeRegistry) Owns(proc.Record) (bool, error) {
	f.mu.Lock()
	call := f.ownsCalls
	f.ownsCalls++
	result := ownsResult{owns: f.owns, err: f.ownsErr}
	if call < len(f.ownsResults) {
		result = f.ownsResults[call]
	}
	started := f.ownsStarted
	release := f.ownsRelease
	f.mu.Unlock()
	if call == 0 && started != nil {
		close(started)
	}
	if call == 0 && release != nil {
		<-release
	}
	return result.owns, result.err
}

func (f *fakeRegistry) Reap(context.Context) error { return f.reapErr }

func (f *fakeRegistry) recordCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

type signalCall struct {
	pid int
	sig syscall.Signal
}

type signalRecorder struct {
	mu    sync.Mutex
	calls []signalCall
}

func (r *signalRecorder) signal(pid int, sig syscall.Signal) error {
	r.mu.Lock()
	r.calls = append(r.calls, signalCall{pid: pid, sig: sig})
	r.mu.Unlock()
	return syscall.Kill(pid, sig)
}

func (r *signalRecorder) snapshot() []signalCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]signalCall(nil), r.calls...)
}

type observedBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	match string
	ready chan struct{}
	once  sync.Once
}

func newObservedBuffer(match string) *observedBuffer {
	return &observedBuffer{match: match, ready: make(chan struct{})}
}

func (b *observedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	n, err := b.buf.Write(p)
	matched := strings.Contains(b.buf.String(), b.match)
	b.mu.Unlock()
	if matched {
		b.once.Do(func() { close(b.ready) })
	}
	return n, err
}

func (b *observedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNewPoolValidation(t *testing.T) {
	if _, err := NewPool(0, newFakeRegistry()); err == nil {
		t.Fatal("NewPool accepted zero limit")
	}
	if _, err := NewPool(1, nil); err == nil {
		t.Fatal("NewPool accepted nil registry")
	}
	if TerminationGrace != 500*time.Millisecond {
		t.Fatalf("TerminationGrace = %s, want 500ms", TerminationGrace)
	}
}

func workerInput(t *testing.T, payload string) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "worker-input-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := file.WriteString(payload); err != nil {
		_ = file.Close()
		t.Fatalf("write worker input: %v", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = file.Close()
		t.Fatalf("rewind worker input: %v", err)
	}
	return file
}

func TestRunTracksBeforeDispatchAndReaps(t *testing.T) {
	started := make(chan int, 1)
	release := make(chan struct{})
	registry := newFakeRegistry()
	registry.trackStarted = started
	registry.trackRelease = release
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	out := newObservedBuffer("payload")
	input := workerInput(t, "payload")
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(context.Background(), Task{
			Path:   "/bin/cat",
			Stdin:  input,
			Stdout: out,
		})
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker was not offered for durable tracking")
	}
	if got := out.String(); got != "" {
		t.Fatalf("output before durable tracking = %q, want empty", got)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := out.String(); got != "payload" {
		t.Fatalf("output = %q, want payload", got)
	}
	if _, err := input.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("input Stat error = %v, want os.ErrClosed", err)
	}
	if registry.recordCount() != 0 {
		t.Fatalf("durable records = %d, want 0 after synchronous reap", registry.recordCount())
	}
	pool.Close()
	if err := pool.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestRunRejectsRecordWithoutBootBeforeDispatch(t *testing.T) {
	registry := newFakeRegistry()
	registry.recordBoot = ""
	registry.trackStarted = make(chan int, 1)
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	err = pool.Run(context.Background(), Task{
		Path:   "/bin/cat",
		Stdin:  workerInput(t, "must not execute"),
		Stdout: out,
	})
	if !errors.Is(err, proc.ErrInvalidRecord) {
		t.Fatalf("Run error = %v, want ErrInvalidRecord", err)
	}
	pid := <-registry.trackStarted
	assertPIDGone(t, pid)
	if got := out.String(); got != "" {
		t.Fatalf("output = %q, want no dispatch", got)
	}
	if got := registry.recordCount(); got != 0 {
		t.Fatalf("durable records = %d, want invalid record removed", got)
	}
}

func TestRunOwnsInputOnValidationFailure(t *testing.T) {
	pool, err := NewPool(1, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	input := workerInput(t, "unused")
	if err := pool.Run(t.Context(), Task{Stdin: input}); err == nil {
		t.Fatal("Run accepted an empty worker path")
	}
	if _, err := input.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("input Stat error = %v, want os.ErrClosed", err)
	}
}

func TestRunReturnsWorkerFailure(t *testing.T) {
	pool, err := NewPool(1, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	err = pool.Run(context.Background(), Task{Path: "/bin/sh", Args: []string{"-c", "exit 23"}})
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("Run error = %v, want exit status 23", err)
	}
	if got := strings.Count(err.Error(), "supervise: worker exit:"); got != 1 {
		t.Fatalf("worker exit wrapping count = %d, want exactly 1: %v", got, err)
	}
}

func TestRunSurfacesDurableUntrackFailureAfterReap(t *testing.T) {
	registry := newFakeRegistry()
	registry.untrackErr = errors.New("record store unavailable")
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.Run(context.Background(), Task{Path: "/usr/bin/true"})
	if !errors.Is(err, registry.untrackErr) || !strings.Contains(err.Error(), "untrack worker") {
		t.Fatalf("Run error = %v, want wrapped untrack failure", err)
	}
	pool.Close()
	if err := pool.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after reaped worker: %v", err)
	}
}

func TestRunCleansDaemonizedDescendantBeforeReturning(t *testing.T) {
	started := make(chan int, 1)
	registry := newFakeRegistry()
	registry.trackStarted = started
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 80 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "descendant.pid")
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(context.Background(), Task{
			Path: "/bin/sh",
			Args: []string{"-c", `
/bin/sh -c 'trap "" TERM; echo $$ > "$1"; while :; do sleep 10; done' descendant "$1" &
while [ ! -s "$1" ]; do sleep 0.01; done
exit 0
`, "worker", marker},
		})
	}()
	var leaderPID int
	select {
	case leaderPID = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("group leader was not tracked")
	}
	childPID := readPIDFile(t, marker)
	select {
	case err := <-done:
		t.Fatalf("Run returned before group cleanup grace: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not settle daemonized descendant")
	}
	assertPIDGone(t, leaderPID)
	assertPIDGone(t, childPID)
}

func TestRunCancellationEscalatesAgainstProcessGroup(t *testing.T) {
	registry := newFakeRegistry()
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 30 * time.Millisecond
	signals := &signalRecorder{}
	pool.signal = signals.signal
	out := newObservedBuffer("ready\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(ctx, Task{
			Path:   "/bin/sh",
			Args:   []string{"-c", `trap "" TERM; echo ready; while :; do sleep 10; done`},
			Stdout: out,
		})
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not become ready")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not settle after cancellation")
	}
	calls := signals.snapshot()
	if len(calls) != 2 || calls[0].pid >= 0 || calls[0].sig != syscall.SIGTERM || calls[1].pid != calls[0].pid || calls[1].sig != syscall.SIGKILL {
		t.Fatalf("signals = %v, want group SIGTERM then same-group SIGKILL", calls)
	}
	if registry.recordCount() != 0 {
		t.Fatalf("durable records = %d, want 0", registry.recordCount())
	}
}

func TestCancelReapsLeaderAndTermIgnoringDescendant(t *testing.T) {
	started := make(chan int, 1)
	registry := newFakeRegistry()
	registry.trackStarted = started
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 30 * time.Millisecond
	marker := filepath.Join(t.TempDir(), "descendant.pid")
	runDone := make(chan error, 1)
	go func() {
		runDone <- pool.Run(context.Background(), Task{
			Path: "/bin/sh",
			Args: []string{"-c", `
/bin/sh -c 'trap "" TERM; echo $$ > "$1"; while :; do sleep 10; done' descendant "$1" &
while [ ! -s "$1" ]; do sleep 0.01; done
wait
`, "worker", marker},
		})
	}()
	var leaderPID int
	select {
	case leaderPID = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("group leader was not tracked")
	}
	childPID := readPIDFile(t, marker)
	pool.Cancel()
	if err := pool.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Cancel")
	}
	assertPIDGone(t, leaderPID)
	assertPIDGone(t, childPID)
}

func TestCancellationRetainsRecordUntilIdentityProofRecovers(t *testing.T) {
	registry := newFakeRegistry()
	registry.ownsResults = []ownsResult{
		{err: errors.New("identity probe unavailable")},
		{owns: true},
	}
	registry.ownsStarted = make(chan struct{})
	releaseProof := make(chan struct{})
	registry.ownsRelease = releaseProof
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 20 * time.Millisecond
	signals := &signalRecorder{}
	pool.signal = signals.signal
	out := newObservedBuffer("ready\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(ctx, Task{
			Path:   "/bin/sh",
			Args:   []string{"-c", `trap "" TERM; echo ready; while :; do :; done`},
			Stdout: out,
		})
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not become ready")
	}
	cancel()
	select {
	case <-registry.ownsStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not enter identity revalidation")
	}
	if registry.recordCount() != 1 {
		t.Fatalf("durable records during failed proof = %d, want 1", registry.recordCount())
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before group proof recovered: %v", err)
	default:
	}
	close(releaseProof)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "identity probe unavailable") {
			t.Fatalf("Run error = %v, want cancellation and transient identity error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not settle after identity proof recovered")
	}
	calls := signals.snapshot()
	if len(calls) != 2 || calls[0].sig != syscall.SIGTERM || calls[1].sig != syscall.SIGKILL {
		t.Fatalf("group signals = %v, want TERM then KILL after proof recovery", calls)
	}
}

func TestCancellationReapsLeaderWhenGroupKillFails(t *testing.T) {
	pool, err := NewPool(1, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 20 * time.Millisecond
	var mu sync.Mutex
	var calls []signalCall
	pool.signal = func(pid int, sig syscall.Signal) error {
		mu.Lock()
		calls = append(calls, signalCall{pid: pid, sig: sig})
		callCount := len(calls)
		mu.Unlock()
		if sig == syscall.SIGKILL && callCount == 2 {
			return syscall.EPERM
		}
		return syscall.Kill(pid, sig)
	}
	out := newObservedBuffer("ready\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pool.Run(ctx, Task{
			Path:   "/bin/sh",
			Args:   []string{"-c", `trap "" TERM; echo ready; while :; do :; done`},
			Stdout: out,
		})
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not become ready")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EPERM) {
			t.Fatalf("Run error = %v, want cancellation and group-kill failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not reap leader after group SIGKILL failure")
	}
	mu.Lock()
	got := append([]signalCall(nil), calls...)
	mu.Unlock()
	if len(got) != 3 || got[0].sig != syscall.SIGTERM || got[1].sig != syscall.SIGKILL || got[2].sig != syscall.SIGKILL {
		t.Fatalf("group signals = %v, want TERM then retried KILL", got)
	}
}

func TestTrackFailureKillsWithoutDispatch(t *testing.T) {
	registry := newFakeRegistry()
	registry.trackErr = errors.New("disk unavailable")
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	signals := &signalRecorder{}
	pool.signal = signals.signal
	marker := filepath.Join(t.TempDir(), "dispatched")
	err = pool.Run(context.Background(), Task{
		Path:  "/bin/sh",
		Args:  []string{"-c", `IFS= read -r line && : > "$1"`, "worker", marker},
		Stdin: workerInput(t, "must-not-arrive\n"),
	})
	if err == nil || !strings.Contains(err.Error(), "track worker") {
		t.Fatalf("Run error = %v, want track failure", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dispatch marker stat = %v, want not exist", err)
	}
	calls := signals.snapshot()
	if len(calls) != 1 || calls[0].pid >= 0 || calls[0].sig != syscall.SIGKILL {
		t.Fatalf("signals = %v, want immediate process-group SIGKILL", calls)
	}
}

func TestCloseRejectsQueuedAndFutureWork(t *testing.T) {
	pool, err := NewPool(1, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	out := newObservedBuffer("ready\n")
	ctx, cancel := context.WithCancel(context.Background())
	active := make(chan error, 1)
	go func() {
		active <- pool.Run(ctx, Task{
			Path:   "/bin/sh",
			Args:   []string{"-c", `trap "exit 0" TERM; echo ready; while :; do sleep 10; done`},
			Stdout: out,
		})
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not become ready")
	}
	queued := make(chan error, 1)
	go func() { queued <- pool.Run(context.Background(), Task{Path: "/usr/bin/true"}) }()
	pool.Close()
	select {
	case err := <-queued:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("queued Run error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued Run was not rejected")
	}
	if err := pool.Run(context.Background(), Task{Path: "/usr/bin/true"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("future Run error = %v, want ErrClosed", err)
	}
	cancel()
	select {
	case <-active:
	case <-time.After(5 * time.Second):
		t.Fatal("active Run did not settle")
	}
}

func TestWaitDefersContextErrorUntilCancelSettlesWorkers(t *testing.T) {
	pool, err := NewPool(1, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 80 * time.Millisecond
	out := newObservedBuffer("ready\n")
	runDone := make(chan error, 1)
	go func() {
		runDone <- pool.Run(context.Background(), Task{
			Path:   "/bin/sh",
			Args:   []string{"-c", `trap "" TERM; echo ready; while :; do sleep 10; done`},
			Stdout: out,
		})
	}()
	select {
	case <-out.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not become ready")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waitCancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- pool.Wait(waitCtx) }()
	time.Sleep(40 * time.Millisecond)
	select {
	case err := <-waitDone:
		t.Fatalf("Wait returned before worker safety settlement: %v", err)
	default:
	}
	startedCancel := time.Now()
	pool.Cancel()
	select {
	case err := <-waitDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait error = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(startedCancel); elapsed < pool.grace {
			t.Fatalf("Wait returned after %s, before %s termination grace", elapsed, pool.grace)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Cancel settled worker")
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Cancel")
	}
	if err := pool.Run(context.Background(), Task{Path: "/usr/bin/true"}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run after Cancel error = %v, want ErrCanceled", err)
	}
}

func TestCancelTerminatesEveryActiveProcessGroup(t *testing.T) {
	pool, err := NewPool(2, newFakeRegistry())
	if err != nil {
		t.Fatal(err)
	}
	pool.grace = 30 * time.Millisecond
	signals := &signalRecorder{}
	pool.signal = signals.signal
	outputs := []*observedBuffer{newObservedBuffer("ready\n"), newObservedBuffer("ready\n")}
	runs := make(chan error, len(outputs))
	for _, out := range outputs {
		go func() {
			runs <- pool.Run(context.Background(), Task{
				Path:   "/bin/sh",
				Args:   []string{"-c", `trap "" TERM; echo ready; while :; do sleep 10; done`},
				Stdout: out,
			})
		}()
	}
	for _, out := range outputs {
		select {
		case <-out.ready:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not become ready")
		}
	}
	pool.Cancel()
	if err := pool.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	for range outputs {
		select {
		case err := <-runs:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after Cancel")
		}
	}
	calls := signals.snapshot()
	if len(calls) != 4 {
		t.Fatalf("signals = %v, want TERM and KILL for two process groups", calls)
	}
	groups := make(map[int]map[syscall.Signal]bool)
	for _, call := range calls {
		if call.pid >= 0 {
			t.Fatalf("signal target = %d, want process group", call.pid)
		}
		if groups[call.pid] == nil {
			groups[call.pid] = make(map[syscall.Signal]bool)
		}
		groups[call.pid][call.sig] = true
	}
	if len(groups) != 2 {
		t.Fatalf("signaled groups = %v, want two", groups)
	}
	for group, sent := range groups {
		if !sent[syscall.SIGTERM] || !sent[syscall.SIGKILL] {
			t.Fatalf("group %d signals = %v, want TERM and KILL", group, sent)
		}
	}
}

func TestRecoverDelegatesToRegistry(t *testing.T) {
	registry := newFakeRegistry()
	registry.reapErr = errors.New("store corrupt")
	pool, err := NewPool(1, registry)
	if err != nil {
		t.Fatal(err)
	}
	err = pool.Recover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "recover workers") || !errors.Is(err, registry.reapErr) {
		t.Fatalf("Recover error = %v, want wrapped registry error", err)
	}
}

func TestRecoverReapsLeaderlessTermIgnoringSession(t *testing.T) {
	ctx := context.Background()
	store := &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.json")}
	cmd := exec.Command("/bin/sh", "-c", `read ignored; /bin/sh -c 'trap "" TERM; printf "%s\n" "$$"; while :; do sleep 3600 & wait $!; done' & exit 0`)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("worker leader stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("worker leader stdout: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker leader: %v", err)
	}
	leaderPID := cmd.Process.Pid
	var waitOnce sync.Once
	waitLeader := func() { waitOnce.Do(func() { _ = cmd.Wait() }) }
	t.Cleanup(func() {
		_ = syscall.Kill(-leaderPID, syscall.SIGKILL)
		_ = cmd.Process.Kill()
		waitLeader()
	})

	prior := &proc.Reaper{Store: store, Generation: "prior-generation"}
	if _, err := prior.TrackGroup(ctx, leaderPID); err != nil {
		t.Fatalf("TrackGroup: %v", err)
	}
	if _, err := stdin.Write([]byte("start\n")); err != nil {
		t.Fatalf("release worker leader: %v", err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatalf("close worker leader stdin: %v", err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read descendant pid: %v", err)
	}
	descendantPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("parse descendant pid %q: %v", line, err)
	}
	waitLeader()
	if err := syscall.Kill(descendantPID, 0); err != nil {
		t.Fatalf("descendant %d did not survive leader exit: %v", descendantPID, err)
	}

	next := &proc.Reaper{Store: store, Generation: "next-generation", Grace: 30 * time.Millisecond}
	pool, err := NewPool(1, next)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	if err := pool.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertPIDGone(t, descendantPID)
	records, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load worker records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("worker records after recovery = %v, want empty", records)
	}
}

func readPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		body, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
			if err != nil {
				t.Fatalf("parse pid file %s: %v", path, err)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read pid file %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pid file %s was not created", path)
	return 0
}

func assertPIDGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("pid %d remains signalable after group settlement: %v", pid, err)
	}
}
