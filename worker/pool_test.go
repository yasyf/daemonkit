package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestConfigRequiresEveryBound(t *testing.T) {
	valid := workerTestConfig()
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "capacity", mutate: func(c *Config) { c.Capacity = 0 }},
		{name: "queue", mutate: func(c *Config) { c.QueueCapacity = -1 }},
		{name: "runtime", mutate: func(c *Config) { c.MaxTotalRun = totalReserve }},
		{name: "stdin", mutate: func(c *Config) { c.MaxStdinBytes = -1 }},
		{name: "stdout", mutate: func(c *Config) { c.MaxStdoutBytes = 0 }},
		{name: "stderr", mutate: func(c *Config) { c.MaxStderrBytes = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if _, err := NewPool(config, workerTestReaper(t)); err == nil {
				t.Fatal("NewPool accepted incomplete configuration")
			}
		})
	}
}

func TestRuntimeClaimAndRecoveryGateAdmission(t *testing.T) {
	pool, err := NewPool(workerTestConfig(), workerTestReaper(t))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	request := shellRequest(t.TempDir(), "printf ready")
	if result, err := pool.Run(context.Background(), request); err == nil || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("unclaimed Run = %+v, %v", result, err)
	}
	claim, err := pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("ClaimRuntime: %v", err)
	}
	if result, err := pool.Run(context.Background(), request); err == nil || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("unrecovered Run = %+v, %v", result, err)
	}
	if err := claim.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if result, err := pool.Run(context.Background(), request); err == nil || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("inactive Run = %+v, %v", result, err)
	}
	if err := claim.Release(context.Background()); err != nil {
		t.Fatalf("Release before activation: %v", err)
	}
	if result, err := pool.Run(context.Background(), request); err == nil || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("released Run = %+v, %v", result, err)
	}
	if err := claim.Activate(); !errors.Is(err, ErrRuntimeOwnership) {
		t.Fatalf("stale claim Activate = %v", err)
	}
	claim, err = pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("second ClaimRuntime: %v", err)
	}
	if err := claim.Recover(context.Background()); err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if _, err := pool.Run(context.Background(), request); err != nil {
		t.Fatalf("recovered Run: %v", err)
	}
	if err := claim.Release(context.Background()); !errors.Is(err, ErrRuntimeOwnership) {
		t.Fatalf("Release accepted an activated pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := claim.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRuntimeClaimReleaseSettlesFailedRecoveryExactlyOnce(t *testing.T) {
	pool, err := NewPool(workerTestConfig(), workerTestReaper(t))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	claim, err := pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("ClaimRuntime: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := claim.Recover(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover canceled = %v", err)
	}

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			<-start
			errs <- claim.Release(context.Background())
		}()
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Release: %v", err)
		}
	}
	if err := claim.Release(canceled); err != nil {
		t.Fatalf("idempotent Release with canceled context: %v", err)
	}

	next, err := pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("ClaimRuntime after failed recovery release: %v", err)
	}
	if err := next.Release(context.Background()); err != nil {
		t.Fatalf("Release unrecovered claim: %v", err)
	}
}

func TestRunCapturesBoundedStreamsAndImmutableReceipt(t *testing.T) {
	pool := newWorkerTestPool(t, workerTestConfig())
	result, err := pool.Run(context.Background(), CommandRequest{
		Path: "/bin/sh", Dir: t.TempDir(),
		Args: []string{"-c", `IFS= read -r line; printf 'out:%s:%s' "$VALUE" "$line"; printf 'err:%s' "$VALUE" >&2`},
		Env:  []string{"VALUE=exact"}, Stdin: []byte("input\n"), TotalTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(result.Stdout) != "out:exact:input" || string(result.Stderr) != "err:exact" || result.ExitCode != 0 {
		t.Fatalf("result = stdout %q stderr %q exit %d", result.Stdout, result.Stderr, result.ExitCode)
	}
	identity := result.Receipt.ProcessIdentity()
	if identity.PID <= 1 || identity.StartTime == "" || identity.Boot == "" {
		t.Fatalf("receipt identity = %+v", identity)
	}
	if result.Receipt.ExpectedExecutable() != "/bin/sh" {
		t.Fatalf("receipt executable = %q", result.Receipt.ExpectedExecutable())
	}
	if _, present := result.Receipt.ExpectedSignature(); present || result.Receipt.RequiresPeerFence() {
		t.Fatalf("worker receipt carried a peer fence or signature: %+v", result.Receipt)
	}
	if result.Receipt.RequestDigest() == (proc.SpawnRequestDigest{}) {
		t.Fatal("worker receipt lost immutable request digest")
	}

	result.Stdout[0] = 'X'
	again, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf second"))
	if err != nil || string(again.Stdout) != "second" {
		t.Fatalf("second Run = %q, %v", again.Stdout, err)
	}
}

func TestRunDrainsExactLargeOutputTailsAfterSettlement(t *testing.T) {
	config := workerTestConfig()
	config.MaxStdoutBytes = 128 << 10
	config.MaxStderrBytes = 128 << 10
	pool := newWorkerTestPool(t, config)
	const chunk = "0123456789abcdef"
	const errChunk = "fedcba9876543210"
	result, err := pool.Run(context.Background(), shellRequest(t.TempDir(),
		`i=0; while [ "$i" -lt 4096 ]; do printf 0123456789abcdef; printf fedcba9876543210 >&2; i=$((i+1)); done`,
	))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, want := string(result.Stdout), strings.Repeat(chunk, 4096); got != want {
		t.Fatalf("stdout bytes=%d want=%d tail=%q", len(got), len(want), got[max(0, len(got)-32):])
	}
	if got, want := string(result.Stderr), strings.Repeat(errChunk, 4096); got != want {
		t.Fatalf("stderr bytes=%d want=%d tail=%q", len(got), len(want), got[max(0, len(got)-32):])
	}
}

func TestRunReturnsResultWithTypedNonzeroExit(t *testing.T) {
	pool := newWorkerTestPool(t, workerTestConfig())
	result, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf retained; printf problem >&2; exit 17"))
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode != 17 {
		t.Fatalf("Run error = %v, want ExitError(17)", err)
	}
	if string(result.Stdout) != "retained" || string(result.Stderr) != "problem" || result.ExitCode != 17 {
		t.Fatalf("result = %+v", result)
	}
	if result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatal("nonzero exit lost process receipt")
	}
}

func TestRunRejectsInputBeforeSpawnAndKillsOnOutputLimit(t *testing.T) {
	config := workerTestConfig()
	config.MaxStdinBytes = 3
	config.MaxStdoutBytes = 4
	pool := newWorkerTestPool(t, config)

	result, err := pool.Run(context.Background(), CommandRequest{
		Path: "/bin/cat", Dir: t.TempDir(), Stdin: []byte("four"), TotalTimeout: 2 * time.Second,
	})
	if !errors.Is(err, ErrInputLimit) || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("input rejection = %+v, %v", result, err)
	}

	result, err = pool.Run(context.Background(), shellRequest(t.TempDir(), "while :; do printf 12345; done"))
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("output limit error = %v", err)
	}
	if string(result.Stdout) != "1234" || result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatalf("output-limited result = %+v", result)
	}
	if _, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf ok")); err != nil {
		t.Fatalf("pool did not reuse after settled output kill: %v", err)
	}
}

func TestClaimRuntimeVerifierLaneIgnoresPathologicalProductBudgets(t *testing.T) {
	config := workerTestConfig()
	config.MaxStdoutBytes = 1
	config.MaxStderrBytes = 1
	pool, err := NewPool(config, workerTestReaper(t))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	claim, err := pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("ClaimRuntime: %v", err)
	}
	if err := claim.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	const verdict = `{"protocol":1,"result":"trusted"}`
	result, err := claim.RunVerifier(context.Background(), shellRequest(t.TempDir(), "printf '"+verdict+"'"))
	if err != nil || string(result.Stdout) != verdict {
		t.Fatalf("verifier lane under 1-byte product budget = %q, %v", result.Stdout, err)
	}
	if _, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf '"+verdict+"'")); !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("product lane escaped its own budget: %v", err)
	}
}

func TestRunTimeoutAndCallerCancellationReturnPartialReceipt(t *testing.T) {
	pool := newWorkerTestPool(t, workerTestConfig())

	result, err := pool.Run(context.Background(), CommandRequest{
		Path: "/bin/sh", Dir: t.TempDir(), Args: []string{"-c", "printf partial; sleep 10"},
		TotalTimeout: 1600 * time.Millisecond,
	})
	if !errors.Is(err, ErrTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error = %v", err)
	}
	if result.Receipt.ProcessIdentity().PID == 0 || !strings.HasPrefix(string(result.Stdout), "partial") {
		t.Fatalf("timeout result = %+v", result)
	}

	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		result CommandResult
		err    error
	}, 1)
	go func() {
		result, err := pool.Run(ctx, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
		done <- struct {
			result CommandResult
			err    error
		}{result: result, err: err}
	}()
	waitForPath(t, marker)
	cancel()
	canceled := <-done
	if !errors.Is(canceled.err, ErrCanceled) || !errors.Is(canceled.err, context.Canceled) {
		t.Fatalf("cancellation error = %v", canceled.err)
	}
	if canceled.result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatal("cancellation lost process receipt")
	}
}

func TestRunTimeoutClosesAndJoinsBlockedInput(t *testing.T) {
	config := workerTestConfig()
	config.MaxStdinBytes = 1 << 20
	pool := newWorkerTestPool(t, config)
	request := shellRequest(t.TempDir(), "trap '' TERM; sleep 10")
	request.Stdin = make([]byte, 1<<20)
	request.TotalTimeout = 1600 * time.Millisecond
	result, err := pool.Run(context.Background(), request)
	if !errors.Is(err, ErrTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked-input timeout = %v", err)
	}
	if result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatal("blocked-input timeout lost receipt")
	}
	if _, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf joined")); err != nil {
		t.Fatalf("pool did not reuse after joining blocked input: %v", err)
	}
}

func TestRunJoinsAndReportsInputDeliveryFailure(t *testing.T) {
	config := workerTestConfig()
	config.MaxStdinBytes = 1 << 20
	pool := newWorkerTestPool(t, config)
	request := shellRequest(t.TempDir(), "exec 0<&-; sleep 0.1")
	request.Stdin = make([]byte, 1<<20)
	_, err := pool.Run(context.Background(), request)
	if !errors.Is(err, ErrInputDelivery) {
		t.Fatalf("input delivery error = %v", err)
	}
	if _, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "printf joined")); err != nil {
		t.Fatalf("pool did not reuse after joining failed input: %v", err)
	}
}

func TestWriteInputRejectsShortDeliveryAndClosesEndpoint(t *testing.T) {
	writer := &shortWriteCloser{}
	err := writeInput(writer, []byte("exact"))
	if !errors.Is(err, io.ErrShortWrite) || !writer.closed {
		t.Fatalf("writeInput = %v closed=%t", err, writer.closed)
	}
}

type shortWriteCloser struct{ closed bool }

func (w *shortWriteCloser) Write(input []byte) (int, error) { return len(input) - 1, nil }

func (w *shortWriteCloser) Close() error {
	w.closed = true
	return nil
}

func TestQueueCapacityIsNonblockingAndDeadlineCoversQueue(t *testing.T) {
	config := workerTestConfig()
	config.QueueCapacity = 0
	pool := newWorkerTestPool(t, config)
	marker := filepath.Join(t.TempDir(), "active")
	ctx, cancel := context.WithCancel(context.Background())
	first := runAsync(ctx, pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
	waitForPath(t, marker)

	result, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "exit 0"))
	if !errors.Is(err, ErrCapacity) || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("capacity result = %+v, %v", result, err)
	}
	cancel()
	<-first

	config.QueueCapacity = 1
	pool = newWorkerTestPool(t, config)
	marker = filepath.Join(t.TempDir(), "queued-active")
	ctx, cancel = context.WithCancel(context.Background())
	first = runAsync(ctx, pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
	waitForPath(t, marker)
	queued := shellRequest(t.TempDir(), "exit 0")
	queued.TotalTimeout = 1540 * time.Millisecond
	result, err = pool.Run(context.Background(), queued)
	if !errors.Is(err, ErrTimedOut) || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("queued timeout = %+v, %v", result, err)
	}
	cancel()
	<-first
}

func TestRunRejectsBudgetWithoutSettlementReserve(t *testing.T) {
	pool := newWorkerTestPool(t, workerTestConfig())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := pool.Run(ctx, shellRequest(t.TempDir(), "exit 0"))
	if !errors.Is(err, ErrBudgetTooSmall) || result.Receipt.ProcessIdentity().PID != 0 || pool.Active() != 0 {
		t.Fatalf("small budget = %+v, %v active=%d", result, err, pool.Active())
	}
}

func TestTotalTimeoutBoundsTerminationReapAndCollectors(t *testing.T) {
	config := workerTestConfig()
	config.MaxTotalRun = 1800 * time.Millisecond
	pool := newWorkerTestPool(t, config)
	request := shellRequest(t.TempDir(), "trap '' TERM; sleep 10")
	request.TotalTimeout = 1700 * time.Millisecond
	started := time.Now()
	result, err := pool.Run(context.Background(), request)
	elapsed := time.Since(started)
	if !errors.Is(err, ErrTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("total timeout error = %v", err)
	}
	if result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatal("total timeout lost process receipt")
	}
	if elapsed > request.TotalTimeout+200*time.Millisecond {
		t.Fatalf("Run elapsed %s beyond total timeout %s", elapsed, request.TotalTimeout)
	}
}

func TestDeadlineCoversLifecycleAdmission(t *testing.T) {
	pool, claim := newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	if err := claim.acquire(context.Background()); err != nil {
		t.Fatalf("acquire lifecycle: %v", err)
	}
	request := shellRequest(t.TempDir(), "exit 0")
	request.TotalTimeout = 1540 * time.Millisecond
	result, err := pool.Run(context.Background(), request)
	claim.releaseLifecycle()
	if !errors.Is(err, ErrTimedOut) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lifecycle admission timeout = %v", err)
	}
	if result.Receipt.ProcessIdentity().PID != 0 || pool.Active() != 0 {
		t.Fatalf("lifecycle admission spawned work: %+v active=%d", result, pool.Active())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := claim.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRunDeepCopiesQueuedRequest(t *testing.T) {
	pool := newWorkerTestPool(t, workerTestConfig())
	request := CommandRequest{
		Path: "/bin/sh", Dir: t.TempDir(),
		Args: []string{"-c", `printf '%s|%s|' "$1" "$VALUE"; cat`, "worker", "original"},
		Env:  []string{"VALUE=exact"}, Stdin: []byte("input"), TotalTimeout: 2 * time.Second,
	}
	copied, err := pool.copyAndValidate(request)
	if err != nil {
		t.Fatalf("copy request: %v", err)
	}
	request.Args[3] = "mutated"
	request.Env[0] = "VALUE=mutated"
	request.Stdin[0] = 'X'
	if copied.Args[3] != "original" || copied.Env[0] != "VALUE=exact" || string(copied.Stdin) != "input" {
		t.Fatalf("copied request changed: %+v", copied)
	}
}

func TestCloseCancelsActiveWorkAndIsTerminalIdempotent(t *testing.T) {
	pool, claim := newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	marker := filepath.Join(t.TempDir(), "active")
	running := runAsync(context.Background(), pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
	waitForPath(t, marker)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := claim.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stopped := <-running
	if !errors.Is(stopped.err, ErrCanceled) || stopped.result.Receipt.ProcessIdentity().PID == 0 {
		t.Fatalf("stopped run = %+v, %v", stopped.result, stopped.err)
	}
	if err := claim.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	result, err := pool.Run(context.Background(), shellRequest(t.TempDir(), "exit 0"))
	if !errors.Is(err, ErrClosed) || result.Receipt.ProcessIdentity().PID != 0 {
		t.Fatalf("post-close Run = %+v, %v", result, err)
	}
}

func TestCloseJoinsEveryAdmittedRun(t *testing.T) {
	config := workerTestConfig()
	config.QueueCapacity = 1
	pool, claim := newWorkerTestPoolWithoutCleanup(t, config)
	marker := filepath.Join(t.TempDir(), "active")
	active := runAsync(context.Background(), pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
	waitForPath(t, marker)
	queued := runAsync(context.Background(), pool, shellRequest(t.TempDir(), "sleep 10"))
	waitFor(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		return pool.inflight == 2
	}, "queued worker admission")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := claim.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for name, done := range map[string]<-chan asyncRun{"active": active, "queued": queued} {
		select {
		case result := <-done:
			if !errors.Is(result.err, ErrCanceled) {
				t.Fatalf("%s Run = %+v, %v", name, result.result, result.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Close returned before %s Run joined", name)
		}
	}
}

func TestConcurrentCloseCallersShareOneStickyResult(t *testing.T) {
	pool, claim := newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	marker := filepath.Join(t.TempDir(), "active")
	running := runAsync(context.Background(), pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; sleep 10"))
	waitForPath(t, marker)
	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			results <- claim.Close(ctx)
		}()
	}
	for range callers {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Close = %v", err)
		}
	}
	if result := <-running; !errors.Is(result.err, ErrCanceled) {
		t.Fatalf("active Run = %+v, %v", result.result, result.err)
	}
}

func TestPreexistingSettlementFailureRemainsStickyThroughClose(t *testing.T) {
	_, claim := newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	sentinel := errors.New("forced settlement failure")
	first := claim.terminalize(errors.Join(ErrSettlementIncomplete, sentinel))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	closed := claim.Close(ctx)
	if !errors.Is(closed, ErrSettlementIncomplete) || !errors.Is(closed, sentinel) || closed.Error() != first.Error() {
		t.Fatalf("Close = %v, want sticky %v", closed, first)
	}
	if again := claim.Close(context.Background()); again == nil || again.Error() != first.Error() {
		t.Fatalf("second Close = %v, want sticky %v", again, first)
	}

	_, claim = newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	first = claim.terminalize(errors.Join(ErrSettlementIncomplete, sentinel))
	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	if closed := claim.Close(canceled); closed == nil || closed.Error() != first.Error() {
		t.Fatalf("canceled Close = %v, want preexisting sticky %v", closed, first)
	}
}

func TestCloseIncompleteResultIsStickyAndJoinsActiveRun(t *testing.T) {
	pool, claim := newWorkerTestPoolWithoutCleanup(t, workerTestConfig())
	marker := filepath.Join(t.TempDir(), "active")
	running := runAsync(context.Background(), pool, shellRequest(filepath.Dir(marker), "touch "+marker+"; trap '' TERM; sleep 10"))
	waitForPath(t, marker)
	closeCtx, cancel := context.WithCancel(context.Background())
	cancel()
	firstErr := claim.Close(closeCtx)
	if !errors.Is(firstErr, ErrSettlementIncomplete) || !errors.Is(firstErr, context.Canceled) {
		t.Fatalf("first Close = %v, want sticky incomplete cancellation", firstErr)
	}
	secondErr := claim.Close(context.Background())
	if !errors.Is(secondErr, ErrSettlementIncomplete) || secondErr.Error() != firstErr.Error() {
		t.Fatalf("second Close = %v, want sticky %v", secondErr, firstErr)
	}
	for attempt := range 256 {
		again := claim.Close(closeCtx)
		if again == nil || again.Error() != firstErr.Error() {
			t.Fatalf("canceled repeat %d = %v, want sticky %v", attempt, again, firstErr)
		}
	}
	select {
	case stopped := <-running:
		if stopped.err == nil || stopped.result.Receipt.ProcessIdentity().PID == 0 {
			t.Fatalf("active Run after incomplete Close = %+v, %v", stopped.result, stopped.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("active Run survived terminal Close")
	}
}

type asyncRun struct {
	result CommandResult
	err    error
}

func runAsync(ctx context.Context, pool *Pool, request CommandRequest) <-chan asyncRun {
	done := make(chan asyncRun, 1)
	go func() {
		result, err := pool.Run(ctx, request)
		done <- asyncRun{result: result, err: err}
	}()
	return done
}

func workerTestConfig() Config {
	return Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: 3 * time.Second,
		MaxStdinBytes: 1024, MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
	}
}

func workerTestVerifierBudgets() VerifierBudgets {
	return VerifierBudgets{
		MaxTotalRun: 3 * time.Second, MaxStdinBytes: 1024, MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
	}
}

func workerTestReaper(t *testing.T) *proc.Reaper {
	t.Helper()
	generation, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	return &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(t.TempDir(), "workers.db")},
		Generation: generation,
		Grace:      10 * time.Millisecond, Settlement: time.Second,
	}
}

func newWorkerTestPool(t *testing.T, config Config) *Pool {
	t.Helper()
	pool, claim := newWorkerTestPoolWithoutCleanup(t, config)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := claim.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return pool
}

func newWorkerTestPoolWithoutCleanup(t *testing.T, config Config) (*Pool, *RuntimeClaim) {
	t.Helper()
	pool, err := NewPool(config, workerTestReaper(t))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	claim, err := pool.ClaimRuntime(workerTestVerifierBudgets())
	if err != nil {
		t.Fatalf("ClaimRuntime: %v", err)
	}
	if err := claim.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := claim.Activate(); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	return pool, claim
}

func shellRequest(dir, script string) CommandRequest {
	return CommandRequest{Path: "/bin/sh", Dir: dir, Args: []string{"-c", script}, TotalTimeout: 2 * time.Second}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	waitFor(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, "path "+path)
}

func waitFor(t *testing.T, ready func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
