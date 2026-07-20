// Package supervise runs bounded disposable worker processes. Each task gets a
// fresh process group and is durably tracked before its payload is delivered.
package supervise

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// TerminationGrace is the fixed interval between a worker group's SIGTERM and
// identity-revalidated SIGKILL.
const TerminationGrace = 500 * time.Millisecond

const settlementRetry = 25 * time.Millisecond

const workerWrapper = `
trap ':' TERM
printf r >&3
exec 3>&-
while ! IFS= read -r _ <&4; do
    :
done
exec 4<&-
(
    trap - TERM
    exec "$@"
) <&5 &
exec 5<&-
worker_pid=$!
while :; do
    wait "$worker_pid"
    worker_status=$?
    if ! kill -0 "$worker_pid" 2>/dev/null; then
        break
    fi
done
printf '%s\n' "$worker_status" >&6
exec 6>&-
while :; do
    sleep 3600 &
    wait $!
done
`

var (
	// ErrClosed means Close permanently stopped task admission.
	ErrClosed = errors.New("worker pool closed")
	// ErrCanceled means Cancel permanently stopped task admission and canceled
	// every admitted task.
	ErrCanceled = errors.New("worker pool canceled")
	// ErrUnsettledGroup means the durable group leader exited before the process
	// group could be identity-gated and terminated. Its record is retained.
	ErrUnsettledGroup = errors.New("worker process group is not settled")
)

// ExitError reports the exact exit status returned by the disposable worker.
type ExitError struct {
	Code int
}

func (e *ExitError) Error() string { return fmt.Sprintf("worker exited with status %d", e.Code) }

// ExitCode returns the worker's numeric exit status.
func (e *ExitError) ExitCode() int { return e.Code }

// Workers is the lifecycle contract for a disposable worker pool.
type Workers interface {
	Close()
	Cancel()
	Wait(ctx context.Context) error
}

// WorkerRegistry persists worker identities across daemon generations.
// *proc.Reaper implements WorkerRegistry.
type WorkerRegistry interface {
	TrackGroup(ctx context.Context, pid int, class proc.RecoveryClass) (proc.Record, error)
	Untrack(ctx context.Context, rec proc.Record) error
	Owns(rec proc.Record) (bool, error)
	Reap(ctx context.Context) error
}

// Task describes one disposable worker invocation. Stdin is withheld from the
// worker until its process-group identity is durable.
type Task struct {
	// RecoveryClass names the exact recovery barrier for a crash receipt.
	RecoveryClass proc.RecoveryClass
	// Path is the worker executable.
	Path string
	// Args are the worker arguments after Path.
	Args []string
	// Dir is the worker's working directory. Empty inherits the caller's.
	Dir string
	// Env is the worker environment. Nil inherits the caller's environment.
	Env []string
	// Stdin is passed directly to the worker after durable identity tracking.
	// Pool takes ownership and closes it before Run returns. Nil reads EOF.
	Stdin *os.File
	// Stdout and Stderr receive worker output. Nil discards the corresponding
	// stream, following os/exec semantics.
	Stdout io.Writer
	Stderr io.Writer
}

// TaskRunner executes one killable, synchronously reaped disposable task.
type TaskRunner interface {
	Run(context.Context, Task) error
}

// Pool bounds concurrently running disposable worker processes.
type Pool struct {
	limit    int
	registry WorkerRegistry

	mu       sync.Mutex
	active   int
	closed   bool
	canceled bool
	changed  chan struct{}
	nextID   uint64
	workers  map[uint64]context.CancelFunc

	grace  time.Duration
	signal func(int, syscall.Signal) error
}

// NewPool builds a worker pool. limit and registry are required.
func NewPool(limit int, registry WorkerRegistry) (*Pool, error) {
	if limit <= 0 {
		return nil, errors.New("supervise: worker limit must be positive")
	}
	if registry == nil {
		return nil, errors.New("supervise: worker registry is required")
	}
	return &Pool{
		limit:    limit,
		registry: registry,
		changed:  make(chan struct{}),
		workers:  make(map[uint64]context.CancelFunc),
		grace:    TerminationGrace,
		signal:   syscall.Kill,
	}, nil
}

var (
	_ Workers    = (*Pool)(nil)
	_ TaskRunner = (*Pool)(nil)
)

// Close permanently stops new task admission. Running tasks are unchanged.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.notifyLocked()
}

// Cancel permanently stops task admission and asks every admitted task to run
// its identity-gated process-group termination ladder. Call Wait to join them.
func (p *Pool) Cancel() {
	p.mu.Lock()
	if p.canceled {
		p.mu.Unlock()
		return
	}
	p.canceled = true
	cancels := make([]context.CancelFunc, 0, len(p.workers))
	for _, cancel := range p.workers {
		cancels = append(cancels, cancel)
	}
	p.notifyLocked()
	p.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

// Wait joins every admitted task. A caller deadline is reported only after all
// admitted workers have been reaped or proven no longer to own their record.
func (p *Pool) Wait(ctx context.Context) error {
	done := ctx.Done()
	var ctxErr error
	for {
		p.mu.Lock()
		if p.active == 0 {
			p.mu.Unlock()
			if ctxErr == nil {
				ctxErr = ctx.Err()
			}
			if ctxErr != nil {
				return fmt.Errorf("supervise: wait for workers: %w", ctxErr)
			}
			return nil
		}
		changed := p.changed
		p.mu.Unlock()

		select {
		case <-changed:
		case <-done:
			ctxErr = ctx.Err()
			done = nil
		}
	}
}

// Recover settles every durable prior-generation worker identity.
func (p *Pool) Recover(ctx context.Context) error {
	err := p.registry.Reap(ctx)
	if err != nil {
		return fmt.Errorf("supervise: recover workers: %w", err)
	}
	return nil
}

// Run executes one task and synchronously reaps its process. Cancellation and
// deadlines terminate the entire worker process group before Run returns.
func (p *Pool) Run(ctx context.Context, task Task) error {
	if err := task.RecoveryClass.Validate(); err != nil {
		return fmt.Errorf("supervise: worker recovery class: %w", err)
	}
	stdin := task.Stdin
	if stdin == nil {
		var err error
		stdin, err = os.Open(os.DevNull)
		if err != nil {
			return fmt.Errorf("supervise: open empty worker stdin: %w", err)
		}
	}
	defer stdin.Close()
	if task.Path == "" {
		return errors.New("supervise: worker path is required")
	}
	workerCtx, workerID, cancel, err := p.acquire(ctx, ctx)
	if err != nil {
		return err
	}
	defer p.release(workerID)
	defer cancel()
	if err := workerCtx.Err(); err != nil {
		return fmt.Errorf("supervise: worker canceled before start: %w", err)
	}

	wrapperArgs := make([]string, 0, len(task.Args)+4)
	wrapperArgs = append(wrapperArgs, "-c", workerWrapper, "daemonkit-worker", task.Path)
	wrapperArgs = append(wrapperArgs, task.Args...)
	cmd := exec.Command("/bin/sh", wrapperArgs...)
	cmd.Dir = task.Dir
	cmd.Env = task.Env
	cmd.Stdout = task.Stdout
	cmd.Stderr = task.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("supervise: worker readiness pipe: %w", err)
	}
	gateR, gateW, err := os.Pipe()
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		return fmt.Errorf("supervise: worker dispatch gate: %w", err)
	}
	statusR, statusW, err := os.Pipe()
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		_ = gateR.Close()
		_ = gateW.Close()
		return fmt.Errorf("supervise: worker status pipe: %w", err)
	}
	cmd.ExtraFiles = []*os.File{readyW, gateR, stdin, statusW}
	if err := cmd.Start(); err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		_ = gateR.Close()
		_ = gateW.Close()
		_ = statusR.Close()
		_ = statusW.Close()
		return fmt.Errorf("supervise: start worker: %w", err)
	}
	_ = readyW.Close()
	_ = gateR.Close()
	_ = statusW.Close()
	if err := awaitWrapperReady(workerCtx, readyR); err != nil {
		_ = gateW.Close()
		_ = statusR.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return errors.Join(
			fmt.Errorf("supervise: await worker readiness: %w", err),
			killErr,
			unexpectedWaitError(waitErr),
		)
	}

	rec, err := p.registry.TrackGroup(workerCtx, cmd.Process.Pid, task.RecoveryClass)
	if err != nil {
		_ = gateW.Close()
		_ = statusR.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return errors.Join(
			fmt.Errorf("supervise: track worker: %w", err),
			killErr,
			unexpectedWaitError(waitErr),
		)
	}
	if recordErr := rec.Validate(); recordErr != nil || rec.PID != cmd.Process.Pid ||
		!rec.ProcessGroup || rec.SessionID != rec.PID {
		_ = gateW.Close()
		_ = statusR.Close()
		untrackErr := p.registry.Untrack(context.WithoutCancel(workerCtx), rec)
		if untrackErr != nil {
			untrackErr = fmt.Errorf("supervise: untrack invalid worker process record: %w", untrackErr)
		}
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return errors.Join(
			errors.New("supervise: registry returned an invalid worker process record"),
			recordErr,
			untrackErr,
			killErr,
			unexpectedWaitError(waitErr),
		)
	}

	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	status := readWorkerStatus(statusR)
	if err := workerCtx.Err(); err != nil {
		stop := p.stop(rec, waited)
		_ = gateW.Close()
		statusResult := <-status
		return p.finish(workerCtx, rec, errors.Join(
			fmt.Errorf("supervise: worker canceled before dispatch: %w", err),
			stop.err,
			wrapStatusReadError(statusResult.err),
			unexpectedWaitError(stop.waitErr),
		), stop.settled)
	}
	if err := writePayload(gateW, []byte("start\n")); err != nil {
		stop := p.stop(rec, waited)
		statusResult := <-status
		return p.finish(workerCtx, rec, errors.Join(
			fmt.Errorf("supervise: release worker dispatch gate: %w", err),
			stop.err,
			wrapStatusReadError(statusResult.err),
			unexpectedWaitError(stop.waitErr),
		), stop.settled)
	}

	return p.await(workerCtx, rec, status, waited)
}

func (p *Pool) acquire(
	wait context.Context,
	lifetime context.Context,
) (context.Context, uint64, context.CancelFunc, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, 0, nil, ErrClosed
		}
		if p.canceled {
			p.mu.Unlock()
			return nil, 0, nil, ErrCanceled
		}
		if p.active < p.limit {
			workerCtx, cancel := context.WithCancel(lifetime)
			p.active++
			p.nextID++
			workerID := p.nextID
			p.workers[workerID] = cancel
			p.mu.Unlock()
			return workerCtx, workerID, cancel, nil
		}
		changed := p.changed
		p.mu.Unlock()

		select {
		case <-wait.Done():
			return nil, 0, nil, fmt.Errorf("supervise: wait for worker slot: %w", wait.Err())
		case <-changed:
		}
	}
}

func (p *Pool) release(workerID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.workers[workerID]
	if !ok {
		panic(fmt.Sprintf("supervise: release unknown worker %d", workerID))
	}
	delete(p.workers, workerID)
	p.active--
	p.notifyLocked()
}

func (p *Pool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Pool) await(
	ctx context.Context,
	rec proc.Record,
	status <-chan workerResult,
	waited <-chan error,
) error {
	select {
	case statusResult := <-status:
		stop := p.stop(rec, waited)
		return p.finish(ctx, rec, errors.Join(
			workerExitError(statusResult),
			stop.err,
			unexpectedWaitError(stop.waitErr),
		), stop.settled)
	case waitErr := <-waited:
		statusResult := <-status
		return p.finish(ctx, rec, errors.Join(
			wrapWaitError(waitErr),
			wrapStatusReadError(statusResult.err),
		), false)
	case <-ctx.Done():
		stop := p.stop(rec, waited)
		<-status
		return p.finish(ctx, rec, errors.Join(
			fmt.Errorf("supervise: worker canceled: %w", ctx.Err()),
			stop.err,
			unexpectedWaitError(stop.waitErr),
		), stop.settled)
	}
}

type stopResult struct {
	waitErr error
	err     error
	settled bool
}

func (p *Pool) stop(rec proc.Record, waited <-chan error) stopResult {
	termErr := p.signal(-rec.PID, syscall.SIGTERM)
	if errors.Is(termErr, syscall.ESRCH) {
		termErr = nil
	}
	timer := time.NewTimer(p.grace)
	defer timer.Stop()
	select {
	case waitErr := <-waited:
		return stopResult{waitErr: waitErr, err: errors.Join(
			wrapSignalError("terminate worker group", termErr),
			ErrUnsettledGroup,
		)}
	case <-timer.C:
	}

	var settlementErr error
	for {
		owned, err := p.registry.Owns(rec)
		if err != nil {
			if settlementErr == nil {
				settlementErr = fmt.Errorf("supervise: revalidate worker before SIGKILL: %w", err)
			}
			if retry := waitSettlementRetry(waited); retry.exited {
				return stopResult{waitErr: retry.waitErr, err: errors.Join(settlementErr, ErrUnsettledGroup)}
			}
			continue
		}
		if !owned {
			if settlementErr == nil {
				settlementErr = errors.New("supervise: worker identity changed before SIGKILL")
			}
			if retry := waitSettlementRetry(waited); retry.exited {
				return stopResult{waitErr: retry.waitErr, err: errors.Join(settlementErr, ErrUnsettledGroup)}
			}
			continue
		}
		pgid, err := syscall.Getpgid(rec.PID)
		if err != nil {
			if settlementErr == nil {
				settlementErr = fmt.Errorf("supervise: revalidate worker process group: %w", err)
			}
			if retry := waitSettlementRetry(waited); retry.exited {
				return stopResult{waitErr: retry.waitErr, err: errors.Join(settlementErr, ErrUnsettledGroup)}
			}
			continue
		}
		if pgid != rec.PID {
			if settlementErr == nil {
				settlementErr = fmt.Errorf("supervise: worker pid %d moved to process group %d", rec.PID, pgid)
			}
			if retry := waitSettlementRetry(waited); retry.exited {
				return stopResult{waitErr: retry.waitErr, err: errors.Join(settlementErr, ErrUnsettledGroup)}
			}
			continue
		}
		killErr := p.signal(-rec.PID, syscall.SIGKILL)
		if errors.Is(killErr, syscall.ESRCH) {
			killErr = nil
		}
		if killErr != nil {
			if settlementErr == nil {
				settlementErr = wrapSignalError("kill worker group", killErr)
			}
			if retry := waitSettlementRetry(waited); retry.exited {
				return stopResult{waitErr: retry.waitErr, err: errors.Join(settlementErr, ErrUnsettledGroup)}
			}
			continue
		}
		return stopResult{waitErr: <-waited, err: errors.Join(
			wrapSignalError("terminate worker group", termErr),
			settlementErr,
		), settled: true}
	}
}

type retryResult struct {
	waitErr error
	exited  bool
}

func waitSettlementRetry(waited <-chan error) retryResult {
	timer := time.NewTimer(settlementRetry)
	defer timer.Stop()
	select {
	case waitErr := <-waited:
		return retryResult{waitErr: waitErr, exited: true}
	case <-timer.C:
		return retryResult{}
	}
}

func (p *Pool) finish(ctx context.Context, rec proc.Record, runErr error, settled bool) error {
	if !settled {
		return errors.Join(runErr, ErrUnsettledGroup)
	}
	if err := p.registry.Untrack(context.WithoutCancel(ctx), rec); err != nil {
		return errors.Join(runErr, fmt.Errorf("supervise: untrack worker: %w", err))
	}
	return runErr
}

func awaitWrapperReady(ctx context.Context, ready *os.File) error {
	result := make(chan error, 1)
	go func() {
		var marker [1]byte
		_, err := io.ReadFull(ready, marker[:])
		if err == nil && marker[0] != 'r' {
			err = fmt.Errorf("unexpected marker %q", marker[0])
		}
		result <- err
	}()
	select {
	case err := <-result:
		_ = ready.Close()
		return err
	case <-ctx.Done():
		_ = ready.Close()
		<-result
		return ctx.Err()
	}
}

func (p *Pool) killUntrackedGroup(pid int) error {
	var settlementErr error
	for {
		err := p.signal(-pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			err = nil
		}
		if err == nil {
			return settlementErr
		}
		settlementErr = errors.Join(settlementErr, wrapSignalError("kill untracked worker group", err))
		time.Sleep(settlementRetry)
	}
}

type workerResult struct {
	code int
	err  error
}

func readWorkerStatus(status *os.File) <-chan workerResult {
	result := make(chan workerResult, 1)
	go func() {
		defer status.Close()
		var code int
		_, err := fmt.Fscan(status, &code)
		if err == nil && (code < 0 || code > 255) {
			err = fmt.Errorf("invalid exit status %d", code)
		}
		result <- workerResult{code: code, err: err}
	}()
	return result
}

func writePayload(stdin io.WriteCloser, payload []byte) error {
	_, writeErr := stdin.Write(payload)
	closeErr := stdin.Close()
	return errors.Join(writeErr, closeErr)
}

func wrapWaitError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("supervise: worker exit: %w", err)
}

func workerExitError(result workerResult) error {
	if result.err != nil {
		return wrapStatusReadError(result.err)
	}
	if result.code == 0 {
		return nil
	}
	return fmt.Errorf("supervise: worker exit: %w", &ExitError{Code: result.code})
}

func wrapStatusReadError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("supervise: read worker exit status: %w", err)
}

func wrapSignalError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("supervise: %s: %w", action, err)
}

func unexpectedWaitError(err error) error {
	if err == nil {
		return errors.New("supervise: terminated worker exited successfully")
	}
	return nil
}
