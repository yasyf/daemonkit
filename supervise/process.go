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

const processWrapper = `
trap ':' TERM
printf r >&3
exec 3>&-
while ! IFS= read -r _ <&4; do
    :
done
exec 4<&-
exec "$@"
`

// DefaultReadinessTimeout bounds a managed process's readiness callback.
const DefaultReadinessTimeout = 5 * time.Second

// ErrProcessStopped means a managed process was intentionally terminated.
var ErrProcessStopped = errors.New("managed process stopped")

// ProcessSpec describes one durably tracked, long-lived child process.
type ProcessSpec struct {
	// RecoveryClass names the exact recovery barrier for a crash receipt.
	RecoveryClass proc.RecoveryClass
	// Path is the child executable.
	Path string
	// Args are the child arguments after Path.
	Args []string
	// Dir is the child's working directory. Empty inherits the caller's.
	Dir string
	// Env is the child environment. Nil inherits the caller's environment.
	Env []string
	// Stdout and Stderr receive child output. Nil discards the corresponding stream.
	Stdout io.Writer
	Stderr io.Writer
	// Ready runs only after the process-group identity is durable and execution
	// has been released. A nil callback makes durable launch the readiness point.
	Ready func(context.Context, proc.Record) error
	// Recorded runs after the exact process-group identity is durable but before
	// the wrapper can execute Path. An error aborts and reaps the process.
	Recorded func(context.Context, proc.Record) error
	// ReadinessTimeout bounds Ready. A non-positive value uses DefaultReadinessTimeout.
	ReadinessTimeout time.Duration
}

// Process is a durably tracked long-lived child. Its Record is immutable.
type Process struct {
	record   proc.Record
	pool     *Pool
	workerID uint64
	cancel   context.CancelFunc
	done     chan struct{}

	mu         sync.Mutex
	result     error
	stopResult error
}

// Record returns the immutable process-group identity persisted before launch.
func (p *Process) Record() proc.Record { return p.record }

// Wait waits for the process to exit or ctx to expire.
func (p *Process) Wait(ctx context.Context) error {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.result
	case <-ctx.Done():
		return fmt.Errorf("supervise: wait for managed process: %w", ctx.Err())
	}
}

// Stop terminates, reaps, and durably untracks the process. A caller deadline
// is reported only after settlement completes.
func (p *Process) Stop(ctx context.Context) error {
	p.cancel()
	done := ctx.Done()
	var ctxErr error
	for {
		select {
		case <-p.done:
			p.mu.Lock()
			result := p.stopResult
			p.mu.Unlock()
			return errors.Join(result, ctxErr)
		case <-done:
			ctxErr = fmt.Errorf("supervise: stop managed process: %w", ctx.Err())
			done = nil
		}
	}
}

// Start launches a long-lived child in a dedicated process group. startup
// bounds launch and readiness only; after success, only Process.Stop or pool
// cancellation ends the process lifetime.
func (p *Pool) Start(startup context.Context, spec ProcessSpec) (*Process, error) {
	if err := spec.RecoveryClass.Validate(); err != nil {
		return nil, fmt.Errorf("supervise: managed process recovery class: %w", err)
	}
	if spec.Path == "" {
		return nil, errors.New("supervise: managed process path is required")
	}
	processCtx, workerID, cancel, err := p.acquire(startup, context.WithoutCancel(startup))
	if err != nil {
		return nil, err
	}
	startupCtx, cancelStartup := context.WithCancel(startup)
	stopStartup := context.AfterFunc(processCtx, cancelStartup)
	defer func() {
		stopStartup()
		cancelStartup()
	}()
	cleanupSlot := true
	defer func() {
		if cleanupSlot {
			cancel()
			p.release(workerID)
		}
	}()
	if err := startupCtx.Err(); err != nil {
		return nil, fmt.Errorf("supervise: managed process canceled before start: %w", err)
	}

	cmd, readyR, readyW, gateR, gateW, err := managedCommand(spec)
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		_ = gateR.Close()
		_ = gateW.Close()
		return nil, fmt.Errorf("supervise: start managed process: %w", err)
	}
	_ = readyW.Close()
	_ = gateR.Close()
	if err := awaitWrapperReady(startupCtx, readyR); err != nil {
		_ = gateW.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(
			fmt.Errorf("supervise: await managed wrapper: %w", err),
			killErr,
			unexpectedWaitError(waitErr),
		)
	}

	record, err := p.registry.TrackGroup(startupCtx, cmd.Process.Pid, spec.RecoveryClass)
	if err != nil {
		_ = gateW.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(
			fmt.Errorf("supervise: track managed process: %w", err),
			killErr,
			unexpectedWaitError(waitErr),
		)
	}
	recordErr := record.Validate()
	if recordErr != nil || record.PID != cmd.Process.Pid ||
		!record.ProcessGroup || record.SessionID != record.PID {
		_ = gateW.Close()
		untrackErr := p.registry.Untrack(context.WithoutCancel(startupCtx), record)
		if untrackErr != nil {
			untrackErr = fmt.Errorf("supervise: untrack invalid managed process record: %w", untrackErr)
		}
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(
			errors.New("supervise: registry returned an invalid managed process record"),
			recordErr,
			untrackErr,
			killErr,
			unexpectedWaitError(waitErr),
		)
	}
	if spec.Recorded != nil {
		if err := spec.Recorded(startupCtx, record); err != nil {
			_ = gateW.Close()
			untrackErr := p.registry.Untrack(context.WithoutCancel(startupCtx), record)
			if untrackErr != nil {
				untrackErr = fmt.Errorf("supervise: untrack rejected managed process record: %w", untrackErr)
			}
			killErr := p.killUntrackedGroup(cmd.Process.Pid)
			waitErr := cmd.Wait()
			return nil, errors.Join(
				fmt.Errorf("supervise: accept managed process record: %w", err),
				untrackErr,
				killErr,
				unexpectedWaitError(waitErr),
			)
		}
	}
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	process := &Process{
		record: record, pool: p, workerID: workerID, cancel: cancel, done: make(chan struct{}),
	}
	go process.run(processCtx, waited)
	cleanupSlot = false

	if err := writePayload(gateW, []byte("start\n")); err != nil {
		stopErr := process.Stop(context.WithoutCancel(startup))
		return nil, errors.Join(fmt.Errorf("supervise: release managed process gate: %w", err), stopErr)
	}
	if spec.Ready == nil {
		if err := startupCtx.Err(); err != nil {
			stopErr := process.Stop(context.WithoutCancel(startup))
			return nil, errors.Join(
				fmt.Errorf("supervise: managed process startup: %w", err),
				stopErr,
			)
		}
		return process, nil
	}
	if err := process.awaitReady(startupCtx, spec); err != nil {
		stopErr := process.Stop(context.WithoutCancel(startup))
		return nil, errors.Join(err, stopErr)
	}
	if err := startupCtx.Err(); err != nil {
		stopErr := process.Stop(context.WithoutCancel(startup))
		return nil, errors.Join(
			fmt.Errorf("supervise: managed process startup: %w", err),
			stopErr,
		)
	}
	return process, nil
}

func managedCommand(spec ProcessSpec) (*exec.Cmd, *os.File, *os.File, *os.File, *os.File, error) {
	wrapperArgs := make([]string, 0, len(spec.Args)+4)
	wrapperArgs = append(wrapperArgs, "-c", processWrapper, "daemonkit-process", spec.Path)
	wrapperArgs = append(wrapperArgs, spec.Args...)
	cmd := exec.Command("/bin/sh", wrapperArgs...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	cmd.Stdout = spec.Stdout
	cmd.Stderr = spec.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("supervise: managed readiness pipe: %w", err)
	}
	gateR, gateW, err := os.Pipe()
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("supervise: managed dispatch gate: %w", err)
	}
	cmd.ExtraFiles = []*os.File{readyW, gateR}
	return cmd, readyR, readyW, gateR, gateW, nil
}

func (p *Process) awaitReady(ctx context.Context, spec ProcessSpec) error {
	timeout := spec.ReadinessTimeout
	if timeout <= 0 {
		timeout = DefaultReadinessTimeout
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- spec.Ready(readyCtx, p.record) }()
	select {
	case err := <-ready:
		if err != nil {
			return fmt.Errorf("supervise: managed process readiness: %w", err)
		}
		select {
		case <-p.done:
			return errors.New("supervise: managed process exited during readiness")
		default:
			return nil
		}
	case <-p.done:
		return errors.New("supervise: managed process exited before readiness")
	case <-readyCtx.Done():
		return fmt.Errorf("supervise: managed process readiness: %w", readyCtx.Err())
	}
}

func (p *Process) run(ctx context.Context, waited <-chan error) {
	defer close(p.done)
	defer p.pool.release(p.workerID)
	defer p.cancel()
	select {
	case waitErr := <-waited:
		result := p.pool.finish(context.WithoutCancel(ctx), p.record, wrapWaitError(waitErr), true)
		p.complete(result, nil)
	case <-ctx.Done():
		stop := p.pool.stopManaged(p.record, waited)
		result := p.pool.finish(context.WithoutCancel(ctx), p.record, stop.err, stop.settled)
		p.complete(errors.Join(ErrProcessStopped, result), result)
	}
}

func (p *Process) complete(result, stopResult error) {
	p.mu.Lock()
	p.result = result
	p.stopResult = stopResult
	p.mu.Unlock()
}

func (p *Pool) stopManaged(record proc.Record, waited <-chan error) stopResult {
	owned, err := p.registry.Owns(record)
	if err != nil {
		return stopResult{waitErr: <-waited, err: fmt.Errorf("supervise: revalidate managed process: %w", err)}
	}
	if !owned {
		return stopResult{waitErr: <-waited, settled: true}
	}
	groupID, err := syscall.Getpgid(record.PID)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return stopResult{waitErr: <-waited, settled: true}
		}
		return stopResult{waitErr: <-waited, err: fmt.Errorf("supervise: revalidate managed process group: %w", err)}
	}
	if groupID != record.PID {
		return stopResult{waitErr: <-waited, err: fmt.Errorf(
			"supervise: managed pid %d moved to process group %d", record.PID, groupID,
		)}
	}
	termErr := p.signal(-record.PID, syscall.SIGTERM)
	if errors.Is(termErr, syscall.ESRCH) {
		termErr = nil
	}
	timer := time.NewTimer(p.grace)
	defer timer.Stop()
	select {
	case waitErr := <-waited:
		return stopResult{waitErr: waitErr, err: wrapSignalError("terminate managed process group", termErr), settled: true}
	case <-timer.C:
	}
	owned, err = p.registry.Owns(record)
	if err != nil {
		return stopResult{waitErr: <-waited, err: fmt.Errorf("supervise: revalidate managed process before SIGKILL: %w", err)}
	}
	if !owned {
		return stopResult{waitErr: <-waited, err: wrapSignalError("terminate managed process group", termErr), settled: true}
	}
	groupID, err = syscall.Getpgid(record.PID)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return stopResult{waitErr: <-waited, err: wrapSignalError("terminate managed process group", termErr), settled: true}
		}
		return stopResult{waitErr: <-waited, err: fmt.Errorf("supervise: revalidate managed process group before SIGKILL: %w", err)}
	}
	if groupID != record.PID {
		return stopResult{waitErr: <-waited, err: fmt.Errorf(
			"supervise: managed pid %d moved to process group %d before SIGKILL", record.PID, groupID,
		)}
	}
	killErr := p.signal(-record.PID, syscall.SIGKILL)
	if errors.Is(killErr, syscall.ESRCH) {
		killErr = nil
	}
	return stopResult{
		waitErr: <-waited,
		err: errors.Join(
			wrapSignalError("terminate managed process group", termErr),
			wrapSignalError("kill managed process group", killErr),
		),
		settled: killErr == nil,
	}
}
