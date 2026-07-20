package supervise

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/yasyf/daemonkit/proc"
)

const (
	// TerminalChunkSize is the largest input or output chunk retained in memory.
	TerminalChunkSize = 32 * 1024
	// TerminalQueueDepth bounds each direction of a terminal session.
	TerminalQueueDepth = 32
	// DefaultTerminalAttachTimeout bounds the durable but undispatched state.
	DefaultTerminalAttachTimeout = 5 * time.Second

	defaultTerminalRows uint16 = 24
	defaultTerminalCols uint16 = 80
)

var (
	// ErrTerminalAttached means a terminal already has a live attachment.
	ErrTerminalAttached = errors.New("terminal already attached")
	// ErrTerminalDetached means an operation targeted a closed attachment.
	ErrTerminalDetached = errors.New("terminal attachment is closed")
	// ErrTerminalInputClosed means EOF has already closed terminal input.
	ErrTerminalInputClosed = errors.New("terminal input is closed")
)

const terminalWrapper = `
printf r >&3
exec 3>&-
while ! IFS= read -r _ <&4; do
    :
done
exec 4<&-
exec "$@"
`

// TerminalSize is a PTY's character-cell geometry.
type TerminalSize struct {
	Rows uint16
	Cols uint16
}

func (s TerminalSize) normalized() TerminalSize {
	if s.Rows == 0 {
		s.Rows = defaultTerminalRows
	}
	if s.Cols == 0 {
		s.Cols = defaultTerminalCols
	}
	return s
}

// TerminalSpec describes one durably tracked interactive task.
type TerminalSpec struct {
	RecoveryClass proc.RecoveryClass
	Path          string
	Args          []string
	Dir           string
	Env           []string
	Size          TerminalSize
	AttachTimeout time.Duration
}

// TerminalInputKind identifies one ordered client-to-PTY event.
type TerminalInputKind uint8

const (
	// TerminalInputBytes writes Data to the PTY.
	TerminalInputBytes TerminalInputKind = iota + 1
	// TerminalInputResize applies Size to the PTY.
	TerminalInputResize
	// TerminalInputEOF closes input with the terminal EOF character.
	TerminalInputEOF
)

// TerminalInput is one bounded, ordered terminal input event.
type TerminalInput struct {
	Kind TerminalInputKind
	Data []byte
	Size TerminalSize
}

// TerminalDisconnectPolicy controls what closing an attachment does to its task.
type TerminalDisconnectPolicy uint8

const (
	// CancelOnDisconnect terminates and settles the task on attachment loss.
	CancelOnDisconnect TerminalDisconnectPolicy = iota + 1
	// DetachOnDisconnect leaves the bounded session available for reattachment.
	DetachOnDisconnect
)

// TerminalOutcomeKind identifies how the interactive task ended.
type TerminalOutcomeKind uint8

const (
	// TerminalExited reports an ordinary process exit code.
	TerminalExited TerminalOutcomeKind = iota + 1
	// TerminalSignaled reports a process terminated by a signal.
	TerminalSignaled
	// TerminalCanceled reports daemon-requested process-group termination.
	TerminalCanceled
)

// TerminalOutcome is the non-secret, typed settlement result.
type TerminalOutcome struct {
	Kind     TerminalOutcomeKind
	ExitCode int
	Signal   syscall.Signal
	Record   proc.Record
	Digest   [32]byte
}

// Terminal is one durably tracked PTY task. It supports one attachment at a
// time and retains only bounded in-memory I/O between attachments.
type Terminal struct {
	pool     *Pool
	record   proc.Record
	workerID uint64
	cancel   context.CancelFunc
	master   *os.File
	gate     *os.File
	write    func([]byte) error

	input   chan TerminalInput
	output  chan []byte
	ioErr   chan error
	ioStop  chan struct{}
	inDone  chan struct{}
	outDone chan struct{}
	done    chan struct{}

	gateOnce      sync.Once
	sendMu        sync.Mutex
	mu            sync.Mutex
	nextID        uint64
	activeID      uint64
	inputEOF      bool
	result        TerminalOutcome
	resultErr     error
	attachExpired bool
	attachTimer   *time.Timer
}

// TerminalAttachment is an exclusive view of a terminal's bounded I/O queues.
type TerminalAttachment struct {
	terminal *Terminal
	id       uint64
	policy   TerminalDisconnectPolicy
	stopCtx  func() bool
	once     sync.Once
	closed   chan struct{}
}

// StartTerminal creates a tracked PTY task behind a dispatch gate. Attach must
// claim it before AttachTimeout or the task is canceled and reaped.
func (p *Pool) StartTerminal(startup context.Context, spec TerminalSpec) (*Terminal, error) {
	if err := spec.RecoveryClass.Validate(); err != nil {
		return nil, fmt.Errorf("supervise: terminal recovery class: %w", err)
	}
	if spec.Path == "" {
		return nil, errors.New("supervise: terminal path is required")
	}
	attachTimeout := spec.AttachTimeout
	if attachTimeout <= 0 {
		attachTimeout = DefaultTerminalAttachTimeout
	}
	lifetime := context.WithoutCancel(startup)
	terminalCtx, workerID, cancel, err := p.acquire(startup, lifetime)
	if err != nil {
		return nil, err
	}
	cleanupSlot := true
	defer func() {
		if cleanupSlot {
			cancel()
			p.release(workerID)
		}
	}()

	args := make([]string, 0, len(spec.Args)+4)
	args = append(args, "-c", terminalWrapper, "daemonkit-terminal", spec.Path)
	args = append(args, spec.Args...)
	cmd := exec.Command("/bin/sh", args...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("supervise: terminal readiness pipe: %w", err)
	}
	gateR, gateW, err := os.Pipe()
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		return nil, fmt.Errorf("supervise: terminal dispatch gate: %w", err)
	}
	cmd.ExtraFiles = []*os.File{readyW, gateR}
	size := spec.Size.normalized()
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: size.Rows, Cols: size.Cols})
	if err != nil {
		_ = readyR.Close()
		_ = readyW.Close()
		_ = gateR.Close()
		_ = gateW.Close()
		return nil, fmt.Errorf("supervise: start terminal: %w", err)
	}
	_ = readyW.Close()
	_ = gateR.Close()
	if err := awaitWrapperReady(startup, readyR); err != nil {
		_ = gateW.Close()
		_ = master.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(fmt.Errorf("supervise: await terminal readiness: %w", err), killErr, unexpectedWaitError(waitErr))
	}
	record, err := p.registry.TrackGroup(startup, cmd.Process.Pid, spec.RecoveryClass)
	if err != nil {
		_ = gateW.Close()
		_ = master.Close()
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(fmt.Errorf("supervise: track terminal: %w", err), killErr, unexpectedWaitError(waitErr))
	}
	if recordErr := record.Validate(); recordErr != nil || record.PID != cmd.Process.Pid || !record.ProcessGroup || record.SessionID != record.PID {
		_ = gateW.Close()
		_ = master.Close()
		untrackErr := p.registry.Untrack(context.WithoutCancel(startup), record)
		killErr := p.killUntrackedGroup(cmd.Process.Pid)
		waitErr := cmd.Wait()
		return nil, errors.Join(errors.New("supervise: registry returned an invalid terminal process record"), recordErr, untrackErr, killErr, unexpectedWaitError(waitErr))
	}

	t := &Terminal{
		pool: p, record: record, workerID: workerID, cancel: cancel, master: master, gate: gateW,
		input: make(chan TerminalInput, TerminalQueueDepth), output: make(chan []byte, TerminalQueueDepth), ioErr: make(chan error, 1),
		ioStop: make(chan struct{}), inDone: make(chan struct{}), outDone: make(chan struct{}), done: make(chan struct{}),
	}
	t.write = func(data []byte) error { return writeTerminal(master, data) }
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	t.attachTimer = time.AfterFunc(attachTimeout, t.expireAttach)
	go t.pumpInput()
	go t.pumpOutput()
	go t.run(terminalCtx, waited)
	cleanupSlot = false
	return t, nil
}

// Record returns the immutable process-group identity recorded before dispatch.
func (t *Terminal) Record() proc.Record { return t.record }

// Attach exclusively attaches to the session and dispatches it on first use.
func (t *Terminal) Attach(ctx context.Context, policy TerminalDisconnectPolicy) (*TerminalAttachment, error) {
	if policy != CancelOnDisconnect && policy != DetachOnDisconnect {
		return nil, errors.New("supervise: terminal disconnect policy is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("supervise: attach terminal: %w", err)
	}
	t.mu.Lock()
	if t.attachExpired {
		t.mu.Unlock()
		return nil, errors.New("supervise: terminal attach deadline expired")
	}
	if t.activeID != 0 {
		t.mu.Unlock()
		return nil, ErrTerminalAttached
	}
	select {
	case <-t.done:
		t.mu.Unlock()
		return nil, errors.New("supervise: terminal already settled")
	default:
	}
	t.nextID++
	id := t.nextID
	t.activeID = id
	if t.attachTimer != nil {
		t.attachTimer.Stop()
		t.attachTimer = nil
	}
	t.mu.Unlock()

	var gateErr error
	t.gateOnce.Do(func() {
		gateErr = writePayload(t.gate, []byte("start\n"))
	})
	if gateErr != nil {
		t.cancel()
		<-t.done
		return nil, errors.Join(fmt.Errorf("supervise: release terminal dispatch gate: %w", gateErr), t.resultErr)
	}
	a := &TerminalAttachment{terminal: t, id: id, policy: policy, closed: make(chan struct{})}
	a.stopCtx = context.AfterFunc(ctx, func() { _ = a.Close() })
	return a, nil
}

// Send enqueues one ordered input event with context-aware backpressure.
func (a *TerminalAttachment) Send(ctx context.Context, event TerminalInput) error {
	a.terminal.sendMu.Lock()
	defer a.terminal.sendMu.Unlock()
	if err := a.terminal.validateInput(a.id, event); err != nil {
		return err
	}
	select {
	case a.terminal.input <- cloneTerminalInput(event):
		if event.Kind == TerminalInputEOF {
			a.terminal.markInputEOF()
		}
		return nil
	case <-a.closed:
		return ErrTerminalDetached
	case <-a.terminal.done:
		return errors.New("supervise: terminal settled")
	case <-ctx.Done():
		return fmt.Errorf("supervise: send terminal input: %w", ctx.Err())
	}
}

// Receive returns the next raw merged PTY output chunk.
func (a *TerminalAttachment) Receive(ctx context.Context) ([]byte, error) {
	if !a.terminal.attached(a.id) {
		return nil, ErrTerminalDetached
	}
	select {
	case chunk, ok := <-a.terminal.output:
		if !ok {
			return nil, io.EOF
		}
		return chunk, nil
	case <-a.closed:
		return nil, ErrTerminalDetached
	case <-ctx.Done():
		return nil, fmt.Errorf("supervise: receive terminal output: %w", ctx.Err())
	}
}

// Close applies the attachment's explicit disconnect policy.
func (a *TerminalAttachment) Close() error {
	a.once.Do(func() {
		close(a.closed)
		if a.stopCtx != nil {
			a.stopCtx()
		}
		a.terminal.detach(a.id, a.policy)
	})
	return nil
}

// Cancel terminates, reaps, and durably untracks the terminal task.
func (t *Terminal) Cancel(ctx context.Context) error {
	t.cancel()
	done := ctx.Done()
	var ctxErr error
	for {
		select {
		case <-t.done:
			t.mu.Lock()
			resultErr := t.resultErr
			t.mu.Unlock()
			return errors.Join(resultErr, ctxErr)
		case <-done:
			ctxErr = fmt.Errorf("supervise: cancel terminal: %w", ctx.Err())
			done = nil
		}
	}
}

// Wait returns the typed result after complete PTY and process settlement.
func (t *Terminal) Wait(ctx context.Context) (TerminalOutcome, error) {
	select {
	case <-t.done:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.result, t.resultErr
	case <-ctx.Done():
		return TerminalOutcome{}, fmt.Errorf("supervise: wait for terminal: %w", ctx.Err())
	}
}

func (t *Terminal) validateInput(id uint64, event TerminalInput) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeID != id {
		return ErrTerminalDetached
	}
	switch event.Kind {
	case TerminalInputBytes:
		if t.inputEOF {
			return ErrTerminalInputClosed
		}
		if len(event.Data) == 0 || len(event.Data) > TerminalChunkSize {
			return fmt.Errorf("supervise: terminal input must contain 1..%d bytes", TerminalChunkSize)
		}
	case TerminalInputResize:
		size := event.Size.normalized()
		if size != event.Size {
			return errors.New("supervise: terminal resize requires non-zero rows and columns")
		}
	case TerminalInputEOF:
		if t.inputEOF {
			return ErrTerminalInputClosed
		}
	default:
		return errors.New("supervise: unknown terminal input event")
	}
	return nil
}

func (t *Terminal) markInputEOF() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inputEOF = true
}

func (t *Terminal) expireAttach() {
	t.mu.Lock()
	if t.activeID != 0 || t.attachExpired {
		t.mu.Unlock()
		return
	}
	t.attachExpired = true
	t.mu.Unlock()
	t.cancel()
}

func (t *Terminal) attached(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activeID == id
}

func (t *Terminal) detach(id uint64, policy TerminalDisconnectPolicy) {
	t.mu.Lock()
	owned := false
	if t.activeID == id {
		t.activeID = 0
		owned = true
	}
	t.mu.Unlock()
	if owned && policy == CancelOnDisconnect {
		t.cancel()
	}
}

func (t *Terminal) run(ctx context.Context, waited <-chan error) {
	defer close(t.done)
	defer t.pool.release(t.workerID)
	defer t.cancel()
	var outcome TerminalOutcome
	var resultErr error
	canceled := false
	ioAborted := false
	select {
	case waitErr := <-waited:
		outcome = terminalOutcome(t.record, waitErr, false)
		resultErr = t.pool.registry.TerminateWithin(context.WithoutCancel(ctx), t.record, TerminationGrace)
		var exitErr *exec.ExitError
		if waitErr != nil && !errors.As(waitErr, &exitErr) {
			resultErr = errors.Join(resultErr, fmt.Errorf("supervise: wait for terminal process: %w", waitErr))
		}
	case <-ctx.Done():
		canceled = true
		t.gateOnce.Do(func() { _ = t.gate.Close() })
		stop := t.pool.stopAfterTerm(t.record, waited, func() {
			close(t.ioStop)
			_ = t.master.Close()
			ioAborted = true
		})
		outcome = terminalOutcome(t.record, stop.waitErr, true)
		resultErr = t.pool.registry.TerminateWithin(context.WithoutCancel(ctx), t.record, TerminationGrace)
		if resultErr != nil {
			resultErr = errors.Join(resultErr, stop.err, ErrUnsettledGroup)
		}
		select {
		case ioErr := <-t.ioErr:
			resultErr = errors.Join(resultErr, ioErr)
		default:
		}
	}
	_ = t.gate.Close()
	if !ioAborted && (canceled || resultErr != nil) {
		close(t.ioStop)
		_ = t.master.Close()
	}
	<-t.outDone
	if !canceled && resultErr == nil {
		close(t.ioStop)
	}
	_ = t.master.Close()
	<-t.inDone
	close(t.output)
	outcome.Digest = terminalOutcomeDigest(outcome)
	t.mu.Lock()
	t.result = outcome
	t.resultErr = resultErr
	t.activeID = 0
	t.mu.Unlock()
}

func (t *Terminal) pumpInput() {
	defer close(t.inDone)
	for {
		select {
		case <-t.ioStop:
			return
		case event := <-t.input:
			switch event.Kind {
			case TerminalInputBytes:
				if err := t.write(event.Data); err != nil {
					t.failIO(fmt.Errorf("supervise: write terminal input: %w", err))
					return
				}
			case TerminalInputResize:
				if err := pty.Setsize(t.master, &pty.Winsize{Rows: event.Size.Rows, Cols: event.Size.Cols}); err != nil {
					t.failIO(fmt.Errorf("supervise: resize terminal: %w", err))
					return
				}
			case TerminalInputEOF:
				if err := t.write([]byte{4}); err != nil {
					t.failIO(fmt.Errorf("supervise: close terminal input: %w", err))
					return
				}
			}
		}
	}
}

func (t *Terminal) failIO(err error) {
	select {
	case <-t.ioStop:
		return
	default:
	}
	select {
	case t.ioErr <- err:
	default:
	}
	t.cancel()
}

func writeTerminal(master *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := master.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (t *Terminal) pumpOutput() {
	defer close(t.outDone)
	buf := make([]byte, TerminalChunkSize)
	for {
		n, err := t.master.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			select {
			case t.output <- chunk:
			case <-t.ioStop:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func terminalOutcome(record proc.Record, err error, canceled bool) TerminalOutcome {
	out := TerminalOutcome{Record: record, ExitCode: -1}
	if canceled {
		out.Kind = TerminalCanceled
		return out
	}
	if err == nil {
		out.Kind = TerminalExited
		out.ExitCode = 0
		return out
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			out.Kind = TerminalSignaled
			out.Signal = status.Signal()
			return out
		}
		out.Kind = TerminalExited
		out.ExitCode = exitErr.ExitCode()
		return out
	}
	return out
}

func terminalOutcomeDigest(out TerminalOutcome) [32]byte {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d\x00%d\x00%d\x00%d\x00%s", out.Kind, out.ExitCode, out.Signal, out.Record.PID, out.Record.Generation)
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func cloneTerminalInput(event TerminalInput) TerminalInput {
	clone := event
	clone.Data = append([]byte(nil), event.Data...)
	return clone
}
