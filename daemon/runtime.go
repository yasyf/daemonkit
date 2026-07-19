package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// DefaultShutdownTimeout bounds graceful runtime shutdown when no timeout is configured.
const DefaultShutdownTimeout = 30 * time.Second

var errRuntimeExitSelf = errors.New("daemon: same-or-newer runtime already serving")

// Admission tracks admitted frames through their terminal responses.
type Admission interface {
	Admit() (done func(), err error)
	Close()
	Draining() bool
	Settle(ctx context.Context) error
}

// SessionServer serves persistent sessions over a Runtime-owned listener.
type SessionServer interface {
	Serve(ctx context.Context, listener net.Listener, admit func() (func(), error)) error
	CloseIntake() error
}

// Workers owns every disposable worker admitted by a Runtime. Wait must not
// return while an admitted worker or tracked process group remains alive; a
// context error is reported only after identity-safe settlement.
type Workers interface {
	Close()
	Cancel()
	Wait(ctx context.Context) error
}

// Resources owns the consumer resources released last during shutdown.
type Resources interface {
	Close() error
}

// RuntimeConfig defines one daemon process generation. Every ownership seam is
// required so a partial shutdown cannot silently omit a lifecycle phase.
type RuntimeConfig struct {
	Socket   string
	Build    string
	Protocol int

	Peer         Peer
	Contract     Contract
	WaitMode     WaitMode
	Grace        time.Duration
	WaitTimeout  time.Duration
	ListenerWait time.Duration
	Admission    Admission
	Server       SessionServer
	Workers      Workers
	State        io.Closer
	Resources    Resources

	// Handoff transfers external ownership during an upgrade. It is required
	// exactly for ResourceOwner and is never called for ordinary shutdown.
	Handoff func(ctx context.Context) error

	// Busy reports consumer work outside Admission and Workers.
	Busy func() bool
	// HealthState reports degraded or failed consumer health. nil is healthy.
	HealthState func() State

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal
}

type stopKind uint8

const (
	stopShutdown stopKind = iota + 1
	stopHandoff
)

// Runtime is the sole lifecycle coordinator for one daemon generation.
type Runtime struct {
	cfg RuntimeConfig

	mu       sync.Mutex
	started  bool
	finished bool
	runErr   error
	done     chan struct{}
	stop     chan stopKind
	stopOnce sync.Once
}

// NewRuntime validates cfg and constructs a single-use Runtime.
func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if err := validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	return &Runtime{
		cfg:  cfg,
		done: make(chan struct{}),
		stop: make(chan stopKind, 1),
	}, nil
}

func validateRuntimeConfig(cfg RuntimeConfig) error {
	switch {
	case cfg.Socket == "":
		return errors.New("daemon: runtime socket is required")
	case cfg.Build == "":
		return errors.New("daemon: runtime build is required")
	case cfg.Protocol <= 0:
		return errors.New("daemon: runtime protocol must be positive")
	case cfg.Peer == nil:
		return errors.New("daemon: runtime peer is required")
	case cfg.Admission == nil:
		return errors.New("daemon: runtime admission is required")
	case cfg.Server == nil:
		return errors.New("daemon: runtime session server is required")
	case cfg.Workers == nil:
		return errors.New("daemon: runtime workers are required")
	case cfg.State == nil:
		return errors.New("daemon: runtime state is required")
	case cfg.Resources == nil:
		return errors.New("daemon: runtime resources are required")
	}
	switch cfg.Contract {
	case RequestDaemon:
		if cfg.Handoff != nil {
			return errors.New("daemon: request runtime must not configure resource handoff")
		}
	case ResourceOwner:
		if cfg.Handoff == nil {
			return errors.New("daemon: resource-owner runtime requires handoff")
		}
	default:
		return fmt.Errorf("%w: %d", ErrUnknownContract, cfg.Contract)
	}
	switch cfg.WaitMode {
	case SocketRelease, PIDExit:
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnknownWaitMode, cfg.WaitMode)
	}
}

// Health returns the exact build/protocol and current lifecycle state.
func (r *Runtime) Health(ctx context.Context) (Health, error) {
	if err := ctx.Err(); err != nil {
		return Health{}, err
	}
	state := StateHealthy
	if r.cfg.HealthState != nil {
		state = r.cfg.HealthState()
	}
	busy := false
	if r.cfg.Busy != nil {
		busy = r.cfg.Busy()
	}
	return Health{
		Build:    r.cfg.Build,
		Protocol: r.cfg.Protocol,
		PID:      os.Getpid(),
		State:    state,
		Draining: r.cfg.Admission.Draining(),
		Busy:     busy,
	}, nil
}

// Shutdown requests an ordinary graceful shutdown and returns before teardown.
func (r *Runtime) Shutdown(ctx context.Context) error {
	return r.requestStop(ctx, stopShutdown)
}

// Handoff requests an upgrade handoff and returns before teardown.
func (r *Runtime) Handoff(ctx context.Context) error {
	return r.requestStop(ctx, stopHandoff)
}

// Close requests shutdown and waits for Run to finish. Repeated calls return
// the same terminal result.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return ErrRuntimeNotRunning
	}
	if r.finished {
		err := r.runErr
		r.mu.Unlock()
		return err
	}
	done := r.done
	r.mu.Unlock()
	if err := r.requestStop(ctx, stopShutdown); err != nil && !errors.Is(err, ErrRuntimeClosed) {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.runErr
	}
}

// Run acquires the single-entrant listener, serves sessions, and owns every
// shutdown phase through final resource release. A Runtime is single-use.
func (r *Runtime) Run(ctx context.Context) (err error) {
	if !r.begin() {
		return ErrRuntimeStarted
	}
	defer func() { r.finish(err) }()

	listener, lock, exitSelf, listenErr := r.listen(ctx)
	if listenErr != nil || exitSelf {
		closeErr := r.closeUnstarted(ctx)
		if exitSelf {
			return closeErr
		}
		return errors.Join(listenErr, closeErr)
	}

	serveCtx, cancelServe := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- r.cfg.Server.Serve(serveCtx, listener, r.cfg.Admission.Admit) }()

	signalCh, stopSignals := r.signalChannel()
	defer stopSignals()

	var kind stopKind
	var cause error
	var servedEarly bool
	var serveErr error
	select {
	case kind = <-r.stop:
	case <-ctx.Done():
		kind = stopShutdown
		cause = ctx.Err()
	case <-signalCh:
		kind = stopShutdown
	case serveErr = <-serveDone:
		kind = stopShutdown
		servedEarly = true
		if serveErr == nil {
			cause = ErrSessionServerStopped
		} else {
			cause = fmt.Errorf("daemon: serve sessions: %w", serveErr)
		}
	}

	shutdownErr := r.shutdown(ctx, kind, listener, lock, cancelServe, serveDone, servedEarly)
	return errors.Join(cause, shutdownErr)
}

func (r *Runtime) begin() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return false
	}
	r.started = true
	return true
}

func (r *Runtime) finish(err error) {
	r.mu.Lock()
	r.runErr = err
	r.finished = true
	close(r.done)
	r.mu.Unlock()
}

func (r *Runtime) requestStop(ctx context.Context, kind stopKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	started, finished := r.started, r.finished
	r.mu.Unlock()
	if !started {
		return ErrRuntimeNotRunning
	}
	if finished {
		return ErrRuntimeClosed
	}
	r.stopOnce.Do(func() { r.stop <- kind })
	return nil
}

func (r *Runtime) listen(ctx context.Context) (net.Listener, *os.File, bool, error) {
	exitSelf := false
	entrant := proc.SingleEntrant{
		Socket:  r.cfg.Socket,
		Timeout: r.cfg.ListenerWait,
		Evict: func() (bool, error) {
			outcome, err := Run(ctx, TakeoverConfig{
				Self:        r.cfg.Build,
				Protocol:    r.cfg.Protocol,
				Peer:        r.cfg.Peer,
				Contract:    r.cfg.Contract,
				WaitMode:    r.cfg.WaitMode,
				Grace:       r.cfg.Grace,
				WaitTimeout: r.cfg.WaitTimeout,
			})
			if err != nil {
				return false, err
			}
			switch outcome {
			case ExitSelf:
				exitSelf = true
				return false, errRuntimeExitSelf
			case Bind:
				return true, nil
			default:
				return false, fmt.Errorf("daemon: unknown takeover outcome %d", outcome)
			}
		},
	}
	listener, lock, err := entrant.Listen(ctx)
	if exitSelf && errors.Is(err, errRuntimeExitSelf) {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("daemon: acquire listener: %w", err)
	}
	return listener, lock, false, nil
}

func (r *Runtime) shutdown(
	parent context.Context,
	kind stopKind,
	listener net.Listener,
	lock *os.File,
	cancelServe context.CancelFunc,
	serveDone <-chan error,
	servedEarly bool,
) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.shutdownTimeout())
	defer cancel()

	var errs []error
	r.cfg.Admission.Close()
	if err := r.cfg.Server.CloseIntake(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("daemon: close intake: %w", err))
	}
	if err := r.cfg.Admission.Settle(ctx); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle admission: %w", err))
	}
	r.cfg.Workers.Close()
	r.cfg.Workers.Cancel()
	if err := r.cfg.Workers.Wait(ctx); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle workers: %w", err))
	}
	if kind == stopHandoff && r.cfg.Contract == ResourceOwner && len(errs) == 0 {
		if err := r.cfg.Handoff(ctx); err != nil {
			errs = append(errs, fmt.Errorf("daemon: handoff resources: %w", err))
		}
	}

	cancelServe()
	if !servedEarly {
		if err := <-serveDone; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, fmt.Errorf("daemon: join session server: %w", err))
		}
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("daemon: close listener: %w", err))
	}
	if err := lock.Close(); err != nil {
		errs = append(errs, fmt.Errorf("daemon: close listener lock: %w", err))
	}
	if err := r.cfg.State.Close(); err != nil {
		errs = append(errs, fmt.Errorf("daemon: close state: %w", err))
	}
	if err := r.cfg.Resources.Close(); err != nil {
		errs = append(errs, fmt.Errorf("daemon: close resources: %w", err))
	}
	return errors.Join(errs...)
}

func (r *Runtime) closeUnstarted(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.shutdownTimeout())
	defer cancel()
	r.cfg.Workers.Close()
	r.cfg.Workers.Cancel()
	workerErr := r.cfg.Workers.Wait(ctx)
	stateErr := r.cfg.State.Close()
	resourceErr := r.cfg.Resources.Close()
	return errors.Join(
		wrapRuntimeClose("settle workers", workerErr),
		wrapRuntimeClose("close state", stateErr),
		wrapRuntimeClose("close resources", resourceErr),
	)
}

func wrapRuntimeClose(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("daemon: %s: %w", action, err)
}

func (r *Runtime) shutdownTimeout() time.Duration {
	if r.cfg.ShutdownTimeout > 0 {
		return r.cfg.ShutdownTimeout
	}
	return DefaultShutdownTimeout
}

func (r *Runtime) signalChannel() (<-chan os.Signal, func()) {
	if r.cfg.Signals != nil {
		return r.cfg.Signals, func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}
