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

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/proc"
)

// DefaultShutdownTimeout bounds graceful runtime shutdown when no timeout is configured.
const DefaultShutdownTimeout = 30 * time.Second

var errRuntimeExitSelf = errors.New("daemon: same-or-newer runtime already serving")

// Admission tracks admitted frames through their terminal responses.
type Admission interface {
	Admit() (done func(), err error)
	AdmitProtected() (done func(), err error)
	Close()
	Draining() bool
	Settle(ctx context.Context) error
}

// SessionServer serves persistent sessions over a Runtime-owned listener.
type SessionServer interface {
	Serve(
		ctx context.Context,
		listener net.Listener,
		ready func() error,
		admit, admitProtected func() (func(), error),
	) error
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

// Activation separates bounded startup work from the runtime-owned resource
// lifetime. Startup is canceled when Activate returns. Lifetime remains live
// after successful activation until ordered shutdown reaches resource teardown.
type Activation struct {
	Startup  context.Context
	Lifetime context.Context
}

// RuntimeConfig defines one daemon process generation. Every ownership seam is
// required so a partial shutdown cannot silently omit an owned phase.
type RuntimeConfig struct {
	Socket          string
	RuntimeBuild    string
	RuntimeProtocol int

	ListenerWait time.Duration
	Admission    Admission
	Server       SessionServer
	Workers      Workers
	State        io.Closer
	Resources    Resources
	// Activate constructs and publishes the generation's owned state only after
	// listener acquisition has established exclusive ownership.
	// It must use Startup only for bounded construction and readiness, and
	// Lifetime for resources retained after a successful return.
	Activate func(Activation) error

	// Busy reports consumer work outside Admission and Workers.
	Busy func() bool
	// HealthState reports degraded or failed consumer health. nil is healthy.
	HealthState func() State

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal
}

// Runtime is the sole process coordinator for one daemon generation.
type Runtime struct {
	cfg               RuntimeConfig
	processGeneration string
	mu                sync.Mutex
	started           bool
	finished          bool
	stopping          bool
	runErr            error
	done              chan struct{}
	ready             chan struct{}
	isReady           bool
	stop              chan struct{}
	stopOnce          sync.Once
}

func init() {
	runtimeauth.Register(func(config any) (any, error) {
		cfg, ok := config.(RuntimeConfig)
		if !ok {
			return nil, errors.New("daemon: invalid private runtime config")
		}
		return newRuntime(cfg)
	})
}

func newRuntime(cfg RuntimeConfig) (*Runtime, error) {
	if err := validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return nil, fmt.Errorf("daemon: derive process generation: %w", err)
	}
	return &Runtime{
		cfg:               cfg,
		processGeneration: generation,
		done:              make(chan struct{}),
		ready:             make(chan struct{}),
		stop:              make(chan struct{}, 1),
	}, nil
}

func validateRuntimeConfig(cfg RuntimeConfig) error {
	switch {
	case cfg.Socket == "":
		return errors.New("daemon: runtime socket is required")
	case cfg.RuntimeBuild == "":
		return errors.New("daemon: runtime build is required")
	case cfg.RuntimeProtocol <= 0:
		return errors.New("daemon: runtime protocol must be positive")
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
	case cfg.Activate == nil:
		return errors.New("daemon: runtime activation is required")
	}
	return nil
}

// Health returns the exact build, protocol, and current runtime state.
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
	r.mu.Lock()
	stopping := r.stopping
	draining := stopping || r.cfg.Admission.Draining()
	ready := r.isReady && !stopping && !r.finished && !draining
	generation := r.processGeneration
	r.mu.Unlock()
	return Health{
		RuntimeBuild:      r.cfg.RuntimeBuild,
		RuntimeProtocol:   r.cfg.RuntimeProtocol,
		ProcessGeneration: generation,
		PID:               os.Getpid(),
		State:             state,
		Draining:          draining,
		Busy:              busy,
		Ready:             ready,
	}, nil
}

// Shutdown requests an ordinary graceful shutdown and returns before teardown.
func (r *Runtime) Shutdown(ctx context.Context) error {
	return r.requestStop(ctx)
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
	r.mu.Unlock()
	if err := r.requestStop(ctx); err != nil && !errors.Is(err, ErrRuntimeClosed) {
		return err
	}
	return r.Wait(ctx)
}

// WaitReady waits for exact healthy serving readiness. Session servers publish
// readiness once after every serving prerequisite is live; no polling occurs.
func (r *Runtime) WaitReady(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		finished, stopping, runErr, ready := r.finished, r.stopping, r.runErr, r.isReady
		r.mu.Unlock()
		if finished {
			return errors.Join(ErrRuntimeNotReady, runErr)
		}
		if stopping {
			return ErrRuntimeNotReady
		}
		if ready {
			health, err := r.Health(ctx)
			if err != nil {
				return err
			}
			if health.State != StateHealthy || health.Draining {
				return fmt.Errorf(
					"%w: state=%q draining=%t",
					ErrRuntimeNotReady,
					health.State,
					health.Draining,
				)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ready:
		case <-r.done:
		}
	}
}

// Wait joins Run and replays its terminal result to every waiter.
func (r *Runtime) Wait(ctx context.Context) error {
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return ErrRuntimeNotRunning
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
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

	signalCh, stopSignals := r.signalChannel()
	defer stopSignals()
	cancelActivation, activated, activationCause, activateErr := r.activate(ctx, signalCh)
	if !activated {
		closeErr := r.closeAcquired(ctx, listener, lock)
		return errors.Join(activationCause, activateErr, closeErr)
	}

	serveCtx, cancelServe := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServe()
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- r.cfg.Server.Serve(
			serveCtx,
			listener,
			r.publishReady,
			r.cfg.Admission.Admit,
			r.cfg.Admission.AdmitProtected,
		)
	}()

	var cause error
	var servedEarly bool
	var serveErr error
	select {
	case <-r.stop:
	case <-ctx.Done():
		cause = ctx.Err()
	case <-signalCh:
	case serveErr = <-serveDone:
		servedEarly = true
		if serveErr == nil {
			cause = ErrSessionServerStopped
		} else {
			cause = fmt.Errorf("daemon: serve sessions: %w", serveErr)
		}
	}

	shutdownErr := r.shutdown(
		ctx, listener, lock, cancelActivation, cancelServe, serveDone, servedEarly,
	)
	return errors.Join(cause, shutdownErr)
}

func (r *Runtime) activate(
	parent context.Context,
	signalCh <-chan os.Signal,
) (context.CancelFunc, bool, error, error) {
	startup, cancelStartup := context.WithCancel(parent)
	lifetime, cancelLifetime := context.WithCancel(context.WithoutCancel(parent))
	done := make(chan error, 1)
	go func() {
		done <- runActivation(Activation{Startup: startup, Lifetime: lifetime}, r.cfg.Activate)
	}()

	select {
	case err := <-done:
		cancelStartup()
		if err != nil {
			cancelLifetime()
			return nil, false, nil, fmt.Errorf("daemon: activate runtime: %w", err)
		}
		if err := parent.Err(); err != nil {
			cancelLifetime()
			return nil, false, err, nil
		}
		select {
		case <-r.stop:
			cancelLifetime()
			return nil, false, nil, nil
		default:
		}
		select {
		case <-signalCh:
			cancelLifetime()
			return nil, false, nil, nil
		default:
		}
		return cancelLifetime, true, nil, nil
	case <-r.stop:
		cancelStartup()
		cancelLifetime()
		return nil, false, nil, interruptedActivationError(<-done)
	case <-parent.Done():
		cancelStartup()
		cancelLifetime()
		return nil, false, parent.Err(), interruptedActivationError(<-done)
	case <-signalCh:
		cancelStartup()
		cancelLifetime()
		return nil, false, nil, interruptedActivationError(<-done)
	}
}

func runActivation(activation Activation, activate func(Activation) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("daemon: activation panic: %v", recovered)
		}
	}()
	return activate(activation)
}

func interruptedActivationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("daemon: settle interrupted activation: %w", err)
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

func (r *Runtime) publishReady() error {
	health, err := r.Health(context.Background())
	if err != nil {
		return err
	}
	if health.State != StateHealthy || health.Draining {
		return fmt.Errorf(
			"%w: state=%q draining=%t",
			ErrRuntimeNotReady,
			health.State,
			health.Draining,
		)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return ErrRuntimeNotReady
	}
	if r.isReady {
		return ErrRuntimeReady
	}
	r.isReady = true
	close(r.ready)
	return nil
}

func (r *Runtime) requestStop(ctx context.Context) error {
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
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopping = true
		r.mu.Unlock()
		r.stop <- struct{}{}
	})
	return nil
}

func (r *Runtime) listen(ctx context.Context) (net.Listener, *os.File, bool, error) {
	shouldExit := false
	entrant := proc.SingleEntrant{
		Socket:  r.cfg.Socket,
		Timeout: r.cfg.ListenerWait,
		Evict: func() (bool, error) {
			conn, err := net.DialTimeout("unix", r.cfg.Socket, 100*time.Millisecond)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
					return true, nil
				}
				return false, fmt.Errorf("daemon: probe incumbent listener: %w", err)
			}
			_ = conn.Close()
			shouldExit = true
			return false, errRuntimeExitSelf
		},
	}
	listener, lock, err := entrant.Listen(ctx)
	if shouldExit && errors.Is(err, errRuntimeExitSelf) {
		return nil, nil, true, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("daemon: acquire listener: %w", err)
	}
	return listener, lock, false, nil
}

func (r *Runtime) shutdown(
	parent context.Context,
	listener net.Listener,
	lock *os.File,
	cancelActivation context.CancelFunc,
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
	if err := settleWorkers(ctx, r.cfg.Workers); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle workers: %w", err))
	}
	cancelActivation()
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

func (r *Runtime) closeAcquired(parent context.Context, listener net.Listener, lock *os.File) error {
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
	if err := settleWorkers(ctx, r.cfg.Workers); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle workers: %w", err))
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
	workerErr := settleWorkers(ctx, r.cfg.Workers)
	stateErr := r.cfg.State.Close()
	resourceErr := r.cfg.Resources.Close()
	return errors.Join(
		wrapRuntimeClose("settle workers", workerErr),
		wrapRuntimeClose("close state", stateErr),
		wrapRuntimeClose("close resources", resourceErr),
	)
}

func settleWorkers(ctx context.Context, workers Workers) error {
	workers.Close()
	workers.Cancel()
	return workers.Wait(ctx)
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
