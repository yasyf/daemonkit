package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	peeridentity "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

// DefaultShutdownTimeout bounds graceful runtime shutdown when no timeout is configured.
const DefaultShutdownTimeout = 30 * time.Second

const trustProbeTimeout = 10 * time.Second

// Activation is one generation-bound consumer preparation authority.
type Activation struct {
	runtime    *Runtime
	generation uint64
	ctx        context.Context
}

// RecoveryCapability is one unforgeable generation-bound recovery proof.
type RecoveryCapability struct {
	runtime    *Runtime
	generation uint64
	id         proc.RecoveryID
	receipt    proc.RecoveryReceipt
	state      *recoveryCapabilityState
}

type recoveryCapabilityState struct {
	consumed bool
}

// RuntimeConfig defines one daemon process generation. Every ownership seam is
// required so a partial shutdown cannot silently omit an owned phase.
type RuntimeConfig struct {
	Socket          string
	RuntimeBuild    string
	RuntimeProtocol int

	ListenerWait time.Duration
	Workers      *worker.Pool
	Children     *proc.Manager

	ShutdownTimeout time.Duration
	Signals         <-chan os.Signal
}

// Runtime is the sole process coordinator for one daemon generation.
type Runtime struct {
	cfg                  RuntimeConfig
	processGeneration    proc.OwnerGeneration
	mu                   sync.Mutex
	started              bool
	finished             bool
	stopping             bool
	runErr               error
	done                 chan struct{}
	lifecycle            *Lifecycle
	stop                 chan struct{}
	stopOnce             sync.Once
	controllerGeneration uint64
	publication          *publicationCore
	activationCancel     context.CancelFunc
	serveCancel          context.CancelFunc
	serverLive           bool
	serverTerminal       bool
	startupFailure       error
	productSettlement    *productSettlementState
	workerClaim          *worker.RuntimeClaim
	workerActivated      bool
	childrenClaimed      bool
	trustWorkers         *worker.RuntimeClaim
	server               runtimeauth.SessionServer
	retainedListener     net.Listener
	retainedLock         *proc.FileLockHandle
	childFences          map[childFenceKey]*childFenceState
	trustPolicy          trust.TrustPolicy
	trustExecutable      string
	childFenceTimeout    time.Duration
	childFenceVerifier   func(context.Context, peeridentity.Identity, trust.Requirement) error
	recoveryCapabilities map[proc.RecoveryID]*recoveryCapabilityState
}

func init() {
	runtimeauth.Register(func(config any) (any, error) {
		composition, ok := config.(runtimeauth.Composition)
		if !ok || composition.Server == nil {
			return nil, errors.New("daemon: invalid private runtime composition")
		}
		cfg, ok := composition.RuntimeConfig.(RuntimeConfig)
		if !ok {
			return nil, errors.New("daemon: invalid private runtime config")
		}
		policy, ok := composition.TrustPolicy.(trust.TrustPolicy)
		if !ok {
			return nil, errors.New("daemon: invalid private trust policy")
		}
		runtime, err := newRuntime(cfg, policy)
		if err != nil {
			return nil, err
		}
		runtime.server = composition.Server
		return runtime, nil
	})
}

func newRuntime(cfg RuntimeConfig, policy trust.TrustPolicy) (*Runtime, error) {
	if err := validateRuntimeConfig(cfg); err != nil {
		return nil, err
	}
	generation, err := proc.ProcessGeneration()
	if err != nil {
		return nil, fmt.Errorf("daemon: derive process generation: %w", err)
	}
	if cfg.Children.OwnerGeneration() != generation {
		return nil, errors.New("daemon: runtime process and process owners must share one generation")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("daemon: trust verifier executable: %w", err)
	}
	return &Runtime{
		cfg:                  cfg,
		processGeneration:    generation,
		done:                 make(chan struct{}),
		lifecycle:            newLifecycle(),
		stop:                 make(chan struct{}, 1),
		controllerGeneration: 1,
		childFences:          make(map[childFenceKey]*childFenceState),
		trustPolicy:          policy,
		trustExecutable:      executable,
		recoveryCapabilities: make(map[proc.RecoveryID]*recoveryCapabilityState),
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
	case cfg.Workers == nil:
		return errors.New("daemon: runtime workers are required")
	case cfg.Children == nil:
		return errors.New("daemon: runtime child manager is required")
	case cfg.Children.OwnerGeneration() == (proc.OwnerGeneration{}) || cfg.Workers.OwnerGeneration() == (proc.OwnerGeneration{}):
		return errors.New("daemon: runtime owner generation is required")
	case cfg.Children.OwnerGeneration() != cfg.Workers.OwnerGeneration():
		return errors.New("daemon: runtime children and workers must share one owner generation")
	}
	return nil
}

// Health returns the exact build, protocol, and current runtime state.
func (r *Runtime) Health(ctx context.Context) (Health, error) {
	if err := ctx.Err(); err != nil {
		return Health{}, err
	}
	r.mu.Lock()
	r.lifecycle.mu.Lock()
	progress := cloneLifecycleProgress(r.lifecycle.progress)
	healthStatus := HealthStatus{
		State: r.lifecycle.health.State, Detail: append([]byte{}, r.lifecycle.health.Detail...),
	}
	fatal := r.lifecycle.fatal
	if fatal != nil {
		healthStatus = HealthStatus{State: StateFailed, Detail: []byte(fatal.Error())}
	}
	productBusy := r.productSettlement != nil && !r.productSettlement.completed &&
		r.productSettlement.ctx.Err() != nil
	busy := r.lifecycle.inflight > 0 || len(r.lifecycle.activities) > 0 || productBusy ||
		r.cfg.Workers.Active() > 0 || r.cfg.Children.Active() > 0
	r.lifecycle.mu.Unlock()
	ready := progress.State == LifecycleReady && fatal == nil
	draining := progress.State == LifecycleDraining
	generation := r.processGeneration
	r.mu.Unlock()
	return Health{
		RuntimeBuild:      r.cfg.RuntimeBuild,
		RuntimeProtocol:   r.cfg.RuntimeProtocol,
		ProcessGeneration: generation,
		PID:               os.Getpid(),
		State:             healthStatus.State,
		Detail:            healthStatus.Detail,
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
	stopErr := r.requestStop(ctx)
	r.mu.Lock()
	stopIssued := r.stopping
	r.mu.Unlock()
	if stopErr != nil && !stopIssued {
		return stopErr
	}
	return errors.Join(stopErr, r.Wait(ctx))
}

// WaitReady waits for exact healthy serving readiness. Session servers publish
// readiness once after every serving prerequisite is live; no polling occurs.
func (r *Runtime) WaitReady(ctx context.Context) error {
	sequence := uint64(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if fatal := r.lifecycle.fatalError(); fatal != nil {
			return fatal
		}
		progress := r.lifecycle.Snapshot()
		sequence = progress.Sequence
		switch progress.State {
		case LifecycleReady:
			return nil
		case LifecycleFailed, LifecycleDraining:
			return ErrRuntimeNotReady
		}
		_, err := r.lifecycle.WaitChange(ctx, sequence)
		if err != nil {
			return err
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

// Begin acquires the single-entrant listener and starts authenticated sessions
// in Starting. The caller owns consumer preparation through the returned
// generation-bound Activation. The acquisition context has no authority after
// Begin returns.
//
//nolint:contextcheck // Serving and settlement lifetimes are Runtime-owned after acquisition.
func (r *Runtime) Begin(ctx context.Context) (activation Activation, err error) {
	if !r.begin() {
		return Activation{}, ErrRuntimeStarted
	}
	if claimErr := r.cfg.Children.ClaimRuntime(); claimErr != nil {
		r.finish(claimErr)
		return Activation{}, fmt.Errorf("daemon: claim runtime children: %w", claimErr)
	}
	r.mu.Lock()
	r.childrenClaimed = true
	r.mu.Unlock()
	if recoverErr := r.cfg.Children.Recover(ctx); recoverErr != nil {
		closeErr := r.closeUnstarted(ctx)
		err = errors.Join(fmt.Errorf("daemon: recover runtime children: %w", recoverErr), closeErr)
		r.finish(err)
		return Activation{}, err
	}
	claim, claimErr := r.cfg.Workers.ClaimRuntime(trust.VerifierWorkerBudgets())
	if claimErr != nil {
		_ = r.cfg.Children.ReleaseRuntime()
		r.mu.Lock()
		r.childrenClaimed = false
		r.mu.Unlock()
		r.finish(claimErr)
		return Activation{}, fmt.Errorf("daemon: claim runtime workers: %w", claimErr)
	}
	r.mu.Lock()
	r.workerClaim = claim
	r.trustWorkers = claim
	r.mu.Unlock()
	if recoverErr := claim.Recover(ctx); recoverErr != nil {
		closeErr := r.closeUnstarted(ctx)
		err = errors.Join(fmt.Errorf("daemon: recover runtime workers: %w", recoverErr), closeErr)
		r.finish(err)
		return Activation{}, err
	}
	r.mu.Lock()
	publicationBound := r.publication != nil
	r.mu.Unlock()
	if !publicationBound {
		err = errors.New("daemon: runtime publication slot is required")
		closeErr := r.closeUnstarted(ctx)
		err = errors.Join(err, closeErr)
		r.finish(err)
		return Activation{}, err
	}
	if r.lifecycle.Snapshot().State != LifecycleStarting {
		err = errors.New("daemon: runtime controller is not starting")
		closeErr := r.closeUnstarted(ctx)
		err = errors.Join(err, closeErr)
		r.finish(err)
		return Activation{}, err
	}

	listener, lock, exitSelf, listenErr := r.listen(ctx)
	if listenErr != nil || exitSelf {
		closeErr := r.closeUnstarted(ctx)
		err = errors.Join(listenErr, closeErr)
		r.finish(err)
		if exitSelf {
			return Activation{}, closeErr
		}
		return Activation{}, err
	}
	if activateErr := claim.Activate(); activateErr != nil {
		closeErr := r.closeAcquired(ctx, listener, lock)
		err = errors.Join(fmt.Errorf("daemon: activate runtime workers: %w", activateErr), closeErr)
		r.finish(err)
		return Activation{}, err
	}
	r.mu.Lock()
	r.workerActivated = true
	r.mu.Unlock()
	if probeErr := r.probeTrustVerifier(ctx); probeErr != nil {
		closeErr := r.closeAcquired(ctx, listener, lock)
		err = errors.Join(fmt.Errorf("%w: %w", ErrTrustVerifierProbe, probeErr), closeErr)
		r.finish(err)
		return Activation{}, err
	}

	signalCh, stopSignals := r.signalChannel()
	activationCtx, cancelActivation := context.WithCancel(context.WithoutCancel(ctx))
	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	serveStarted := make(chan error, 1)
	var serverExitOnce sync.Once
	var serverExitResult error
	serverExit := func(serveErr error) error {
		serverExitOnce.Do(func() { serverExitResult = r.markServerTerminal(serveErr) })
		return serverExitResult
	}
	go func() {
		serveErr := r.server.ServeRuntime(
			serveCtx,
			listener,
			r.lifecycle,
			r.trustWorkers,
			func() (any, func(), error) { return r.admitReady() },
			func() (any, func(), error) { return r.admitProtected() },
			r.matchChildFence,
			serverExit,
			serveStarted,
		)
		serveDone <- serverExit(serveErr)
	}()
	abort := func(cause error, joined bool) error {
		result := r.abortBegin(cause, listener, lock, cancelActivation, cancelServe, serveDone, joined)
		stopSignals()
		r.finish(result)
		return result
	}

	select {
	case <-ctx.Done():
		return Activation{}, abort(ctx.Err(), false)
	case <-r.stop:
		return Activation{}, abort(r.startupStopCause(), false)
	case <-signalCh:
		return Activation{}, abort(ErrRuntimeNotReady, false)
	case serveErr := <-serveDone:
		if serveErr == nil {
			serveErr = ErrSessionServerStopped
		}
		return Activation{}, abort(serveErr, true)
	case startErr := <-serveStarted:
		if startErr != nil {
			return Activation{}, abort(startErr, false)
		}
		r.mu.Lock()
		if !r.serverTerminal {
			r.serverLive = true
		}
		r.mu.Unlock()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return Activation{}, abort(ctxErr, false)
	}
	select {
	case <-r.stop:
		err = r.startupStopCause()
	case <-signalCh:
		err = ErrRuntimeNotReady
	case serveErr := <-serveDone:
		if serveErr == nil {
			serveErr = ErrSessionServerStopped
		}
		err = serveErr
	default:
	}
	if err != nil {
		return Activation{}, abort(err, false)
	}

	r.mu.Lock()
	r.lifecycle.mu.Lock()
	if r.stopping || r.finished || !r.serverLive || r.serverTerminal || r.lifecycle.progress.State != LifecycleStarting || ctx.Err() != nil {
		state := r.lifecycle.progress.State
		r.lifecycle.mu.Unlock()
		r.mu.Unlock()
		return Activation{}, abort(
			errors.Join(fmt.Errorf("daemon: Begin lost activation authority in state %s", state), ctx.Err()),
			false,
		)
	}
	r.activationCancel = cancelActivation
	r.serveCancel = cancelServe
	generation := r.controllerGeneration
	r.lifecycle.mu.Unlock()
	r.mu.Unlock()
	activation = Activation{runtime: r, generation: generation, ctx: activationCtx}
	go r.runStarted(listener, lock, signalCh, stopSignals, serveDone)
	return activation, nil
}

// probeTrustVerifier proves one verifier child exchange end to end before the
// runtime serves. The verdict is ignored: the probe's minimal self identity
// (euid is all a nil-requirement policy consumes; pid is carried but unused)
// exists only to carry the exchange, and only transport failures abort startup.
func (r *Runtime) probeTrustVerifier(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, trustProbeTimeout)
	defer cancel()
	verifier := trust.ProcessVerifier{Runner: r.trustWorkers, Executable: r.trustExecutable}
	return verifier.Probe(probeCtx, peeridentity.Identity{PID: os.Getpid(), UID: os.Geteuid()})
}

func (r *Runtime) startupStopCause() error {
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if lifecycle.fatal != nil {
		return lifecycle.fatal
	}
	if r.startupFailure != nil {
		return r.startupFailure
	}
	if lifecycle.progress.State == LifecycleReady {
		return nil
	}
	if lifecycle.progress.State == LifecycleDraining && lifecycle.publication != nil && lifecycle.publication.publishedSet {
		return nil
	}
	return ErrRuntimeNotReady
}

func (r *Runtime) signalStop() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopping = true
		r.mu.Unlock()
		r.stop <- struct{}{}
	})
}

func (r *Runtime) markServerTerminal(serveErr error) error {
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	r.serverLive = false
	r.serverTerminal = true
	cancel := r.activationCancel
	switch lifecycle.progress.State {
	case LifecycleStarting, LifecycleReady:
		if serveErr == nil {
			serveErr = ErrSessionServerStopped
		}
		if lifecycle.progress.Sequence == math.MaxUint64 {
			serveErr = ErrSequenceExhausted
			lifecycle.setFatalLocked(serveErr)
		} else {
			r.startupFailure = serveErr
			if lifecycle.publication != nil {
				lifecycle.publication.invalidateStagedLocked()
			}
			lifecycle.invalidateActivitiesLocked()
			if err := lifecycle.advanceTerminalLocked(LifecycleFailed, lifecycle.progress.Detail); err != nil {
				panic("daemon: server terminalization violated sequence preflight")
			}
		}
	case LifecycleFailed:
		if r.startupFailure != nil {
			serveErr = r.startupFailure
		}
	case LifecycleDraining:
		if serveErr == nil || errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, net.ErrClosed) {
			serveErr = nil
		}
	}
	lifecycle.mu.Unlock()
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if serveErr != nil {
		r.signalStop()
	}
	return serveErr
}

//nolint:contextcheck // Shutdown must outlive the signal or serving context that initiated it.
func (r *Runtime) runStarted(
	listener net.Listener,
	lock *proc.FileLockHandle, signalCh <-chan os.Signal,
	stopSignals func(),
	serveDone <-chan error,
) {
	defer stopSignals()
	var cause error
	servedEarly := false
	select {
	case <-r.stop:
		cause = r.startupStopCause()
	case <-signalCh:
		cause = r.startupStopCause()
	case serveErr := <-serveDone:
		servedEarly = true
		cause = serveErr
	}
	r.mu.Lock()
	if r.serverTerminal && r.startupFailure != nil {
		cause = r.startupFailure
	}
	cancelActivation := r.activationCancel
	cancelServe := r.serveCancel
	r.mu.Unlock()
	shutdownErr := r.shutdown(listener, lock, cancelActivation, cancelServe, serveDone, servedEarly)
	r.finish(errors.Join(cause, shutdownErr))
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

//nolint:contextcheck // Terminal lifecycle publication is not request-scoped.
func (r *Runtime) finish(err error) {
	progress := r.lifecycle.Snapshot()
	switch progress.State {
	case LifecycleStarting:
		_ = r.lifecycle.fail()
	case LifecycleReady:
		_ = r.Drain()
	}
	r.mu.Lock()
	r.runErr = err
	r.finished = true
	close(r.done)
	r.mu.Unlock()
}

// Lifecycle returns the singular read-only runtime publication view.
func (r *Runtime) Lifecycle() LifecycleView { return r.lifecycle }

// Context returns the preparation lifetime canceled by failure or drain.
func (a Activation) Context() context.Context {
	if a.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return a.ctx
}

// RecoveryCapability issues one barrier proof during this Starting generation.
// Exactly one capability may be issued for each recovery ID.
func (a Activation) RecoveryCapability(id proc.RecoveryID) (RecoveryCapability, error) {
	if a.runtime == nil {
		return RecoveryCapability{}, ErrPublicationStale
	}
	if err := id.Validate(); err != nil {
		return RecoveryCapability{}, fmt.Errorf("daemon: recovery id: %w", err)
	}
	r := a.runtime
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if a.generation != r.controllerGeneration || r.finished || r.stopping || !r.serverLive ||
		r.serverTerminal || lifecycle.progress.State != LifecycleStarting || lifecycle.fatal != nil ||
		r.workerClaim == nil || !r.childrenClaimed {
		return RecoveryCapability{}, ErrPublicationStale
	}
	if _, exists := r.recoveryCapabilities[id]; exists {
		return RecoveryCapability{}, errors.New("daemon: recovery capability already issued")
	}
	children, err := r.cfg.Children.RecoveryReceipt(id)
	if err != nil {
		return RecoveryCapability{}, fmt.Errorf("daemon: child recovery proof: %w", err)
	}
	workers, err := r.workerClaim.RecoveryReceipt(id)
	if err != nil {
		return RecoveryCapability{}, fmt.Errorf("daemon: worker recovery proof: %w", err)
	}
	receipt, err := proc.CombineRecoveryReceipts(id, r.processGeneration, children, workers)
	if err != nil {
		return RecoveryCapability{}, fmt.Errorf("daemon: combine runtime recovery proof: %w", err)
	}
	state := &recoveryCapabilityState{}
	r.recoveryCapabilities[id] = state
	return RecoveryCapability{
		runtime: r, generation: a.generation, id: id, receipt: receipt, state: state,
	}, nil
}

// Receipt returns the immutable process-settlement proof carried by c.
func (c RecoveryCapability) Receipt() proc.RecoveryReceipt { return c.receipt }

// Consume marks c complete after product recovery has settled its receipt.
func (c RecoveryCapability) Consume() error {
	if c.runtime == nil || c.state == nil {
		return ErrPublicationStale
	}
	r := c.runtime
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	state := r.recoveryCapabilities[c.id]
	if c.generation != r.controllerGeneration || r.finished || r.stopping || !r.serverLive ||
		r.serverTerminal || lifecycle.progress.State != LifecycleStarting || lifecycle.fatal != nil ||
		state == nil || state != c.state || c.receipt.RecoveryID() != c.id ||
		c.receipt.Current() != r.processGeneration || c.receipt.Validate() != nil {
		return ErrPublicationStale
	}
	if state.consumed {
		return errors.New("daemon: recovery capability already consumed")
	}
	state.consumed = true
	return nil
}

// UpdateProgress publishes copied opaque Starting progress.
func (a Activation) UpdateProgress(detail []byte) error {
	if len(detail) > MaxLifecycleDetailBytes {
		return fmt.Errorf("daemon: lifecycle detail bytes=%d exceeds %d", len(detail), MaxLifecycleDetailBytes)
	}
	if a.runtime == nil {
		return ErrPublicationStale
	}
	r := a.runtime
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	defer r.mu.Unlock()
	if a.generation != r.controllerGeneration || r.finished {
		return ErrPublicationStale
	}
	if lifecycle.fatal != nil {
		return lifecycle.fatal
	}
	if lifecycle.progress.State == LifecycleDraining {
		return ErrDraining
	}
	if lifecycle.progress.State != LifecycleStarting {
		return ErrPublicationStale
	}
	if bytes.Equal(lifecycle.progress.Detail, detail) {
		return nil
	}
	return lifecycle.advanceStartingProgressLocked(detail)
}

// CommitReady atomically installs publication and opens Ready-only admission.
func (a Activation) CommitReady(publication Publication) error {
	if a.runtime == nil || publication.core == nil {
		return ErrPublicationStale
	}
	r := a.runtime
	r.mu.Lock()
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	if a.runtime != publication.core.runtime || a.generation != publication.generation ||
		a.generation != publication.core.generation || publication.core != lifecycle.publication ||
		publication.token != publication.core.token ||
		!publication.core.stagedSet || publication.stage != publication.core.nextStage || r.finished ||
		!r.serverLive || r.serverTerminal {
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return ErrPublicationStale
	}
	if lifecycle.fatal != nil {
		cause := lifecycle.fatal
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return cause
	}
	if lifecycle.progress.State == LifecycleDraining {
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return ErrDraining
	}
	if lifecycle.progress.State != LifecycleStarting {
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return ErrPublicationUnavailable
	}
	for _, recovery := range r.recoveryCapabilities {
		if !recovery.consumed {
			lifecycle.mu.Unlock()
			r.mu.Unlock()
			return errors.New("daemon: issued recovery capability is unconsumed")
		}
	}
	core := publication.core
	if lifecycle.progress.Sequence >= math.MaxUint64-1 {
		cause := ErrSequenceExhausted
		lifecycle.setFatalLocked(cause)
		cancel := r.activationCancel
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		r.signalStop()
		return cause
	}
	core.published = core.staged
	core.publishedSet = true
	core.publishedStage = publication.stage
	core.staged = nil
	core.stagedSet = false
	r.startupFailure = nil
	if err := lifecycle.advanceLocked(LifecycleReady, lifecycle.progress.Detail); err != nil {
		panic("daemon: commit readiness violated reserved lifecycle sequence")
	}
	lifecycle.mu.Unlock()
	r.mu.Unlock()
	return nil
}

// Fail atomically publishes Failed and terminates this preparation generation.
func (a Activation) Fail(cause error) error {
	if cause == nil {
		return errors.New("daemon: activation failure cause is required")
	}
	if a.runtime == nil {
		return ErrPublicationStale
	}
	r := a.runtime
	lifecycle := r.lifecycle
	r.mu.Lock()
	lifecycle.mu.Lock()
	if a.generation != r.controllerGeneration || r.finished || lifecycle.progress.State != LifecycleStarting {
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return ErrPublicationStale
	}
	if lifecycle.fatal != nil {
		cause := lifecycle.fatal
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		return cause
	}
	if lifecycle.progress.Sequence == math.MaxUint64 {
		sequenceErr := ErrSequenceExhausted
		lifecycle.setFatalLocked(sequenceErr)
		cancel := r.activationCancel
		lifecycle.mu.Unlock()
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		r.signalStop()
		return sequenceErr
	}
	r.startupFailure = cause
	cancel := r.activationCancel
	lifecycle.invalidateActivitiesLocked()
	if lifecycle.publication != nil {
		lifecycle.publication.invalidateStagedLocked()
	}
	if err := lifecycle.advanceTerminalLocked(LifecycleFailed, lifecycle.progress.Detail); err != nil {
		panic("daemon: activation failure violated sequence preflight")
	}
	lifecycle.mu.Unlock()
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.signalStop()
	return nil
}

func (r *Runtime) bindPublication(token *publicationToken) (*publicationCore, error) {
	if token == nil {
		return nil, errors.New("daemon: publication token is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil, errors.New("daemon: publication must be bound before Begin")
	}
	if r.publication != nil {
		return nil, errors.New("daemon: runtime publication is already bound")
	}
	core := &publicationCore{
		runtime: r, lifecycle: r.lifecycle, token: token, generation: r.controllerGeneration,
	}
	r.publication = core
	r.lifecycle.publication = core
	return core, nil
}

func (r *Runtime) admitReady() (Publication, func(), error) {
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	if lifecycle.fatal != nil {
		err := lifecycle.fatal
		lifecycle.mu.Unlock()
		return Publication{}, nil, err
	}
	if lifecycle.progress.State == LifecycleDraining {
		lifecycle.mu.Unlock()
		return Publication{}, nil, ErrDraining
	}
	core := lifecycle.publication
	if lifecycle.progress.State != LifecycleReady || core == nil || !core.publishedSet {
		lifecycle.mu.Unlock()
		return Publication{}, nil, ErrRuntimeNotReady
	}
	publication := Publication{
		core: core, token: core.token, generation: core.generation, stage: core.publishedStage, value: core.published,
	}
	lease := &publicationLease{alive: true}
	publication.lease = lease
	lifecycle.inflight++
	lifecycle.mu.Unlock()
	return publication, sync.OnceFunc(func() { r.finishAdmission(lease) }), nil
}

func (r *Runtime) admitProtected() (Publication, func(), error) {
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	if lifecycle.fatal != nil {
		err := lifecycle.fatal
		lifecycle.mu.Unlock()
		return Publication{}, nil, err
	}
	if lifecycle.progress.State == LifecycleDraining || lifecycle.progress.State == LifecycleFailed {
		lifecycle.mu.Unlock()
		return Publication{}, nil, ErrDraining
	}
	lifecycle.inflight++
	lifecycle.mu.Unlock()
	return Publication{}, sync.OnceFunc(func() { r.finishAdmission(nil) }), nil
}

func (r *Runtime) finishAdmission(lease *publicationLease) {
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	if lease != nil {
		lease.alive = false
	}
	lifecycle.inflight--
	if lifecycle.inflight == 0 && lifecycle.settled != nil {
		close(lifecycle.settled)
		lifecycle.settled = nil
	}
	lifecycle.mu.Unlock()
}

func (r *Runtime) settleAdmission(ctx context.Context) error {
	lifecycle := r.lifecycle
	lifecycle.mu.Lock()
	if lifecycle.inflight == 0 {
		lifecycle.mu.Unlock()
		return nil
	}
	if lifecycle.settled == nil {
		lifecycle.settled = make(chan struct{})
	}
	settled := lifecycle.settled
	lifecycle.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-settled:
		return nil
	}
}

// Drain atomically closes Ready-only admission and cancels preparation.
func (r *Runtime) Drain() error {
	r.mu.Lock()
	started, finished := r.started, r.finished
	if !started {
		r.mu.Unlock()
		return ErrRuntimeNotRunning
	}
	if finished {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	r.lifecycle.mu.Lock()
	var transitionErr error
	fatal := r.lifecycle.fatal
	if fatal != nil {
		fencedChildren := r.revokeChildFencesLocked()
		cancel := r.activationCancel
		r.lifecycle.mu.Unlock()
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		_ = r.server.CloseRuntimeIntake()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), r.shutdownTimeout())
		stopErr := stopFencedChildren(stopCtx, fencedChildren)
		stopCancel()
		if stopErr != nil {
			r.fatalFenceSettlement(errors.Join(fatal, stopErr))
		}
		r.signalStop()
		return errors.Join(fatal, stopErr)
	}
	switch r.lifecycle.progress.State {
	case LifecycleFailed, LifecycleDraining:
	case LifecycleStarting, LifecycleReady:
		if r.lifecycle.progress.Sequence == math.MaxUint64 {
			transitionErr = ErrSequenceExhausted
			r.lifecycle.setFatalLocked(transitionErr)
		} else {
			if r.lifecycle.publication != nil {
				r.lifecycle.publication.invalidateStagedLocked()
			}
			if err := r.lifecycle.advanceTerminalLocked(LifecycleDraining, r.lifecycle.progress.Detail); err != nil {
				panic("daemon: runtime drain violated sequence preflight")
			}
			r.lifecycle.invalidateActivitiesLocked()
		}
	default:
		r.lifecycle.mu.Unlock()
		r.mu.Unlock()
		return errors.New("daemon: invalid lifecycle state")
	}
	cancel := r.activationCancel
	fencedChildren := r.revokeChildFencesLocked()
	r.lifecycle.mu.Unlock()
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	_ = r.server.CloseRuntimeIntake()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), r.shutdownTimeout())
	stopErr := stopFencedChildren(stopCtx, fencedChildren)
	stopCancel()
	if stopErr != nil {
		r.fatalFenceSettlement(stopErr)
	}
	if transitionErr != nil {
		r.signalStop()
	}
	return errors.Join(transitionErr, stopErr)
}

//nolint:contextcheck // Drain is immediate; ctx bounds only the stop request.
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
	progress := r.lifecycle.Snapshot()
	var drainErr error
	if progress.State != LifecycleFailed && progress.State != LifecycleDraining {
		drainErr = r.Drain()
	}
	r.signalStop()
	return drainErr
}

func (r *Runtime) listen(ctx context.Context) (net.Listener, *proc.FileLockHandle, bool, error) {
	wait := r.cfg.ListenerWait
	if wait <= 0 {
		wait = 30 * time.Second
	}
	spec := proc.FileLockSpec{Path: r.cfg.Socket + ".lock", Mode: proc.FileLockExclusive, Deadline: wait}
	lock, err := spec.TryAcquire()
	if err != nil && !errors.Is(err, proc.ErrLockBusy) {
		return nil, nil, false, fmt.Errorf("daemon: acquire listener lock: %w", err)
	}
	conn, probeErr := net.DialTimeout("unix", r.cfg.Socket, 100*time.Millisecond)
	if probeErr == nil {
		_ = conn.Close()
		if lock != nil {
			_ = lock.Close()
		}
		return nil, nil, true, nil
	}
	if !errors.Is(probeErr, os.ErrNotExist) && !errors.Is(probeErr, syscall.ENOENT) && !errors.Is(probeErr, syscall.ECONNREFUSED) {
		if lock != nil {
			_ = lock.Close()
		}
		return nil, nil, false, fmt.Errorf("daemon: probe incumbent listener: %w", probeErr)
	}
	if lock == nil {
		lock, err = spec.Acquire(ctx)
		if err != nil {
			return nil, nil, false, fmt.Errorf("daemon: wait for listener lock: %w", err)
		}
	}
	_ = os.Remove(r.cfg.Socket)
	listener, err := net.Listen("unix", r.cfg.Socket)
	if err != nil {
		_ = lock.Close()
		return nil, nil, false, fmt.Errorf("daemon: bind listener: %w", err)
	}
	if err := os.Chmod(r.cfg.Socket, 0o600); err != nil {
		_ = listener.Close()
		_ = lock.Close()
		return nil, nil, false, fmt.Errorf("daemon: chmod listener: %w", err)
	}
	return listener, lock, false, nil
}

func (r *Runtime) shutdown(
	listener net.Listener,
	lock *proc.FileLockHandle, cancelActivation context.CancelFunc,
	cancelServe context.CancelFunc,
	serveDone <-chan error,
	servedEarly bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout())
	defer cancel()

	var errs []error
	if err := r.Drain(); err != nil && !errors.Is(err, ErrDraining) {
		errs = append(errs, fmt.Errorf("daemon: drain runtime: %w", err))
	}
	if err := r.server.CloseRuntimeIntake(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("daemon: close intake: %w", err))
	}
	cancelActivation()
	r.server.CancelRuntimeRequests()
	settled := true
	if err := r.settleAdmission(ctx); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle admission: %w", err))
		settled = false
	} else if err := r.server.SettleRuntimeSessions(ctx); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle session transport: %w", err))
		settled = false
	}
	cancelServe()
	if err := r.settleProduct(ctx); err != nil {
		cause := errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: settle product: %w", err))
		errs = append(errs, cause)
		r.lifecycle.mu.Lock()
		r.lifecycle.setFatalLocked(cause)
		r.lifecycle.mu.Unlock()
		r.retainOwnership(listener, lock)
		return errors.Join(errs...)
	}
	if err := r.workerClaim.Close(ctx); err != nil {
		errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: settle workers: %w", err)))
		settled = false
	}
	if err := r.cfg.Children.Shutdown(ctx); err != nil {
		errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: settle children: %w", err)))
		settled = false
	}
	var serveErr error
	if !servedEarly {
		select {
		case serveErr = <-serveDone:
		case <-ctx.Done():
			errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: join session server: %w", ctx.Err())))
			settled = false
		}
	}
	if serveErr != nil {
		errs = append(errs, serveErr)
	}
	if settled {
		r.lifecycle.mu.Lock()
		if r.lifecycle.publication != nil {
			r.lifecycle.publication.clearLocked()
		}
		r.lifecycle.mu.Unlock()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: close listener: %w", err)))
			settled = false
		}
	}
	if settled {
		if err := lock.Close(); err != nil {
			errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: close listener lock: %w", err)))
			settled = false
		}
	}
	if !settled {
		r.retainOwnership(listener, lock)
		if !errors.Is(errors.Join(errs...), ErrShutdownIncomplete) {
			errs = append(errs, ErrShutdownIncomplete)
		}
	}
	return errors.Join(errs...)
}

func (r *Runtime) abortBegin(
	cause error,
	listener net.Listener,
	lock *proc.FileLockHandle, cancelActivation context.CancelFunc,
	cancelServe context.CancelFunc,
	serveDone <-chan error,
	joined bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout())
	defer cancel()
	_ = r.Drain()
	_ = r.server.CloseRuntimeIntake()
	cancelActivation()
	var settleErr error
	if !joined {
		r.server.CancelRuntimeRequests()
		if err := r.settleAdmission(ctx); err != nil {
			settleErr = fmt.Errorf("daemon: settle admission: %w", err)
		} else if err := r.server.SettleRuntimeSessions(ctx); err != nil {
			settleErr = fmt.Errorf("daemon: settle session transport: %w", err)
		}
	}
	cancelServe()
	var serveErr error
	if !joined {
		select {
		case serveErr = <-serveDone:
		case <-ctx.Done():
			r.retainOwnership(listener, lock)
			return errors.Join(cause, ErrShutdownIncomplete, fmt.Errorf("daemon: join session server: %w", ctx.Err()))
		}
	}
	if serveErr != nil {
		cause = serveErr
	}
	closeErr := r.closeAcquired(ctx, listener, lock)
	return errors.Join(cause, settleErr, closeErr)
}

func (r *Runtime) retainOwnership(listener net.Listener, lock *proc.FileLockHandle) {
	r.mu.Lock()
	r.retainedListener = listener
	r.retainedLock = lock
	r.mu.Unlock()
}

//nolint:contextcheck // Drain is immediate while ctx bounds resource settlement.
func (r *Runtime) closeAcquired(ctx context.Context, listener net.Listener, lock *proc.FileLockHandle) error {
	var errs []error
	_ = r.Drain()
	if err := r.server.CloseRuntimeIntake(); err != nil && !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("daemon: close intake: %w", err))
	}
	settled := true
	if err := r.settleAdmission(ctx); err != nil {
		errs = append(errs, fmt.Errorf("daemon: settle admission: %w", err))
		settled = false
	}
	if err := r.releaseUnactivatedWorkers(ctx); err != nil {
		errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: release runtime workers: %w", err)))
		settled = false
	}
	if settled {
		r.lifecycle.mu.Lock()
		if r.lifecycle.publication != nil {
			r.lifecycle.publication.clearLocked()
		}
		r.lifecycle.mu.Unlock()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: close listener: %w", err)))
			settled = false
		}
	}
	if settled {
		if err := lock.Close(); err != nil {
			errs = append(errs, errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: close listener lock: %w", err)))
			settled = false
		}
	}
	if !settled {
		r.retainOwnership(listener, lock)
		if !errors.Is(errors.Join(errs...), ErrShutdownIncomplete) {
			errs = append(errs, ErrShutdownIncomplete)
		}
	}
	return errors.Join(errs...)
}

func (r *Runtime) closeUnstarted(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.shutdownTimeout())
	defer cancel()
	if err := r.releaseUnactivatedWorkers(ctx); err != nil {
		return wrapShutdownIncomplete("release runtime workers", err)
	}
	return nil
}

func (r *Runtime) releaseUnactivatedWorkers(ctx context.Context) error {
	r.mu.Lock()
	claim := r.workerClaim
	activated := r.workerActivated
	childrenClaimed := r.childrenClaimed
	r.mu.Unlock()
	if claim != nil {
		var err error
		if activated {
			err = claim.Close(ctx)
		} else {
			err = claim.Release(ctx)
		}
		if err != nil {
			return err
		}
	}
	r.mu.Lock()
	if r.workerClaim == claim {
		r.workerClaim = nil
		r.workerActivated = false
		r.trustWorkers = nil
	}
	r.mu.Unlock()
	if childrenClaimed {
		if err := r.cfg.Children.ReleaseRuntime(); err != nil {
			return err
		}
		r.mu.Lock()
		r.childrenClaimed = false
		r.mu.Unlock()
	}
	return nil
}

func wrapShutdownIncomplete(action string, err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrShutdownIncomplete, fmt.Errorf("daemon: %s: %w", action, err))
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
