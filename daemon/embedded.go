package daemon

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

// EmbeddedRuntime is a runtime whose readiness and terminal settlement are
// exact, replayable barriers.
type EmbeddedRuntime interface {
	Run(context.Context) error
	WaitReady(context.Context) error
	Close(context.Context) error
	Wait(context.Context) error
}

// EmbeddedFactory constructs one runtime. A non-nil runtime returned with an
// error remains owned by EmbeddedProcess and must support Close and Wait.
type EmbeddedFactory func(context.Context) (EmbeddedRuntime, error)

// EmbeddedProcess owns one in-process Runtime execution from construction
// through exact terminal settlement.
type EmbeddedProcess struct {
	mu sync.Mutex

	started       bool
	initialized   chan struct{}
	startupCancel context.CancelFunc
	runtime       EmbeddedRuntime
	runCancel     context.CancelFunc
	done          chan struct{}
	terminal      error
	closeOnce     sync.Once
	closeErr      error
}

// Start constructs and runs one runtime, returning only after exact readiness.
// Cancellation before readiness cancels and joins the runtime; after readiness,
// the caller context no longer owns the runtime lifetime.
func (p *EmbeddedProcess) Start(ctx context.Context, factory EmbeddedFactory) error {
	if factory == nil {
		return errors.New("daemon: embedded runtime factory is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	startup, cancelStartup := context.WithCancel(ctx)
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		cancelStartup()
		return ErrEmbeddedProcessStarted
	}
	p.started = true
	p.initialized = make(chan struct{})
	p.done = make(chan struct{})
	p.startupCancel = cancelStartup
	p.mu.Unlock()

	runtime, factoryErr := factory(startup)
	if embeddedRuntimeIsNil(runtime) {
		cancelStartup()
		p.setRuntime(nil, nil)
		if factoryErr != nil {
			return p.complete(factoryErr)
		}
		return p.complete(errors.New("daemon: embedded runtime factory returned nil"))
	}
	if factoryErr != nil {
		cancelStartup()
		settleErr := settleRejectedRuntime(startup, runtime)
		p.setRuntime(nil, nil)
		return p.complete(errors.Join(factoryErr, settleErr))
	}

	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	p.setRuntime(runtime, cancelRun)
	go func() {
		_ = p.complete(runtime.Run(runCtx))
	}()

	readyErr := runtime.WaitReady(startup)
	cancelStartup()
	p.clearStartupCancel()
	if readyErr == nil {
		return nil
	}
	cancelRun()
	return errors.Join(readyErr, p.waitSettled())
}

// Ready waits for exact current runtime readiness without polling.
func (p *EmbeddedProcess) Ready(ctx context.Context) error {
	runtime, err := p.waitRuntime(ctx)
	if err != nil {
		return err
	}
	return runtime.WaitReady(ctx)
}

// Close requests shutdown and returns only after the one Run execution and all
// runtime resources have settled. Caller cancellation is reported after join.
func (p *EmbeddedProcess) Close(ctx context.Context) error {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()
	if !started {
		return ErrEmbeddedProcessNotStarted
	}
	p.closeOnce.Do(func() {
		p.closeErr = p.close(ctx)
	})
	return p.closeErr
}

func (p *EmbeddedProcess) close(ctx context.Context) error {
	runtime, cancelRun, err := p.runtimeForClose(ctx)
	if err != nil {
		return err
	}
	closeErr := runtime.Close(ctx)
	if closeErr != nil {
		cancelRun()
	}
	terminal := p.waitSettled()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(ctxErr, terminal)
	}
	if terminal != nil {
		return terminal
	}
	return closeErr
}

// Wait joins the one Run execution and replays its terminal result.
func (p *EmbeddedProcess) Wait(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return ErrEmbeddedProcessNotStarted
	}
	done := p.done
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.terminal
	}
}

func (p *EmbeddedProcess) setRuntime(runtime EmbeddedRuntime, cancel context.CancelFunc) {
	p.mu.Lock()
	p.runtime = runtime
	p.runCancel = cancel
	close(p.initialized)
	p.mu.Unlock()
}

func (p *EmbeddedProcess) clearStartupCancel() {
	p.mu.Lock()
	p.startupCancel = nil
	p.mu.Unlock()
}

func (p *EmbeddedProcess) complete(err error) error {
	p.mu.Lock()
	p.terminal = err
	close(p.done)
	p.mu.Unlock()
	return err
}

func (p *EmbeddedProcess) waitRuntime(ctx context.Context) (EmbeddedRuntime, error) {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil, ErrEmbeddedProcessNotStarted
	}
	initialized := p.initialized
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-initialized:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runtime == nil {
		return nil, errors.Join(ErrRuntimeNotReady, p.terminal)
	}
	return p.runtime, nil
}

func (p *EmbeddedProcess) runtimeForClose(
	ctx context.Context,
) (EmbeddedRuntime, context.CancelFunc, error) {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil, nil, ErrEmbeddedProcessNotStarted
	}
	cancelStartup, initialized := p.startupCancel, p.initialized
	p.mu.Unlock()
	if cancelStartup != nil {
		cancelStartup()
	}
	select {
	case <-ctx.Done():
		<-initialized
	case <-initialized:
	}
	p.mu.Lock()
	if p.runtime == nil {
		p.mu.Unlock()
		return nil, nil, p.waitSettled()
	}
	runtime, cancelRun := p.runtime, p.runCancel
	p.mu.Unlock()
	return runtime, cancelRun, nil
}

func (p *EmbeddedProcess) waitSettled() error {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	<-done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminal
}

func settleRejectedRuntime(ctx context.Context, runtime EmbeddedRuntime) error {
	if runtime == nil {
		return nil
	}
	settleCtx := context.WithoutCancel(ctx)
	return errors.Join(runtime.Close(settleCtx), runtime.Wait(settleCtx))
}

func embeddedRuntimeIsNil(runtime EmbeddedRuntime) bool {
	if runtime == nil {
		return true
	}
	value := reflect.ValueOf(runtime)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ EmbeddedRuntime = (*Runtime)(nil)
