package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmbeddedProcessOwnsLifetimeAfterExactReadiness(t *testing.T) {
	runtime := newEmbeddedRuntime(nil)
	process := &EmbeddedProcess{}
	startCtx, cancelStart := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() {
		started <- process.Start(startCtx, func(context.Context) (EmbeddedRuntime, error) {
			return runtime, nil
		})
	}()
	<-runtime.runStarted
	select {
	case err := <-started:
		t.Fatalf("Start returned before readiness: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	runtime.publishReady()
	if err := <-started; err != nil {
		t.Fatalf("Start = %v", err)
	}
	cancelStart()
	select {
	case <-runtime.done:
		t.Fatal("startup context cancellation ended ready runtime lifetime")
	case <-time.After(30 * time.Millisecond):
	}
	if err := process.Ready(context.Background()); err != nil {
		t.Fatalf("Ready = %v", err)
	}
	if err := process.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
	for i := range 3 {
		if err := process.Wait(context.Background()); err != nil {
			t.Fatalf("Wait %d = %v", i, err)
		}
		if err := process.Close(context.Background()); err != nil {
			t.Fatalf("repeated Close %d = %v", i, err)
		}
	}
	if got := runtime.closeCalls.Load(); got != 1 {
		t.Fatalf("physical runtime Close calls = %d, want 1", got)
	}
}

func TestEmbeddedProcessStartupCancellationCancelsAndJoins(t *testing.T) {
	runtime := newEmbeddedRuntime(nil)
	process := &EmbeddedProcess{}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() {
		started <- process.Start(ctx, func(context.Context) (EmbeddedRuntime, error) {
			return runtime, nil
		})
	}()
	<-runtime.runStarted
	cancel()
	if err := <-started; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want context.Canceled", err)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("Start returned before canceled runtime joined")
	}
	if err := process.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want replayed context.Canceled", err)
	}
}

func TestEmbeddedProcessCloseCancellationStillJoins(t *testing.T) {
	runtime := newEmbeddedRuntime(nil)
	runtime.publishReady()
	process := &EmbeddedProcess{}
	if err := process.Start(context.Background(), func(context.Context) (EmbeddedRuntime, error) {
		return runtime, nil
	}); err != nil {
		t.Fatalf("Start = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := process.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v, want context.Canceled after settlement", err)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("Close returned caller cancellation before runtime joined")
	}
	if err := process.Wait(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want nil or canceled terminal", err)
	}
}

func TestEmbeddedProcessConcurrentCloseAndWaitUseOneRunJoin(t *testing.T) {
	runtime := newEmbeddedRuntime(nil)
	runtime.publishReady()
	process := &EmbeddedProcess{}
	if err := process.Start(context.Background(), func(context.Context) (EmbeddedRuntime, error) {
		return runtime, nil
	}); err != nil {
		t.Fatalf("Start = %v", err)
	}

	const callers = 8
	results := make(chan error, callers*2)
	var callersWG sync.WaitGroup
	for range callers {
		callersWG.Add(2)
		go func() {
			defer callersWG.Done()
			results <- process.Close(context.Background())
		}()
		go func() {
			defer callersWG.Done()
			results <- process.Wait(context.Background())
		}()
	}
	callersWG.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent terminal result = %v", err)
		}
	}
	if got := runtime.closeCalls.Load(); got != 1 {
		t.Fatalf("physical runtime Close calls = %d, want 1", got)
	}
}

func TestEmbeddedProcessCloseDuringFactoryCancelsStartupAndJoins(t *testing.T) {
	process := &EmbeddedProcess{}
	factoryEntered := make(chan struct{})
	started := make(chan error, 1)
	go func() {
		started <- process.Start(context.Background(), func(ctx context.Context) (EmbeddedRuntime, error) {
			close(factoryEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	}()
	<-factoryEntered
	closeErr := process.Close(context.Background())
	if !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("Close = %v, want factory context cancellation", closeErr)
	}
	if err := <-started; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v, want factory context cancellation", err)
	}
	if err := process.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want replayed factory cancellation", err)
	}
}

func TestEmbeddedProcessSettlesRuntimeReturnedWithFactoryError(t *testing.T) {
	want := errors.New("factory failed after ownership")
	runtime := newEmbeddedRuntime(nil)
	go func() { _ = runtime.Run(context.Background()) }()
	<-runtime.runStarted
	process := &EmbeddedProcess{}
	err := process.Start(context.Background(), func(context.Context) (EmbeddedRuntime, error) {
		return runtime, want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Start = %v, want %v", err, want)
	}
	select {
	case <-runtime.done:
	default:
		t.Fatal("factory-error runtime was not joined")
	}
	if got := runtime.closeCalls.Load(); got != 1 {
		t.Fatalf("factory-error runtime Close calls = %d, want 1", got)
	}
	if err := process.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Wait = %v, want replayed factory error", err)
	}
}

func TestEmbeddedProcessRejectsSecondStartAndPrestartLifecycle(t *testing.T) {
	process := &EmbeddedProcess{}
	if err := process.Ready(context.Background()); !errors.Is(err, ErrEmbeddedProcessNotStarted) {
		t.Fatalf("Ready before Start = %v", err)
	}
	if err := process.Wait(context.Background()); !errors.Is(err, ErrEmbeddedProcessNotStarted) {
		t.Fatalf("Wait before Start = %v", err)
	}
	if err := process.Close(context.Background()); !errors.Is(err, ErrEmbeddedProcessNotStarted) {
		t.Fatalf("Close before Start = %v", err)
	}
	runtime := newEmbeddedRuntime(nil)
	runtime.publishReady()
	if err := process.Start(context.Background(), func(context.Context) (EmbeddedRuntime, error) {
		return runtime, nil
	}); err != nil {
		t.Fatalf("Start = %v", err)
	}
	if err := process.Start(context.Background(), func(context.Context) (EmbeddedRuntime, error) {
		return newEmbeddedRuntime(nil), nil
	}); !errors.Is(err, ErrEmbeddedProcessStarted) {
		t.Fatalf("second Start = %v, want ErrEmbeddedProcessStarted", err)
	}
	if err := process.Close(context.Background()); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

type embeddedRuntime struct {
	ready      chan struct{}
	stop       chan struct{}
	done       chan struct{}
	runStarted chan struct{}
	terminal   error

	readyOnce  sync.Once
	stopOnce   sync.Once
	runOnce    sync.Once
	finishOnce sync.Once
	closeCalls atomic.Int32
}

func newEmbeddedRuntime(terminal error) *embeddedRuntime {
	return &embeddedRuntime{
		ready:      make(chan struct{}),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		runStarted: make(chan struct{}),
		terminal:   terminal,
	}
}

func (r *embeddedRuntime) Run(ctx context.Context) error {
	ran := false
	r.runOnce.Do(func() {
		ran = true
		close(r.runStarted)
	})
	if !ran {
		return ErrRuntimeStarted
	}
	var err error
	select {
	case <-ctx.Done():
		err = errors.Join(r.terminal, ctx.Err())
	case <-r.stop:
		err = r.terminal
	}
	r.finishOnce.Do(func() { close(r.done) })
	return err
}

func (r *embeddedRuntime) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.ready:
		return nil
	case <-r.done:
		return errors.Join(ErrRuntimeNotReady, r.terminal)
	}
}

func (r *embeddedRuntime) Close(ctx context.Context) error {
	r.closeCalls.Add(1)
	r.stopOnce.Do(func() { close(r.stop) })
	return r.Wait(ctx)
}

func (r *embeddedRuntime) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return r.terminal
	}
}

func (r *embeddedRuntime) publishReady() {
	r.readyOnce.Do(func() { close(r.ready) })
}
