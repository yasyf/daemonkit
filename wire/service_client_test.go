package wire

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type serviceTestServer struct {
	path   string
	cancel context.CancelFunc
	done   chan error
}

func startServiceTestServer(t *testing.T, server *Server, ready func() error) *serviceTestServer {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "dks-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "service.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener, ready, func() (func(), error) {
			return func() {}, nil
		}, func() (func(), error) {
			return func() {}, nil
		})
	}()
	running := &serviceTestServer{path: path, cancel: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve did not settle")
		}
	})
	return running
}

func registerServiceCall(server *Server, calls *atomic.Int32) {
	server.RegisterControl("service.call", func(context.Context, Request) (any, error) {
		calls.Add(1)
		return true, nil
	})
}

type serviceReadiness struct {
	entered   chan struct{}
	release   chan struct{}
	published atomic.Bool
}

func (r *serviceReadiness) BeforeReady(ctx context.Context) error {
	close(r.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
		return nil
	}
}

func (r *serviceReadiness) AfterReady(err error) { r.published.Store(err == nil) }
func (r *serviceReadiness) Published() bool      { return r.published.Load() }

func TestServiceClientWaitsThroughStartingOnOneSession(t *testing.T) {
	readiness := &serviceReadiness{entered: make(chan struct{}), release: make(chan struct{})}
	server := &Server{WireBuild: "service.v1", readiness: readiness}
	var handlerCalls atomic.Int32
	registerServiceCall(server, &handlerCalls)
	running := startServiceTestServer(t, server, func() error { return nil })
	<-readiness.entered

	var dials atomic.Int32
	client, err := NewServiceClient(ClientConfig{
		WireBuild: "service.v1",
		Dial: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			return UnixDialer(running.path)(ctx)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var waits atomic.Int32
	client.wait = func(context.Context, time.Duration) error {
		if waits.Add(1) == 1 {
			close(readiness.release)
		}
		return nil
	}

	result, err := client.Call(t.Context(), "service.call", "", nil)
	if err != nil || result.Outcome != Delivered {
		t.Fatalf("Call = %#v, %v", result, err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want 1", got)
	}
	if got := waits.Load(); got != 1 {
		t.Fatalf("waits = %d, want 1", got)
	}
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestServiceClientCrossesDrainMissingListenerAndTakeover(t *testing.T) {
	oldServer := &Server{WireBuild: "service.v1"}
	newServer := &Server{WireBuild: "service.v1"}
	var oldCalls, newCalls atomic.Int32
	registerServiceCall(oldServer, &oldCalls)
	registerServiceCall(newServer, &newCalls)
	oldRuntime := startServiceTestServer(t, oldServer, func() error { return nil })
	newRuntime := startServiceTestServer(t, newServer, func() error { return nil })

	var phase atomic.Int32
	var dials atomic.Int32
	client, err := NewServiceClient(ClientConfig{
		WireBuild: "service.v1",
		Dial: func(ctx context.Context) (net.Conn, error) {
			dials.Add(1)
			switch phase.Load() {
			case 0:
				return UnixDialer(oldRuntime.path)(ctx)
			case 1:
				return nil, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
			case 2:
				return UnixDialer(newRuntime.path)(ctx)
			default:
				panic("unexpected takeover phase")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if result, err := client.Call(t.Context(), "service.call", "", nil); err != nil || result.Outcome != Delivered {
		t.Fatalf("initial Call = %#v, %v", result, err)
	}
	if err := oldServer.CloseIntake(); err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	var waits atomic.Int32
	client.wait = func(context.Context, time.Duration) error {
		if waits.Add(1) == 2 {
			phase.Store(2)
		}
		return nil
	}

	result, err := client.Call(t.Context(), "service.call", "", nil)
	if err != nil || result.Outcome != Delivered {
		t.Fatalf("takeover Call = %#v, %v", result, err)
	}
	if got := dials.Load(); got != 3 {
		t.Fatalf("dials = %d, want old + absent + successor", got)
	}
	if got := waits.Load(); got != 2 {
		t.Fatalf("waits = %d, want drain + absent endpoint", got)
	}
	if oldCalls.Load() != 1 || newCalls.Load() != 1 {
		t.Fatalf("handler calls old=%d new=%d, want 1 each", oldCalls.Load(), newCalls.Load())
	}
	select {
	case <-client.Done():
		t.Fatal("generation takeover closed service lifetime")
	default:
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("Close did not settle service lifetime")
	}
}

func TestServiceClientCancellationBoundsMissingEndpoint(t *testing.T) {
	var attempts atomic.Int32
	client, err := NewServiceClient(ClientConfig{
		WireBuild: "service.v1",
		Dial: func(context.Context) (net.Conn, error) {
			attempts.Add(1)
			return nil, &os.PathError{Op: "connect", Path: "/missing", Err: syscall.ENOENT}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	client.wait = func(ctx context.Context, _ time.Duration) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Call(ctx, "service.call", "", nil)
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Call error = %v, want canceled", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("dial attempts = %d, want 1", got)
	}
}

func TestServiceClientRejectsEmptyOperationBeforeDial(t *testing.T) {
	var attempts atomic.Int32
	client, err := NewServiceClient(ClientConfig{
		WireBuild: "service.v1",
		Dial: func(context.Context) (net.Conn, error) {
			attempts.Add(1)
			return nil, errors.New("unexpected dial")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Call(t.Context(), "", "", nil)
	if err == nil || err.Error() != "wire: operation is required" {
		t.Fatalf("Call error = %v, want required operation", err)
	}
	if result.Outcome != PreSendFailure {
		t.Fatalf("Call outcome = %v, want PreSendFailure", result.Outcome)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("dial attempts = %d, want 0", got)
	}
}

type manualDeadlineContext struct {
	done chan struct{}
	once sync.Once
}

func newManualDeadlineContext() *manualDeadlineContext {
	return &manualDeadlineContext{done: make(chan struct{})}
}

func (*manualDeadlineContext) Deadline() (time.Time, bool) { return time.Now().Add(time.Hour), true }
func (c *manualDeadlineContext) Done() <-chan struct{}     { return c.done }
func (c *manualDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
func (*manualDeadlineContext) Value(any) any { return nil }
func (c *manualDeadlineContext) expire()     { c.once.Do(func() { close(c.done) }) }

func TestServiceClientDeadlineBoundsStarting(t *testing.T) {
	readiness := &serviceReadiness{entered: make(chan struct{}), release: make(chan struct{})}
	server := &Server{WireBuild: "service.v1", readiness: readiness}
	var calls atomic.Int32
	registerServiceCall(server, &calls)
	running := startServiceTestServer(t, server, func() error { return nil })
	<-readiness.entered
	client, err := NewServiceClient(ClientConfig{
		WireBuild: "service.v1",
		Dial:      UnixDialer(running.path),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx := newManualDeadlineContext()
	client.wait = func(context.Context, time.Duration) error {
		ctx.expire()
		return ctx.Err()
	}
	result, err := client.Call(ctx, "service.call", "", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call error = %v, want deadline exceeded", err)
	}
	if result.Outcome != Rejected || result.Response.Code != ResponseCodeRuntimeStarting {
		t.Fatalf("Call result = %#v, want typed starting rejection", result)
	}
	if calls.Load() != 0 {
		t.Fatalf("starting call dispatched %d handlers", calls.Load())
	}
}

func TestServiceClientRetriesOnlyProvenNondispatch(t *testing.T) {
	tests := []struct {
		name      string
		result    Result
		err       error
		retry     bool
		reconnect bool
	}{
		{name: "starting", result: Result{Outcome: Rejected, Response: Response{Rejected: true, Code: ResponseCodeRuntimeStarting}}, retry: true},
		{name: "draining", result: Result{Outcome: Rejected, Response: Response{Rejected: true, Code: ResponseCodeServerDraining}}, retry: true, reconnect: true},
		{name: "build mismatch", result: Result{Outcome: Rejected, Response: Response{Rejected: true, Reason: ErrBuildMismatch.Error()}}},
		{name: "queue rejection", result: Result{Outcome: Rejected, Response: Response{Rejected: true, Reason: ErrQueueFull.Error()}}},
		{name: "pre-send", result: Result{Outcome: PreSendFailure}, err: errors.New("closed"), retry: true, reconnect: true},
		{name: "protocol pre-send", result: Result{Outcome: PreSendFailure}, err: ErrProtocolVersion},
		{name: "invalid frame pre-send", result: Result{Outcome: PreSendFailure}, err: ErrInvalidFrame},
		{name: "post-send", result: Result{Outcome: PostSendFailure}, err: errors.New("closed")},
		{name: "unknown delivery", result: Result{Outcome: DeliveryUnknown}, err: errors.New("closed")},
		{name: "business fence", result: Result{Outcome: Delivered, Response: Response{Err: "fence mismatch"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry, reconnect := retryServiceCall(test.result, test.err)
			if retry != test.retry || reconnect != test.reconnect {
				t.Fatalf("retry/reconnect = %t/%t, want %t/%t", retry, reconnect, test.retry, test.reconnect)
			}
		})
	}
}
