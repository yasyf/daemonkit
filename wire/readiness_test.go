package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/proc"
)

type readinessClassifier struct{}

func (readinessClassifier) Validate() error                              { return nil }
func (readinessClassifier) Classify(context.Context, Peer) (bool, error) { return false, nil }

type readinessStopVerifier struct{}

func (readinessStopVerifier) Validate() error { return nil }
func (readinessStopVerifier) VerifyStopControl(context.Context, Peer, string) (proc.Record, error) {
	return proc.Record{}, nil
}

type readinessControl struct{}

func (readinessControl) Health(context.Context) (daemon.Health, error) {
	return daemon.Health{
		RuntimeBuild: "runtime-v1", RuntimeProtocol: int(ProtocolVersion), PID: os.Getpid(),
		ProcessGeneration: "test-generation",
	}, nil
}
func (readinessControl) Shutdown(context.Context) error { return nil }

type gatedReadiness struct {
	entered   chan struct{}
	release   chan error
	after     chan error
	published atomic.Bool
}

type blockingPublication struct {
	entered   chan struct{}
	release   chan struct{}
	published atomic.Bool
}

func (*blockingPublication) BeforeReady(context.Context) error { return nil }

func (b *blockingPublication) AfterReady(err error) {
	if err != nil {
		b.published.Store(false)
		return
	}
	close(b.entered)
	<-b.release
	b.published.Store(true)
}

func (b *blockingPublication) Published() bool { return b.published.Load() }

func TestReadinessPublishesProductBeforeDaemonReady(t *testing.T) {
	barrier := &blockingPublication{entered: make(chan struct{}), release: make(chan struct{})}
	server := &Server{readiness: barrier}
	var daemonReady atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- server.runReadiness(t.Context(), func() error {
			daemonReady.Store(true)
			return nil
		})
	}()
	<-barrier.entered
	if daemonReady.Load() || barrier.Published() {
		t.Fatal("daemon readiness became visible before product publication committed")
	}
	close(barrier.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !daemonReady.Load() || !barrier.Published() {
		t.Fatal("readiness did not publish both product and daemon state")
	}
}

func (b *gatedReadiness) BeforeReady(ctx context.Context) error {
	close(b.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-b.release:
		return err
	}
}

func (b *gatedReadiness) AfterReady(err error) {
	b.published.Store(err == nil)
	b.after <- err
}

func (b *gatedReadiness) Published() bool { return b.published.Load() }

func TestReadinessScopesPreReadyDispatchAndPublishesWithIdleHealthClient(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "dkr-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	barrier := &gatedReadiness{
		entered: make(chan struct{}), release: make(chan error, 1), after: make(chan error, 1),
	}
	server := &Server{WireBuild: "product.rpc.v1", MaxSessions: 4}
	var bootstrapCalls, ordinaryCalls, observationCalls, authorizations atomic.Int32
	server.RegisterControl("native.bind", func(context.Context, Request) (any, error) {
		bootstrapCalls.Add(1)
		return true, nil
	})
	server.RegisterControl("tenant.provision", func(context.Context, Request) (any, error) {
		ordinaryCalls.Add(1)
		return true, nil
	})
	if err := server.bindRuntime(
		"runtime-v1",
		readinessClassifier{},
		1,
		readinessControl{},
		readinessStopVerifier{},
		[]ObservationRoute{{
			Op: "fusekit.runtime.health", AvailableBeforeReady: true,
			Handler: func(context.Context, ObservationRequest) (ObservationResponse, error) {
				observationCalls.Add(1)
				return ObservationResponse{Payload: json.RawMessage(`{"state":"starting"}`)}, nil
			},
		}},
		barrier,
		[]BootstrapRoute{{
			Op: "native.bind",
			Authorize: func(_ context.Context, request BootstrapRequest) error {
				authorizations.Add(1)
				if request.Peer.PID != os.Getpid() || request.WireBuild != "product.rpc.v1" {
					return errors.New("unexpected bootstrap peer")
				}
				return nil
			},
		}},
	); err != nil {
		t.Fatalf("bindRuntime: %v", err)
	}
	intake := &drain.Intake{}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(ctx, listener, func() error { return nil }, intake.Admit, intake.AdmitProtected)
	}()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("readiness barrier did not start")
	}
	client, err := NewClient(t.Context(), ClientConfig{Dial: UnixDialer(path), WireBuild: "product.rpc.v1"})
	if err != nil {
		t.Fatalf("pre-ready client: %v", err)
	}
	defer client.Close()
	assertDelivered(t, client, "native.bind")
	assertRejected(t, client, "tenant.provision", ErrNotReady)
	assertDelivered(t, client, "fusekit.runtime.health")
	if bootstrapCalls.Load() != 1 || authorizations.Load() != 1 || ordinaryCalls.Load() != 0 || observationCalls.Load() != 1 {
		t.Fatalf(
			"pre-ready calls bootstrap=%d auth=%d ordinary=%d observation=%d",
			bootstrapCalls.Load(), authorizations.Load(), ordinaryCalls.Load(), observationCalls.Load(),
		)
	}
	// Keep the completed health client's persistent session idle while readiness
	// publishes; session lifetime must not be mistaken for in-flight health work.
	barrier.release <- nil
	select {
	case err := <-barrier.after:
		if err != nil {
			t.Fatalf("AfterReady: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("AfterReady did not run")
	}
	assertDelivered(t, client, "tenant.provision")
	if ordinaryCalls.Load() != 1 {
		t.Fatalf("post-ready ordinary calls = %d", ordinaryCalls.Load())
	}
	cancel()
	if err := <-serveDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve: %v", err)
	}
}

func TestReadinessFailureDispatchesNoOrdinaryHandler(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "dkr-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("bootstrap failed")
	barrier := &gatedReadiness{
		entered: make(chan struct{}), release: make(chan error, 1), after: make(chan error, 1),
	}
	server := &Server{WireBuild: "product.rpc.v1", MaxSessions: 2}
	var calls atomic.Int32
	server.RegisterControl("tenant.provision", func(context.Context, Request) (any, error) {
		calls.Add(1)
		return true, nil
	})
	if err := server.bindRuntime(
		"runtime-v1", readinessClassifier{}, 1, readinessControl{}, readinessStopVerifier{}, nil, barrier, nil,
	); err != nil {
		t.Fatal(err)
	}
	intake := &drain.Intake{}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(t.Context(), listener, func() error { return nil }, intake.Admit, intake.AdmitProtected)
	}()
	<-barrier.entered
	client, err := NewClient(t.Context(), ClientConfig{Dial: UnixDialer(path), WireBuild: "product.rpc.v1"})
	if err != nil {
		t.Fatal(err)
	}
	assertRejected(t, client, "tenant.provision", ErrNotReady)
	_ = client.Close()
	barrier.release <- want
	if err := <-barrier.after; !errors.Is(err, want) {
		t.Fatalf("AfterReady error = %v, want %v", err, want)
	}
	if err := <-serveDone; !errors.Is(err, want) {
		t.Fatalf("Serve error = %v, want %v", err, want)
	}
	if calls.Load() != 0 {
		t.Fatalf("ordinary handler dispatched %d times", calls.Load())
	}
}

func assertDelivered(t *testing.T, client *Client, op Op) {
	t.Helper()
	result, err := client.Call(t.Context(), op, "", nil)
	if err != nil || result.Outcome != Delivered {
		t.Fatalf("%s = %#v, %v, want delivered", op, result, err)
	}
}

func assertRejected(t *testing.T, client *Client, op Op, want error) {
	t.Helper()
	result, err := client.Call(t.Context(), op, "", nil)
	if err != nil || result.Outcome != Rejected || result.Response.Reason != want.Error() {
		t.Fatalf("%s = %#v, %v, want rejected %v", op, result, err, want)
	}
	if errors.Is(want, ErrNotReady) {
		if result.Response.Code != ResponseCodeRuntimeStarting || !errors.Is(result.Rejection(), ErrNotReady) {
			t.Fatalf("%s rejection = %#v/%v, want typed runtime starting", op, result.Response, result.Rejection())
		}
	}
}
