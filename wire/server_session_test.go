package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestServeSessionRoundTripGoAwayAndJoin(t *testing.T) {
	server := &Server{Build: "session-test"}
	server.RegisterConcurrent("echo", func(_ context.Context, request Request) (any, error) {
		return string(request.Payload), nil
	})
	clientConn, serverConn := net.Pipe()
	identity := currentSessionIdentity(t)
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), serverConn, identity,
			func() error { close(ready); return nil }, allowSession, allowSession,
		)
	}()
	<-ready
	client := newExistingSessionClient(t, clientConn)
	result, err := client.Call(context.Background(), "echo", "", []byte("hello"))
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	var payload string
	if err := json.Unmarshal(result.Response.Payload, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload != "hello" || result.Outcome != Delivered {
		t.Fatalf("result = %q/%v, want hello/delivered", payload, result.Outcome)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	awaitSessionServer(t, done)
}

func TestServeSessionPropagatesCancellationAndJoinsHandlers(t *testing.T) {
	entered := make(chan struct{})
	settled := make(chan struct{})
	server := &Server{Build: "session-test"}
	server.RegisterConcurrent("block", func(ctx context.Context, _ Request) (any, error) {
		close(entered)
		<-ctx.Done()
		close(settled)
		return nil, ctx.Err()
	})
	clientConn, serverConn := net.Pipe()
	identity := currentSessionIdentity(t)
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), serverConn, identity,
			func() error { return nil }, allowSession, allowSession,
		)
	}()
	client := newExistingSessionClient(t, clientConn)
	call, err := client.Open(context.Background(), "block", "", nil, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	<-entered
	call.Cancel()
	result, err := call.Response(context.Background())
	if err != nil {
		t.Fatalf("Response: %v", err)
	}
	if result.Response.Err == "" {
		t.Fatal("canceled handler returned no error")
	}
	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Fatal("server handler leaked after cancellation")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	awaitSessionServer(t, done)
}

func TestServeSessionContextCancellationClosesIdleTransport(t *testing.T) {
	server := &Server{Build: "session-test"}
	clientConn, serverConn := net.Pipe()
	identity := currentSessionIdentity(t)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			ctx, serverConn, identity,
			func() error { close(ready); return nil }, allowSession, allowSession,
		)
	}()
	<-ready
	client := newExistingSessionClient(t, clientConn)
	cancel()
	awaitSessionServer(t, done)
	if err := client.Close(); err == nil {
		t.Fatal("client Close succeeded after server context canceled its transport")
	}
}

func TestServeSessionEOFAndOpaqueIdentityFailClosed(t *testing.T) {
	t.Run("EOF", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		identity := currentSessionIdentity(t)
		done := make(chan error, 1)
		go func() {
			done <- (&Server{Build: "session-test"}).ServeSession(
				context.Background(), serverConn, identity,
				func() error { return nil }, allowSession, allowSession,
			)
		}()
		_ = clientConn.Close()
		awaitSessionServer(t, done)
	})

	t.Run("unissued identity", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()
		err := (&Server{Build: "session-test"}).ServeSession(
			context.Background(), serverConn, SessionIdentity{},
			func() error { return nil }, allowSession, allowSession,
		)
		if err == nil {
			t.Fatal("ServeSession accepted a consumer-forged zero identity")
		}
	})

	t.Run("stale issued identity", func(t *testing.T) {
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		identity := currentSessionIdentity(t)
		identity.peer.StartTime += "-stale"
		err := (&Server{Build: "session-test"}).ServeSession(
			context.Background(), serverConn, identity,
			func() error { return nil }, allowSession, allowSession,
		)
		if !errors.Is(err, ErrUntrustedPeer) {
			t.Fatalf("ServeSession stale identity = %v, want ErrUntrustedPeer", err)
		}
	})
}

func TestServeSessionDoesNotElevateOrdinaryIdentity(t *testing.T) {
	classifier := &countingSessionClassifier{}
	server := &Server{Build: "session-test", ProtectedSessionClassifier: classifier}
	clientConn, serverConn := net.Pipe()
	identity := currentSessionIdentity(t)
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), serverConn, identity,
			func() error { return nil }, allowSession, allowSession,
		)
	}()
	client := newExistingSessionClient(t, clientConn)
	if err := client.Close(); err != nil {
		t.Fatalf("client Close: %v", err)
	}
	awaitSessionServer(t, done)
	if classifier.calls.Load() != 0 {
		t.Fatalf("protected classifier calls = %d, want 0", classifier.calls.Load())
	}
}

type countingSessionClassifier struct{ calls atomic.Int32 }

func (*countingSessionClassifier) Validate() error { return nil }

func (c *countingSessionClassifier) Classify(context.Context, Peer) (bool, error) {
	c.calls.Add(1)
	return true, nil
}

func (*countingSessionClassifier) AuthorizeLifecycleBuild(string, string) bool { return true }

func currentSessionIdentity(t *testing.T) SessionIdentity {
	t.Helper()
	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	return SessionIdentity{peer: Peer{
		PID: identity.PID, UID: os.Geteuid(), StartTime: identity.StartTime,
		Comm: identity.Comm, Boot: identity.Boot, Executable: identity.Executable,
	}}
}

func newExistingSessionClient(t *testing.T, conn net.Conn) *Client {
	t.Helper()
	var dialed atomic.Bool
	client, err := NewClient(context.Background(), ClientConfig{
		Build: "session-test",
		Dial: func(context.Context) (net.Conn, error) {
			if !dialed.CompareAndSwap(false, true) {
				return nil, errors.New("session already dialed")
			}
			return conn, nil
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func awaitSessionServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeSession: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeSession did not join; possible goroutine leak")
	}
}

func allowSession() (func(), error) { return func() {}, nil }
