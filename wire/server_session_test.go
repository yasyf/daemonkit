package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type partialFrameWriteConn struct {
	net.Conn
	writes    atomic.Int32
	entered   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *partialFrameWriteConn) Write(payload []byte) (int, error) {
	switch c.writes.Add(1) {
	case 1:
		return c.Conn.Write(payload)
	case 2:
		return 1, nil
	case 3:
		close(c.entered)
		<-c.closed
		return 0, net.ErrClosed
	default:
		return c.Conn.Write(payload)
	}
}

func (c *partialFrameWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type gatedCompleteFrameWriteConn struct {
	net.Conn
	writes  atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *gatedCompleteFrameWriteConn) Write(payload []byte) (int, error) {
	if c.writes.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(payload)
}

func TestServeSessionRoundTripGoAwayAndJoin(t *testing.T) {
	server := &Server{WireBuild: "session-test"}
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

func TestServeSessionGoAwayRejectsPartialFrameAcknowledgement(t *testing.T) {
	var active atomic.Int32
	server := &Server{WireBuild: "session-test", WriteTimeout: 50 * time.Millisecond}
	server.RegisterConcurrent("large", func(context.Context, Request) (any, error) {
		return strings.Repeat("x", 1<<20), nil
	})
	clientConn, rawServerConn := net.Pipe()
	serverConn := &partialFrameWriteConn{
		Conn: rawServerConn, entered: make(chan struct{}), closed: make(chan struct{}),
	}
	identity := currentSessionIdentity(t)
	admit := func() (func(), error) {
		active.Add(1)
		return func() { active.Add(-1) }, nil
	}
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), serverConn, identity,
			func() error { return nil }, admit, allowSession,
		)
	}()
	client := newExistingSessionClient(t, clientConn)
	if _, err := client.Open(context.Background(), "large", "", nil, true); err != nil {
		t.Fatalf("Open: %v", err)
	}
	select {
	case <-serverConn.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("server writer did not enter partial response frame")
	}
	if err := client.Close(); err == nil {
		t.Fatal("Close acknowledged after a partial server frame made the stream unusable")
	}
	awaitSessionServer(t, done)
	if got := active.Load(); got != 0 {
		t.Fatalf("active admissions = %d, want 0", got)
	}
}

func TestServeSessionGoAwayWaitsForTransientCompleteWriteAndAdmission(t *testing.T) {
	sessions := make(chan *AcceptedSession, 1)
	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var releaseHandler sync.Once
	t.Cleanup(func() { releaseHandler.Do(func() { close(handlerRelease) }) })
	server := &Server{WireBuild: "session-test"}
	server.RegisterControl("block", func(ctx context.Context, request Request) (any, error) {
		sessions <- request.Session
		close(handlerEntered)
		<-handlerRelease
		return nil, ctx.Err()
	})
	clientConn, rawServerConn := net.Pipe()
	serverConn := &gatedCompleteFrameWriteConn{
		Conn: rawServerConn, entered: make(chan struct{}), release: make(chan struct{}),
	}
	var releaseWrite sync.Once
	t.Cleanup(func() { releaseWrite.Do(func() { close(serverConn.release) }) })
	identity := currentSessionIdentity(t)
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), serverConn, identity,
			func() error { return nil }, allowSession, allowSession,
		)
	}()
	client := newExistingSessionClient(t, clientConn)
	if _, err := client.Open(context.Background(), "block", "", nil, true); err != nil {
		t.Fatalf("Open: %v", err)
	}
	session := <-sessions
	<-handlerEntered
	<-serverConn.entered
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case <-session.Disconnected():
	case <-time.After(time.Second):
		t.Fatal("Disconnected waited for transient complete write")
	}
	releaseWrite.Do(func() { close(serverConn.release) })
	select {
	case err := <-closed:
		t.Fatalf("client Close returned before blocked admission settled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseHandler.Do(func() { close(handlerRelease) })
	if err := <-closed; err != nil {
		t.Fatalf("client Close: %v", err)
	}
	awaitSessionServer(t, done)
}

func TestServeSessionPropagatesCancellationAndJoinsHandlers(t *testing.T) {
	entered := make(chan struct{})
	settled := make(chan struct{})
	server := &Server{WireBuild: "session-test"}
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
	server := &Server{WireBuild: "session-test"}
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
			done <- (&Server{WireBuild: "session-test"}).ServeSession(
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
		err := (&Server{WireBuild: "session-test"}).ServeSession(
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
		err := (&Server{WireBuild: "session-test"}).ServeSession(
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
	server := &Server{WireBuild: "session-test", protectedSessionClassifier: classifier}
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
		WireBuild: "session-test",
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
