package wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

type runningServer struct {
	path     string
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
}

type blockAfterHandshakeConn struct {
	net.Conn
	mu          sync.Mutex
	reads       int
	release     chan struct{}
	releaseOnce sync.Once
}

func (c *blockAfterHandshakeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	c.reads++
	block := c.reads > 2
	c.mu.Unlock()
	if block {
		<-c.release
	}
	return c.Conn.Read(p)
}

func (c *blockAfterHandshakeConn) releaseReads() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *blockAfterHandshakeConn) Close() error {
	c.releaseReads()
	return c.Conn.Close()
}

func (r *runningServer) stop(t *testing.T) {
	t.Helper()
	r.stopOnce.Do(func() {
		r.cancel()
		select {
		case err := <-r.done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Serve did not stop")
		}
	})
}

func startSessionServer(t *testing.T, server *wire.Server, admit func() (func(), error)) *runningServer {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "daemonkit-wire-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, admit) }()
	running := &runningServer{path: path, cancel: cancel, done: done}
	t.Cleanup(func() { running.stop(t) })
	return running
}

func newClient(t *testing.T, running *runningServer, config func(*wire.ClientConfig)) *wire.Client {
	t.Helper()
	cfg := wire.ClientConfig{Dial: wire.UnixDialer(running.path), Build: "server-test"}
	if config != nil {
		config(&cfg)
	}
	client, err := wire.NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func admitAll(counter *atomic.Int32) func() (func(), error) {
	return func() (func(), error) {
		counter.Add(1)
		return func() { counter.Add(-1) }, nil
	}
}

func TestPersistentSessionMultiplexesEventsAndStreams(t *testing.T) {
	server := &wire.Server{Build: "server-test"}
	server.RegisterConcurrent("echo", func(_ context.Context, request wire.Request) (any, error) {
		return string(request.Payload), nil
	})
	server.RegisterControl("event", func(ctx context.Context, request wire.Request) (any, error) {
		if err := request.Session.PushEvent(ctx, wire.Event{Topic: "changed", Payload: []byte(request.Tenant)}); err != nil {
			return nil, err
		}
		return "ok", nil
	})
	server.RegisterControl("stream", func(context.Context, wire.Request) (any, error) {
		chunks := make(chan []byte, 2)
		chunks <- []byte("a")
		chunks <- []byte("b")
		close(chunks)
		return wire.StreamResponse{Chunks: chunks, Value: "done"}, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, nil)
	if got := client.PeerBuild(); got.Protocol != wire.ProtocolVersion || got.Build != "server-test" {
		t.Fatalf("PeerBuild = %#v", got)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte{byte(i)}
			result, err := client.Call(context.Background(), "echo", "", payload)
			if err != nil {
				errs <- err
				return
			}
			var got string
			if err := json.Unmarshal(result.Response.Payload, &got); err != nil || got != string(payload) {
				errs <- errors.New("echo mismatch")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if _, err := client.Call(context.Background(), "event", "acct-18", nil); err != nil {
		t.Fatalf("event call: %v", err)
	}
	select {
	case event := <-client.Events():
		if event.Topic != "changed" || string(event.Payload) != "acct-18" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("event not pushed")
	}

	call, err := client.Open(context.Background(), "stream", "", nil, true)
	if err != nil {
		t.Fatalf("Open stream: %v", err)
	}
	var chunks []string
	for chunk := range call.Chunks() {
		if !chunk.End {
			chunks = append(chunks, string(chunk.Payload))
		}
	}
	result, err := call.Response(context.Background())
	if err != nil {
		t.Fatalf("stream Response: %v", err)
	}
	if len(chunks) != 2 || chunks[0] != "a" || chunks[1] != "b" || result.Outcome != wire.Delivered {
		t.Fatalf("stream chunks=%v outcome=%s", chunks, result.Outcome)
	}
	again, err := call.Response(context.Background())
	if err != nil || again.Outcome != wire.Delivered {
		t.Fatalf("repeat Response = %#v, %v", again, err)
	}
	if err := call.SendChunk(context.Background(), []byte("late")); !errors.Is(err, wire.ErrCallDone) {
		t.Fatalf("late SendChunk error = %v, want ErrCallDone", err)
	}
	if got := inflight.Load(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
}

func TestSessionSurvivesPastHandshakeDeadline(t *testing.T) {
	const handshakeTimeout = 100 * time.Millisecond
	server := &wire.Server{Build: "server-test", HandshakeTimeout: handshakeTimeout}
	server.RegisterControl("ping", func(context.Context, wire.Request) (any, error) { return "pong", nil })
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, func(config *wire.ClientConfig) {
		config.HandshakeTimeout = handshakeTimeout
	})
	time.Sleep(3 * handshakeTimeout)
	result, err := client.Call(context.Background(), "ping", "", nil)
	if err != nil {
		t.Fatalf("Call after handshake deadline: %v", err)
	}
	if result.Outcome != wire.Delivered {
		t.Fatalf("outcome = %s, want delivered", result.Outcome)
	}
}

func TestClientHandshakeUsesEarlierCallerDeadline(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	release := make(chan struct{})
	defer close(release)
	go func() {
		_, _ = wire.NewCodec(serverConn).ReadFrame()
		<-release
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := wire.NewClient(ctx, wire.ClientConfig{
		Build:            "client-test",
		HandshakeTimeout: 5 * time.Second,
		Dial: func(context.Context) (net.Conn, error) {
			return clientConn, nil
		},
	})
	if err == nil {
		t.Fatal("NewClient unexpectedly completed a handshake without an acknowledgment")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("handshake observed config timeout instead of caller deadline: %s", elapsed)
	}
}

func TestAdmissionCompletesAfterTerminalFrameIsWritten(t *testing.T) {
	const maxFrame = 16 << 20
	handlerStarted := make(chan struct{})
	admissionDone := make(chan struct{}, 1)
	server := &wire.Server{Build: "server-test", MaxFrame: maxFrame}
	server.RegisterControl("large", func(context.Context, wire.Request) (any, error) {
		close(handlerStarted)
		return strings.Repeat("x", 8<<20), nil
	})
	running := startSessionServer(t, server, func() (func(), error) {
		return func() { admissionDone <- struct{}{} }, nil
	})
	conn, err := net.Dial("unix", running.path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	codec := wire.NewCodec(conn)
	codec.MaxFrame = maxFrame
	identity, err := json.Marshal(wire.BuildIdentity{Protocol: wire.ProtocolVersion, Build: "server-test"})
	if err != nil {
		t.Fatalf("Marshal identity: %v", err)
	}
	if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameHello, Flags: wire.FlagEnd, Payload: identity}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := codec.ReadFrame(); err != nil {
		t.Fatalf("read hello ack: %v", err)
	}
	if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameRequest, Flags: wire.FlagEnd, ID: 1, Op: "large"}); err != nil {
		t.Fatalf("write request: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := server.CloseIntake(); err != nil {
		t.Fatalf("CloseIntake: %v", err)
	}
	select {
	case <-admissionDone:
		t.Fatal("admission completed before the blocked terminal frame was written")
	case <-time.After(75 * time.Millisecond):
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read terminal response: %v", err)
	}
	if frame.Kind != wire.FrameResponse || frame.ID != 1 {
		t.Fatalf("terminal frame = %#v", frame)
	}
	select {
	case <-admissionDone:
	case <-time.After(time.Second):
		t.Fatal("admission did not complete after terminal delivery")
	}
	running.stop(t)
	codec.ReadTimeout = 100 * time.Millisecond
	if second, err := codec.ReadFrame(); err == nil {
		t.Fatalf("second terminal frame = %#v", second)
	}
}

func TestBackpressuredResponseCancellationFailsSessionAndReleasesAdmission(t *testing.T) {
	const maxFrame = 2 << 20
	admissionDone := make(chan struct{}, 1)
	server := &wire.Server{Build: "server-test", MaxFrame: maxFrame, OutboundQueue: 1}
	server.RegisterConcurrent("stream", func(context.Context, wire.Request) (any, error) {
		chunks := make(chan []byte, 4)
		payload := []byte(strings.Repeat("x", 1<<20))
		for range 4 {
			chunks <- payload
		}
		close(chunks)
		return wire.StreamResponse{Chunks: chunks, Value: true}, nil
	})
	running := startSessionServer(t, server, func() (func(), error) {
		return func() { admissionDone <- struct{}{} }, nil
	})
	var blocked *blockAfterHandshakeConn
	client, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Build:    "server-test",
		MaxFrame: maxFrame,
		Dial: func(ctx context.Context) (net.Conn, error) {
			conn, err := wire.UnixDialer(running.path)(ctx)
			if err != nil {
				return nil, err
			}
			blocked = &blockAfterHandshakeConn{Conn: conn, release: make(chan struct{})}
			return blocked, nil
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	call, err := client.Open(ctx, "stream", "", nil, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	select {
	case <-admissionDone:
	case <-time.After(2 * time.Second):
		t.Fatal("admission leaked after response stream backpressure cancellation")
	}
	blocked.releaseReads()
	result, err := call.Response(context.Background())
	if err == nil || result.Outcome != wire.PostSendFailure {
		t.Fatalf("Response = %#v, %v; want post-send session failure", result, err)
	}
	again, againErr := call.Response(context.Background())
	if againErr == nil || again.Outcome != wire.PostSendFailure {
		t.Fatalf("repeat Response = %#v, %v; want cached post-send failure", again, againErr)
	}
}

func TestUnreadEventFailsSessionWithoutHeadOfLineBlocking(t *testing.T) {
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("events", func(ctx context.Context, request wire.Request) (any, error) {
		if err := request.Session.PushEvent(ctx, wire.Event{Topic: "one"}); err != nil {
			return nil, err
		}
		if err := request.Session.PushEvent(ctx, wire.Event{Topic: "two"}); err != nil {
			return nil, err
		}
		return true, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, func(config *wire.ClientConfig) { config.EventQueue = 1 })
	_, err := client.Call(context.Background(), "events", "", nil)
	if !errors.Is(err, wire.ErrQueueFull) {
		t.Fatalf("Call error = %v, want ErrQueueFull", err)
	}
}

func TestUnreadResponseStreamFailsOnlyItsCall(t *testing.T) {
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("stream-many", func(context.Context, wire.Request) (any, error) {
		chunks := make(chan []byte, 3)
		chunks <- []byte("one")
		chunks <- []byte("two")
		chunks <- []byte("three")
		close(chunks)
		return wire.StreamResponse{Chunks: chunks, Value: true}, nil
	})
	server.RegisterControl("echo", func(context.Context, wire.Request) (any, error) { return true, nil })
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, func(config *wire.ClientConfig) { config.StreamQueue = 1 })
	call, err := client.Open(context.Background(), "stream-many", "", nil, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_, err = call.Response(context.Background())
	if !errors.Is(err, wire.ErrQueueFull) {
		t.Fatalf("Response error = %v, want ErrQueueFull", err)
	}
	time.Sleep(20 * time.Millisecond)
	result, err := client.Call(context.Background(), "echo", "", nil)
	if err != nil || result.Outcome != wire.Delivered {
		t.Fatalf("subsequent Call = %#v, %v", result, err)
	}
}

func TestCanceledCallSettlementTimeoutFailsSession(t *testing.T) {
	release := make(chan struct{})
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("ignore-cancel", func(context.Context, wire.Request) (any, error) {
		<-release
		return true, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, func(config *wire.ClientConfig) {
		config.CancelSettlementTimeout = 30 * time.Millisecond
	})
	call, err := client.Open(context.Background(), "ignore-cancel", "", nil, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	call.Cancel()
	time.Sleep(60 * time.Millisecond)
	_, err = client.Call(context.Background(), "ignore-cancel", "", nil)
	if !errors.Is(err, wire.ErrCancelSettlement) {
		t.Fatalf("subsequent Call error = %v, want ErrCancelSettlement", err)
	}
	close(release)
}

func TestRequestStreamOrderingAndCancellation(t *testing.T) {
	canceled := make(chan struct{})
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("collect", func(_ context.Context, request wire.Request) (any, error) {
		var values []string
		for chunk := range request.Chunks {
			if !chunk.End {
				values = append(values, string(chunk.Payload))
			}
		}
		return values, nil
	})
	server.RegisterControl("block", func(ctx context.Context, _ wire.Request) (any, error) {
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, nil)

	call, err := client.Open(context.Background(), "collect", "", nil, false)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := call.SendChunk(context.Background(), []byte("one")); err != nil {
		t.Fatalf("SendChunk: %v", err)
	}
	if err := call.SendChunk(context.Background(), []byte("two")); err != nil {
		t.Fatalf("SendChunk: %v", err)
	}
	if err := call.CloseSend(context.Background()); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	result, err := call.Response(context.Background())
	if err != nil {
		t.Fatalf("Response: %v", err)
	}
	var got []string
	if err := json.Unmarshal(result.Response.Payload, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("collected = %v", got)
	}

	blocked, err := client.Open(context.Background(), "block", "", nil, true)
	if err != nil {
		t.Fatalf("Open block: %v", err)
	}
	blocked.Cancel()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled")
	}
	result, err = blocked.Response(context.Background())
	if err != nil {
		t.Fatalf("canceled Response: %v", err)
	}
	if result.Response.Err == "" {
		t.Fatal("canceled handler returned no error")
	}
}

func TestInboundQueueRejectsWithoutDispatch(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := &wire.Server{Build: "server-test", InboundQueue: 1}
	server.RegisterControl("block", func(context.Context, wire.Request) (any, error) {
		calls.Add(1)
		close(entered)
		<-release
		return true, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, nil)
	first, err := client.Open(context.Background(), "block", "", nil, true)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	<-entered
	second, err := client.Open(context.Background(), "block", "", nil, true)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	result, err := second.Response(context.Background())
	if err != nil {
		t.Fatalf("second Response: %v", err)
	}
	if result.Outcome != wire.Rejected || calls.Load() != 1 {
		t.Fatalf("second outcome=%s calls=%d", result.Outcome, calls.Load())
	}
	close(release)
	if _, err := first.Response(context.Background()); err != nil {
		t.Fatalf("first Response: %v", err)
	}
}

func TestCloseIntakeRejectsOrdinaryRequestsOnAcceptedSession(t *testing.T) {
	var calls atomic.Int32
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("mutate", func(context.Context, wire.Request) (any, error) {
		calls.Add(1)
		return true, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	client := newClient(t, running, nil)
	if err := server.CloseIntake(); err != nil {
		t.Fatalf("CloseIntake: %v", err)
	}
	result, err := client.Call(context.Background(), "mutate", "", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if result.Outcome != wire.Rejected || calls.Load() != 0 {
		t.Fatalf("outcome=%s calls=%d", result.Outcome, calls.Load())
	}
}

func TestMismatchedBuildRejectsMutationBeforeAdmission(t *testing.T) {
	var calls atomic.Int32
	var admissions atomic.Int32
	server := &wire.Server{Build: "new-build"}
	server.RegisterControl("mutate", func(context.Context, wire.Request) (any, error) {
		calls.Add(1)
		return true, nil
	})
	running := startSessionServer(t, server, func() (func(), error) {
		admissions.Add(1)
		return func() {}, nil
	})
	client := newClient(t, running, func(config *wire.ClientConfig) { config.Build = "old-build" })
	result, err := client.Call(context.Background(), "mutate", "", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if result.Outcome != wire.Rejected || result.Response.Reason != wire.ErrBuildMismatch.Error() {
		t.Fatalf("result = %#v", result)
	}
	if calls.Load() != 0 || admissions.Load() != 0 {
		t.Fatalf("calls=%d admissions=%d", calls.Load(), admissions.Load())
	}
}

func TestMaxSessionsBoundsStalledHandshakes(t *testing.T) {
	server := &wire.Server{Build: "server-test", MaxSessions: 1, HandshakeTimeout: time.Second}
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	stalled, err := net.Dial("unix", running.path)
	if err != nil {
		t.Fatalf("Dial stalled: %v", err)
	}
	defer stalled.Close()
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = wire.NewClient(ctx, wire.ClientConfig{Dial: wire.UnixDialer(running.path), Build: "server-test"})
	if err == nil {
		t.Fatal("second session unexpectedly passed a saturated handshake bound")
	}
}

func TestDuplicateRequestIDNeverDispatchesTwice(t *testing.T) {
	var calls atomic.Int32
	server := &wire.Server{Build: "server-test"}
	server.RegisterControl("mutate", func(context.Context, wire.Request) (any, error) {
		calls.Add(1)
		return true, nil
	})
	var inflight atomic.Int32
	running := startSessionServer(t, server, admitAll(&inflight))
	conn, err := net.Dial("unix", running.path)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	codec := wire.NewCodec(conn)
	identity, err := json.Marshal(wire.BuildIdentity{Protocol: wire.ProtocolVersion, Build: "server-test"})
	if err != nil {
		t.Fatalf("Marshal identity: %v", err)
	}
	if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameHello, Flags: wire.FlagEnd, Payload: identity}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := codec.ReadFrame(); err != nil {
		t.Fatalf("read hello ack: %v", err)
	}
	request := wire.Frame{Kind: wire.FrameRequest, Flags: wire.FlagEnd, ID: 1, Op: "mutate"}
	if err := codec.WriteFrame(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := codec.WriteFrame(request); err != nil {
		t.Fatalf("write duplicate: %v", err)
	}
	codec.ReadTimeout = 200 * time.Millisecond
	_, err = codec.ReadFrame()
	if err == nil {
		t.Fatal("duplicate request did not terminate the session")
	}
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}
