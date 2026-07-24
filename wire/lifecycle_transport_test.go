package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

var errLifecycleResponseWrite = errors.New("response write failed")

type frameWriteBarrier struct {
	net.Conn
	kind    FrameKind
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *frameWriteBarrier) Write(payload []byte) (int, error) {
	if framePacketKind(payload) == c.kind {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.Conn.Write(payload)
}

type frameWriteFailure struct {
	net.Conn
	kind FrameKind
	err  error
}

func (c *frameWriteFailure) Write(payload []byte) (int, error) {
	if framePacketKind(payload) == c.kind {
		return 0, c.err
	}
	return c.Conn.Write(payload)
}

func framePacketKind(packet []byte) FrameKind {
	if len(packet) <= 10 {
		return 0
	}
	return FrameKind(packet[10])
}

func TestLifecycleFramesBypassUnreadProductEventsAndStayLatestOnly(t *testing.T) {
	sessions := make(chan *session, 1)
	server := &Server{WireBuild: "session-test"}
	server.RegisterControl("capture", func(ctx context.Context, request Request) (any, error) {
		sessions <- request.Session.s
		if err := request.Session.PushEvent(ctx, Event{Topic: "product", Payload: []byte("blocked")}); err != nil {
			return nil, err
		}
		return true, nil
	})
	server.RegisterControl("echo", func(context.Context, Request) (any, error) { return true, nil })
	client, done := startLifecycleTransportSession(t, server, nil, func(config *ClientConfig) {
		config.EventQueue = 1
	})

	if _, err := client.Call(context.Background(), "capture", "", nil); err != nil {
		t.Fatalf("capture: %v", err)
	}
	sess := <-sessions
	if _, err := sess.offerLifecycle([]byte("one"), false); err != nil {
		t.Fatalf("offer one: %v", err)
	}
	if _, err := sess.offerLifecycle([]byte("two"), false); err != nil {
		t.Fatalf("offer two: %v", err)
	}
	awaitLifecycleValue(t, client, "two")
	if result, err := client.Call(context.Background(), "echo", "", nil); err != nil || result.Outcome != Delivered {
		t.Fatalf("echo behind unread product event = %#v, %v", result, err)
	}
	select {
	case event := <-client.Events():
		if event.Topic != "product" || string(event.Payload) != "blocked" {
			t.Fatalf("product event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("product event was lost")
	}
	if _, err := sess.offerLifecycle([]byte("unread"), false); err != nil {
		t.Fatalf("offer unread lifecycle: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close with unread lifecycle stream: %v", err)
	}
	awaitLifecycleTransportServer(t, done)
}

func TestLifecycleServerLaneCoalescesPendingUpdates(t *testing.T) {
	lane := newLatestWriteLane()
	for _, value := range []string{"one", "two", "three"} {
		if _, err := lane.offer([]byte(value), false); err != nil {
			t.Fatalf("offer %q: %v", value, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	generation, payload, err := lane.next(ctx)
	if err != nil || string(payload) != "three" {
		t.Fatalf("next = %q, %v; want three", payload, err)
	}
	lane.complete(generation, nil)
}

func TestLifecycleServerLaneSealsTerminalBeforeCoalescing(t *testing.T) {
	lane := newLatestWriteLane()
	generation, err := lane.offer([]byte("failed"), true)
	if err != nil {
		t.Fatalf("offer terminal: %v", err)
	}
	if _, err := lane.offer([]byte("ready"), false); !errors.Is(err, errStreamSealed) {
		t.Fatalf("offer after terminal = %v, want sealed", err)
	}
	duplicateGeneration, err := lane.offer([]byte("failed"), true)
	if err != nil || duplicateGeneration != generation {
		t.Fatalf("duplicate terminal = %d, %v; want generation %d", duplicateGeneration, err, generation)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	gotGeneration, payload, err := lane.next(ctx)
	if err != nil || gotGeneration != generation || string(payload) != "failed" {
		t.Fatalf("terminal next = %d/%q, %v", gotGeneration, payload, err)
	}
}

func TestLifecycleClientValidatesEveryFrameAndSealsTerminal(t *testing.T) {
	var validated []string
	client := &Client{
		lifecycle: newLatestStream[[]byte](),
		lifecycleValidator: func(payload []byte) (bool, error) {
			validated = append(validated, string(payload))
			return string(payload) == "failed", nil
		},
	}
	frame := func(payload string) Frame {
		return Frame{Kind: FrameLifecycle, Flags: FlagEnd, Payload: []byte(payload)}
	}
	if err := client.receiveLifecycle(frame("failed")); err != nil {
		t.Fatalf("receive terminal: %v", err)
	}
	if err := client.receiveLifecycle(frame("failed")); err != nil {
		t.Fatalf("receive duplicate terminal: %v", err)
	}
	if err := client.receiveLifecycle(frame("ready")); !errors.Is(err, errStreamSealed) {
		t.Fatalf("receive after terminal = %v, want sealed", err)
	}
	if got := validated; len(got) != 3 || got[0] != "failed" || got[1] != "failed" || got[2] != "ready" {
		t.Fatalf("validated = %v", got)
	}
	payload, err := client.nextLifecycle(context.Background())
	if err != nil || string(payload) != "failed" {
		t.Fatalf("terminal lifecycle = %q, %v", payload, err)
	}
	if payload, err, ok := client.tryLifecycle(); ok || err != nil || payload != nil {
		t.Fatalf("duplicate terminal was redelivered: %q, %v, %t", payload, err, ok)
	}
}

func TestLifecycleFrameBeforeValidatorFailsWithoutDelivery(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	server := &Server{WireBuild: "session-test"}
	serverDone := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		codec := NewCodec(serverConn)
		if _, _, err := server.serverHandshake(codec); err != nil {
			serverDone <- err
			return
		}
		if _, err := codec.ReadFrame(); err != nil {
			serverDone <- err
			return
		}
		serverDone <- codec.WriteFrame(Frame{
			Kind: FrameLifecycle, Flags: FlagEnd, Payload: []byte("early"),
		})
	}()
	client, err := NewClient(context.Background(), ClientConfig{
		WireBuild: "session-test",
		Dial:      func(context.Context) (net.Conn, error) { return clientConn, nil },
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload, err := client.nextLifecycle(ctx)
	if payload != nil || err == nil || !bytes.Contains([]byte(err.Error()), []byte("lifecycle validator is not installed")) {
		t.Fatalf("nextLifecycle = %q, %v; want no payload and missing-validator failure", payload, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("raw lifecycle server: %v", err)
	}
}

func TestLifecycleOfferDoesNotWaitForLocalWriteOrPeerAck(t *testing.T) {
	sessions := make(chan *session, 1)
	server := &Server{WireBuild: "session-test"}
	server.RegisterControl("capture", func(_ context.Context, request Request) (any, error) {
		sessions <- request.Session.s
		return true, nil
	})
	clientConn, rawServerConn := net.Pipe()
	barrier := &frameWriteBarrier{
		Conn: rawServerConn, kind: FrameLifecycle, entered: make(chan struct{}), release: make(chan struct{}),
	}
	done := serveLifecycleTransportSession(t, server, barrier)
	codec, identity := openLifecycleRawClient(t, clientConn)
	writeLifecycleRawCall(t, codec, identity, 1, "capture")
	sess := <-sessions
	receipt, err := sess.offerLifecycle([]byte("terminal"), true)
	if err != nil {
		t.Fatalf("offer lifecycle: %v", err)
	}
	settled := make(chan error, 1)
	go func() { settled <- receipt.wait(context.Background()) }()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("lifecycle writer did not reach local barrier")
	}
	select {
	case err := <-settled:
		t.Fatalf("local-write receipt completed before write: %v", err)
	default:
	}
	read := make(chan Frame, 1)
	readErr := make(chan error, 1)
	go func() {
		frame, err := codec.ReadFrame()
		if err != nil {
			readErr <- err
			return
		}
		read <- frame
	}()
	close(barrier.release)
	select {
	case frame := <-read:
		if frame.Kind != FrameLifecycle || frame.Op != "" || string(frame.Payload) != "terminal" {
			t.Fatalf("lifecycle frame = %#v", frame)
		}
	case err := <-readErr:
		t.Fatalf("read lifecycle: %v", err)
	case <-time.After(time.Second):
		t.Fatal("lifecycle frame was not readable")
	}
	if err := <-settled; err != nil {
		t.Fatalf("local-write receipt: %v", err)
	}
	closeLifecycleRawClient(t, codec)
	awaitLifecycleTransportServer(t, done)
}

func TestLifecycleWriterHasReservedPriorityCapacity(t *testing.T) {
	writer, reader := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		server: &Server{}, conn: writer, codec: NewCodec(writer), ctx: ctx, cancel: cancel,
		outbound:     make(chan sessionOutbound, 2),
		eventCredits: newCreditWindow(), lifecycleLane: newLatestWriteLane(),
		requestsDone: make(chan struct{}), writerDone: make(chan struct{}), disconnected: make(chan struct{}),
	}
	sess.outbound <- sessionOutbound{frame: Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "one"}}
	sess.outbound <- sessionOutbound{frame: Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "two"}}
	if _, err := sess.lifecycleLane.offer([]byte("ready"), false); err != nil {
		t.Fatalf("offer lifecycle: %v", err)
	}
	sess.writerWG.Add(1)
	go sess.writeLoop()
	frame, err := NewCodec(reader).ReadFrame()
	if err != nil || frame.Kind != FrameLifecycle {
		t.Fatalf("first server frame = %#v, %v; want lifecycle", frame, err)
	}
	cancel()
	_ = reader.Close()
	close(sess.requestsDone)
	sess.writerWG.Wait()
}

func TestLifecycleWriterTakesLatestAtActualWriteStart(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	barrier := &frameWriteBarrier{
		Conn: serverConn, kind: FrameEvent, entered: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		server: &Server{}, conn: barrier, codec: NewCodec(barrier), ctx: ctx, cancel: cancel,
		outbound: make(chan sessionOutbound, 1), eventCredits: newCreditWindow(),
		lifecycleLane: newLatestWriteLane(), requestsDone: make(chan struct{}),
		writerDone: make(chan struct{}), disconnected: make(chan struct{}),
	}
	sess.outbound <- sessionOutbound{frame: Frame{Kind: FrameEvent, Flags: FlagEnd, Op: "blocked"}}
	sess.writerWG.Add(1)
	go sess.writeLoop()
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("ordinary writer did not reach barrier")
	}
	if _, err := sess.offerLifecycle([]byte("one"), false); err != nil {
		t.Fatalf("offer one: %v", err)
	}
	receipt, err := sess.offerLifecycle([]byte("two"), false)
	if err != nil {
		t.Fatalf("offer two: %v", err)
	}
	codec := NewCodec(clientConn)
	frames := make(chan []Frame, 1)
	readErr := make(chan error, 1)
	go func() {
		first, err := codec.ReadFrame()
		if err != nil {
			readErr <- err
			return
		}
		second, err := codec.ReadFrame()
		if err != nil {
			readErr <- err
			return
		}
		frames <- []Frame{first, second}
	}()
	close(barrier.release)
	select {
	case got := <-frames:
		if got[0].Kind != FrameEvent || got[1].Kind != FrameLifecycle || string(got[1].Payload) != "two" {
			t.Fatalf("frames = %#v; want blocked event then latest lifecycle two", got)
		}
	case err := <-readErr:
		t.Fatalf("read frames: %v", err)
	case <-time.After(time.Second):
		t.Fatal("writer did not emit latest lifecycle")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := receipt.wait(waitCtx); err != nil {
		t.Fatalf("latest write receipt: %v", err)
	}
	cancel()
	_ = clientConn.Close()
	close(sess.requestsDone)
	sess.writerWG.Wait()
}

func TestLifecycleNonreaderClosesOnlyItsSession(t *testing.T) {
	sessions := make(chan *session, 1)
	server := &Server{WireBuild: "session-test", WriteTimeout: 25 * time.Millisecond}
	server.RegisterControl("capture", func(_ context.Context, request Request) (any, error) {
		if request.Tenant == "bad" {
			sessions <- request.Session.s
		}
		return true, nil
	})
	server.RegisterControl("echo", func(context.Context, Request) (any, error) { return true, nil })
	socketDir, err := os.MkdirTemp("/tmp", "daemonkit-lifecycle-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	listener, err := net.Listen("unix", socketDir+"/wire.sock")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener, func() error { close(ready); return nil }, allowSession, allowSession)
	}()
	<-ready

	badConn, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial bad session: %v", err)
	}
	badCodec, identity := openLifecycleRawClient(t, badConn)
	writeLifecycleRawCallTenant(t, badCodec, identity, 1, "capture", "bad")
	badSession := <-sessions

	good, err := NewClient(context.Background(), ClientConfig{
		WireBuild: "session-test", Dial: UnixDialer(listener.Addr().String()),
	})
	if err != nil {
		t.Fatalf("dial good session: %v", err)
	}
	if result, err := good.Call(context.Background(), "echo", "", nil); err != nil || result.Outcome != Delivered {
		t.Fatalf("initial good echo = %#v, %v", result, err)
	}

	receipt, err := badSession.offerLifecycle(bytes.Repeat([]byte{'x'}, 2<<20), false)
	if err != nil {
		t.Fatalf("offer lifecycle: %v", err)
	}
	select {
	case <-badSession.done:
	case <-time.After(time.Second):
		t.Fatal("nonreader lifecycle session remained wedged")
	}
	if err := receipt.wait(context.Background()); err == nil {
		t.Fatal("nonreader local-write receipt succeeded")
	}
	if result, err := good.Call(context.Background(), "echo", "", nil); err != nil || result.Outcome != Delivered {
		t.Fatalf("good echo after bad-session timeout = %#v, %v", result, err)
	}
	if err := good.Close(); err != nil {
		t.Fatalf("close good session: %v", err)
	}
	_ = badConn.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not join")
	}
}

func TestResponseWriteReceiptStartsAfterTerminalBytes(t *testing.T) {
	receipts := make(chan (<-chan error), 1)
	server := &Server{WireBuild: "session-test"}
	server.RegisterControl("subscribe", func(_ context.Context, request Request) (any, error) {
		receipt, err := request.Session.s.responseWritten(request.ID)
		if err != nil {
			return nil, err
		}
		receipts <- receipt
		return true, nil
	})
	barrier := &frameWriteBarrier{
		kind: FrameResponse, entered: make(chan struct{}), release: make(chan struct{}),
	}
	client, done := startLifecycleTransportSession(t, server, func(conn net.Conn) net.Conn {
		barrier.Conn = conn
		return barrier
	}, nil)
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "subscribe", "", nil)
		callDone <- err
	}()
	receipt := <-receipts
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("terminal writer did not reach barrier")
	}
	select {
	case err := <-receipt:
		t.Fatalf("write receipt completed before response bytes: %v", err)
	default:
	}
	close(barrier.release)
	if err := <-receipt; err != nil {
		t.Fatalf("terminal write receipt: %v", err)
	}
	if err := <-callDone; err != nil {
		t.Fatalf("subscribe call: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	awaitLifecycleTransportServer(t, done)
}

func TestResponseWriteReceiptPreservesExactFailure(t *testing.T) {
	receipts := make(chan (<-chan error), 1)
	server := &Server{WireBuild: "session-test"}
	server.RegisterControl("subscribe", func(_ context.Context, request Request) (any, error) {
		receipt, err := request.Session.s.responseWritten(request.ID)
		if err != nil {
			return nil, err
		}
		receipts <- receipt
		return true, nil
	})
	client, done := startLifecycleTransportSession(t, server, func(conn net.Conn) net.Conn {
		return &frameWriteFailure{Conn: conn, kind: FrameResponse, err: errLifecycleResponseWrite}
	}, nil)
	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), "subscribe", "", nil)
		callDone <- err
	}()
	receipt := <-receipts
	select {
	case err := <-receipt:
		if !errors.Is(err, errLifecycleResponseWrite) {
			t.Fatalf("write receipt = %v, want exact response write failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed response write leaked receipt waiter")
	}
	if err := <-callDone; err == nil {
		t.Fatal("call succeeded after response write failure")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ServeSession did not settle after response write failure")
	}
	_ = client.Abort(nil)
}

func TestLatestLifecycleStreamReturnsBufferedValueWithClose(t *testing.T) {
	stream := newLatestStream[[]byte]()
	if err := stream.offer([]byte("one")); err != nil {
		t.Fatalf("offer one: %v", err)
	}
	if err := stream.offer([]byte("two")); err != nil {
		t.Fatalf("offer two: %v", err)
	}
	stream.close()
	if value, err := stream.next(context.Background()); !errors.Is(err, errStreamClosed) || string(value) != "two" {
		t.Fatalf("next after close = %q, %v; want latest plus closed", value, err)
	}
	if value, err, ok := stream.try(); !ok || !errors.Is(err, errStreamClosed) || value != nil {
		t.Fatalf("try after retained value = %q, %v, %t; want closed", value, err, ok)
	}
}

func TestLatestLifecycleStreamCloseWakeRechecksRetainedPayload(t *testing.T) {
	stream := newLatestStream[[]byte]()
	if payload, err, ok := stream.try(); ok || err != nil || payload != nil {
		t.Fatalf("initial try = %q, %v, %t; want empty", payload, err, ok)
	}
	if err := stream.offerTerminalExact([]byte("failed"), bytes.Equal); err != nil {
		t.Fatalf("offer terminal: %v", err)
	}
	stream.close()
	<-stream.notify
	if err := stream.wait(context.Background()); err != nil {
		t.Fatalf("close wake: %v", err)
	}
	payload, err, ok := stream.try()
	if !ok || string(payload) != "failed" || !errors.Is(err, errStreamClosed) {
		t.Fatalf("try after close wake = %q, %v, %t; want retained terminal plus close", payload, err, ok)
	}
}

func TestClientDisconnectReturnsBufferedLifecycleWithCause(t *testing.T) {
	cause := errors.New("exact transport failure")
	clientConn, peerConn := net.Pipe()
	defer peerConn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		conn: clientConn, ctx: ctx, cancel: cancel,
		events: newBoundedStream[Event](1), lifecycle: newLatestStream[[]byte](),
		pending: make(map[uint64]*ClientCall),
	}
	if err := client.lifecycle.offer([]byte("stale-ready")); err != nil {
		t.Fatalf("offer lifecycle: %v", err)
	}
	client.fail(cause)
	if payload, err := client.nextLifecycle(context.Background()); !errors.Is(err, cause) || string(payload) != "stale-ready" {
		t.Fatalf("nextLifecycle after fail = %q, %v; want payload plus exact failure", payload, err)
	}
}

func TestClientTryLifecyclePreservesPayloadAndTerminalCause(t *testing.T) {
	cause := errors.New("exact transport failure")
	client := &Client{lifecycle: newLatestStream[[]byte]()}
	if payload, err, ok := client.tryLifecycle(); ok || err != nil || payload != nil {
		t.Fatalf("empty tryLifecycle = %q, %v, %t", payload, err, ok)
	}
	if err := client.lifecycle.offer([]byte("terminal")); err != nil {
		t.Fatalf("offer lifecycle: %v", err)
	}
	client.mu.Lock()
	client.err = cause
	client.mu.Unlock()
	client.lifecycle.close()
	if payload, err, ok := client.tryLifecycle(); !ok || !errors.Is(err, cause) || string(payload) != "terminal" {
		t.Fatalf("tryLifecycle = %q, %v, %t; want payload plus exact failure", payload, err, ok)
	}
}

func startLifecycleTransportSession(
	t *testing.T,
	server *Server,
	wrap func(net.Conn) net.Conn,
	configure func(*ClientConfig),
) (*Client, <-chan error) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	if wrap != nil {
		serverConn = wrap(serverConn)
	}
	done := serveLifecycleTransportSession(t, server, serverConn)
	config := ClientConfig{
		WireBuild: "session-test",
		Dial:      func(context.Context) (net.Conn, error) { return clientConn, nil },
	}
	if configure != nil {
		configure(&config)
	}
	client, err := newLifecycleClient(context.Background(), config, func([]byte) (bool, error) {
		return false, nil
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client, done
}

func serveLifecycleTransportSession(t *testing.T, server *Server, conn net.Conn) <-chan error {
	t.Helper()
	identity := currentSessionIdentity(t)
	done := make(chan error, 1)
	go func() {
		done <- server.ServeSession(
			context.Background(), conn, identity,
			func() error { return nil }, allowSession, allowSession,
		)
	}()
	return done
}

func awaitLifecycleTransportServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeSession: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeSession did not join")
	}
}

func openLifecycleRawClient(t *testing.T, conn net.Conn) (*Codec, WireIdentity) {
	t.Helper()
	codec := NewCodec(conn)
	payload, err := json.Marshal(WireIdentity{Protocol: ProtocolVersion, WireBuild: "session-test"})
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	var identity WireIdentity
	if err := json.Unmarshal(frame.Payload, &identity); err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	return codec, identity
}

func writeLifecycleRawCall(t *testing.T, codec *Codec, identity WireIdentity, id uint64, op Op) {
	writeLifecycleRawCallTenant(t, codec, identity, id, op, "")
}

func writeLifecycleRawCallTenant(
	t *testing.T,
	codec *Codec,
	identity WireIdentity,
	id uint64,
	op Op,
	tenant string,
) {
	t.Helper()
	if err := codec.WriteFrame(Frame{Kind: FrameRequest, Flags: FlagEnd, ID: id, Op: op, Tenant: tenant}); err != nil {
		t.Fatalf("write request: %v", err)
	}
	var frame Frame
	for {
		var err error
		frame, err = codec.ReadFrame()
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if frame.Kind != FrameWindow {
			break
		}
	}
	if frame.Kind != FrameResponse || frame.ID != id {
		t.Fatalf("response = %#v", frame)
	}
	if err := codec.WriteFrame(Frame{Kind: FrameAck, Flags: FlagEnd, ID: id, Payload: identity.Session}); err != nil {
		t.Fatalf("write acknowledgement: %v", err)
	}
}

func closeLifecycleRawClient(t *testing.T, codec *Codec) {
	t.Helper()
	if err := codec.WriteFrame(Frame{Kind: FrameGoAway, Flags: FlagEnd}); err != nil {
		t.Fatalf("write go-away: %v", err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read go-away: %v", err)
	}
	if frame.Kind != FrameGoAway {
		t.Fatalf("go-away acknowledgement = %#v", frame)
	}
}

func awaitLifecycleValue(t *testing.T, client *Client, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		payload, err := client.nextLifecycle(ctx)
		if err != nil {
			t.Fatalf("next lifecycle: %v", err)
		}
		if string(payload) == want {
			return
		}
	}
}
