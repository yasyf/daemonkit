package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type stubRuntime struct{ phase Phase }

func (r stubRuntime) Handle(context.Context, Request) (any, error) {
	return json.RawMessage("{}"), nil
}

func (r stubRuntime) Phase() PhaseSnapshot { return PhaseSnapshot{Sequence: 1, Phase: r.phase} }

func (r stubRuntime) WaitPhase(ctx context.Context, after uint64) (PhaseSnapshot, error) {
	if after < 1 {
		return r.Phase(), nil
	}
	<-ctx.Done()
	return PhaseSnapshot{}, ctx.Err()
}

func (stubRuntime) Drain() {}

func testSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dkw-")
	if err != nil {
		t.Fatalf("mkdir socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func testPair(t *testing.T) (client, server *net.UnixConn) {
	t.Helper()
	sock := filepath.Join(testSocketDir(t), "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		conn, err := ln.Accept()
		ch <- accepted{conn, err}
	}()
	dialed, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	a := <-ch
	if a.err != nil {
		t.Fatalf("accept: %v", a.err)
	}
	client = dialed.(*net.UnixConn)
	server = a.conn.(*net.UnixConn)
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

func mustServer(t *testing.T, rt Runtime, cfg Config) *Server {
	t.Helper()
	if cfg.Schemas == nil {
		cfg.Schemas = Schemas{"test.v1"}
	}
	server, err := NewServer(rt, cfg)
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}
	return server
}

func startServing(t *testing.T, server *Server) string {
	t.Helper()
	sock := filepath.Join(testSocketDir(t), "srv")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	awaitServing(t, server)
	return sock
}

// awaitServing blocks until Serve has installed serveCtx. Serve does that on
// the goroutine startServing just launched, and an adopt racing ahead of it
// reads the nil serveCtx as a draining server, so a loaded machine fails the
// refusal a test was asserting.
func awaitServing(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		server.mu.Lock()
		serving := server.serveCtx != nil
		server.mu.Unlock()
		if serving {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve() never installed serveCtx")
		}
		time.Sleep(time.Millisecond)
	}
}

func writeHello(t *testing.T, codec *Codec, hello helloIdentity) {
	t.Helper()
	payload, err := json.Marshal(hello)
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
}

func readAck(t *testing.T, codec *Codec) helloAck {
	t.Helper()
	frame, err := codec.ReadFrame()
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if frame.Kind != FrameHelloAck {
		t.Fatalf("ack kind = %d, want FrameHelloAck", frame.Kind)
	}
	var ack helloAck
	if err := decodeStrict(frame.Payload, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	return ack
}

func TestSilentConnectionDoesNotBlockOtherAccepts(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	sock := startServing(t, server)

	silent, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial silent: %v", err)
	}
	defer silent.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := NewClient(ctx, ClientConfig{Dial: UnixDialer(sock), Schema: "test.v1"})
	if err != nil {
		t.Fatalf("NewClient() behind a silent connection = %v", err)
	}
	defer func() { _ = client.Abort(nil) }()
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Fatalf("handshake behind a silent connection took %v", elapsed)
	}
}

func TestPendingCapSaturationDropsWithoutWrite(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{
		Concurrency:   1,
		HandshakeRead: 500 * time.Millisecond,
	})
	sock := startServing(t, server)

	pendingCap := cap(server.pendingSlots)
	if pendingCap != 4 {
		t.Fatalf("pending cap = %d, want 2*Concurrency+2 = 4", pendingCap)
	}
	for range pendingCap {
		conn, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatalf("dial holder: %v", err)
		}
		defer conn.Close()
	}
	time.Sleep(100 * time.Millisecond)

	over, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial over-cap: %v", err)
	}
	defer over.Close()
	if err := over.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := over.Read(make([]byte, 1))
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("over-cap connection Read = (%d, %v), want (0, EOF drop with no write)", n, err)
	}

	time.Sleep(700 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := NewClient(ctx, ClientConfig{Dial: UnixDialer(sock), Schema: "test.v1"})
	if err != nil {
		t.Fatalf("NewClient() after pending slots recycled = %v", err)
	}
	_ = client.Abort(nil)
}

func TestRejectedHelloConsumesNoSlot(t *testing.T) {
	tests := []struct {
		name    string
		hello   helloIdentity
		wantErr error
	}{
		{
			name:    "a nonce outside a spawned session",
			hello:   helloIdentity{Protocol: ProtocolVersion, Schema: "test.v1", Nonce: make([]byte, 32)},
			wantErr: ErrHandshake,
		},
		{
			name:    "old-era protocol in the hello payload",
			hello:   helloIdentity{Protocol: 1, Schema: "test.v1"},
			wantErr: ErrProtocolVersion,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
			clientConn, serverConn := testPair(t)
			done := make(chan error, 1)
			go func() { done <- server.startConnection(context.Background(), serverConn) }()

			codec := NewCodec(clientConn)
			writeHello(t, codec, tt.hello)
			ack := readAck(t, codec)
			if !ack.Rejected || ack.Code != ResponseCodeHandshake {
				t.Fatalf("ack = %+v, want rejection code %q", ack, ResponseCodeHandshake)
			}
			if err := <-done; !errors.Is(err, tt.wantErr) {
				t.Fatalf("startConnection() = %v, want %v", err, tt.wantErr)
			}
			if got := len(server.sessionSlots); got != 0 {
				t.Fatalf("session slots held after rejection = %d, want 0", got)
			}
		})
	}
}

func TestAdmittedPeerHoldsOneSlotUntilItLeaves(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()

	codec := NewCodec(clientConn)
	writeHello(t, codec, helloIdentity{Protocol: ProtocolVersion, Schema: "test.v1"})
	ack := readAck(t, codec)
	if ack.Rejected {
		t.Fatalf("ack rejected: %+v", ack)
	}
	if err := <-done; err != nil {
		t.Fatalf("startConnection() = %v", err)
	}
	if got := len(server.sessionSlots); got != 1 {
		t.Fatalf("session slots after admission = %d, want 1", got)
	}
	_ = clientConn.Close()
	server.sessionWG.Wait()
	if got := len(server.sessionSlots); got != 0 {
		t.Fatalf("session slots after disconnect = %d, want 0", got)
	}
}

func TestGoAwayAcknowledgementFollowsTheSlotRelease(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	sock := startServing(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := NewClient(ctx, ClientConfig{
		Dial:   UnixDialer(sock),
		Schema: "test.v1",
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	if _, err := client.Health(ctx); err != nil {
		t.Fatalf("Health() = %v", err)
	}
	if got := len(server.sessionSlots); got != 1 {
		t.Fatalf("session slots after admission = %d, want 1", got)
	}

	server.mu.Lock()
	closeErr := client.Close(ctx)
	held := len(server.sessionSlots)
	server.mu.Unlock()
	if closeErr != nil {
		t.Fatalf("Close() = %v", closeErr)
	}
	if held != 0 {
		t.Fatalf("session slots when Close() returned = %d, want 0: the next attach would be refused for capacity", held)
	}
}

func frozenDrainPreamble(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "ci", "mixedera", "testdata", "frozen", "drain-preamble.hex"))
	if err != nil {
		t.Fatalf("read frozen preamble: %v", err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("decode frozen preamble: %v", err)
	}
	return decoded
}

func TestDrainingServerEmitsFrozenPreambleInsteadOfAck(t *testing.T) {
	frozen := frozenDrainPreamble(t)
	if !bytes.Equal(frozen, drainPreamble[:]) {
		t.Fatalf("frozen preamble = %x, code emits %x", frozen, drainPreamble)
	}
	server := mustServer(t, stubRuntime{phase: PhaseDraining}, Config{})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()

	codec := NewCodec(clientConn)
	writeHello(t, codec, helloIdentity{Protocol: ProtocolVersion, Schema: "test.v1"})
	if err := <-done; !errors.Is(err, ErrDraining) {
		t.Fatalf("startConnection() = %v, want ErrDraining", err)
	}
	got, err := io.ReadAll(clientConn)
	if err != nil {
		t.Fatalf("read preamble: %v", err)
	}
	if !bytes.Equal(got, frozen) {
		t.Fatalf("draining server emitted %x, want exactly the frozen preamble %x", got, frozen)
	}
}

func TestClientHandshakeSeesTypedDrainOnPreamble(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseDraining}, Config{})
	clientConn, serverConn := testPair(t)
	go func() { _ = server.startConnection(context.Background(), serverConn) }()

	codec := NewCodec(clientConn)
	_, err := clientHandshake(codec, helloIdentity{Protocol: ProtocolVersion, Schema: "test.v1"})
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("clientHandshake() against a draining server = %v, want ErrDraining", err)
	}
}

func encodeEraFrame(t *testing.T, version uint16, kind FrameKind, payload []byte) []byte {
	t.Helper()
	body := make([]byte, frameHeaderSize+len(payload))
	copy(body[:4], []byte("DKS1"))
	binary.BigEndian.PutUint16(body[4:6], version)
	body[6] = byte(kind)
	body[7] = byte(FlagEnd)
	copy(body[frameHeaderSize:], payload)
	packet := make([]byte, framePrefixSize+len(body))
	binary.BigEndian.PutUint32(packet[:framePrefixSize], uint32(len(body)))
	copy(packet[framePrefixSize:], body)
	return packet
}

func TestOldEraPeersGetProtocolMismatch(t *testing.T) {
	t.Run("server rejects a protocol-1 framed hello", func(t *testing.T) {
		server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
		clientConn, serverConn := testPair(t)
		done := make(chan error, 1)
		go func() { done <- server.startConnection(context.Background(), serverConn) }()
		v1Hello := encodeEraFrame(t, 1, FrameHello, []byte(`{"protocol":1,"wire_build":"old","role":"unprotected"}`))
		if _, err := clientConn.Write(v1Hello); err != nil {
			t.Fatalf("write v1 hello: %v", err)
		}
		if err := <-done; !errors.Is(err, ErrProtocolVersion) {
			t.Fatalf("startConnection() = %v, want ErrProtocolVersion", err)
		}
	})
	t.Run("server types a protocol-1 hello payload as a mismatch", func(t *testing.T) {
		server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
		clientConn, serverConn := testPair(t)
		done := make(chan error, 1)
		go func() { done <- server.startConnection(context.Background(), serverConn) }()
		codec := NewCodec(clientConn)
		writeHello(t, codec, helloIdentity{Protocol: 1, Schema: "test.v1"})
		err := <-done
		var mismatch *ProtocolMismatchError
		if !errors.As(err, &mismatch) || mismatch.Theirs != 1 || mismatch.Ours != ProtocolVersion {
			t.Fatalf("startConnection() = %v, want ProtocolMismatchError{1, %d}", err, ProtocolVersion)
		}
	})
	t.Run("client rejects a protocol-1 framed ack", func(t *testing.T) {
		clientConn, serverConn := testPair(t)
		go func() {
			buf := make([]byte, 1024)
			_, _ = serverConn.Read(buf)
			v1Ack := encodeEraFrame(t, 1, FrameHelloAck, []byte(`{"protocol":1,"wire_build":"old"}`))
			_, _ = serverConn.Write(v1Ack)
		}()
		codec := NewCodec(clientConn)
		_, err := clientHandshake(codec, helloIdentity{Protocol: ProtocolVersion, Schema: "test.v1"})
		if !errors.Is(err, ErrProtocolVersion) {
			t.Fatalf("clientHandshake() against an old-era server = %v, want ErrProtocolVersion", err)
		}
	})
}

func TestPreviousProtocolControlLaneReadsHealthOnly(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{Serving: Serving{PID: 4242, Build: "b3", Generation: 7}})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()
	if _, err := clientConn.Write(encodeEraFrame(t, 2, FrameHello, []byte(`{"protocol":2,"lane":"control"}`))); err != nil {
		t.Fatalf("write protocol-2 hello: %v", err)
	}
	codec := NewCodec(clientConn)
	codec.legacy.Store(true)
	if ack := readAck(t, codec); ack.Rejected || ack.Protocol != 2 {
		t.Fatalf("ack = %+v, want a protocol-2 acceptance", ack)
	}
	if err := <-done; err != nil {
		t.Fatalf("startConnection() = %v", err)
	}

	health := legacyCall(t, codec, 1, healthOp)
	var report HealthReport
	if err := json.Unmarshal(health.Payload, &report); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if report.Protocol != ProtocolVersion || report.PID != 4242 || report.Build != "b3" {
		t.Fatalf("health = %+v, want the serving daemon at protocol %d", report, ProtocolVersion)
	}
	if drain := legacyCall(t, codec, 2, drainControlOp); !drain.Rejected || drain.Code != ResponseCodePermissionDenied {
		t.Fatalf("drain = %+v, want permission_denied", drain)
	}
}

func TestPreviousProtocolBusinessLaneGetsProtocolMismatch(t *testing.T) {
	server := mustServer(t, stubRuntime{phase: PhaseReady}, Config{})
	clientConn, serverConn := testPair(t)
	done := make(chan error, 1)
	go func() { done <- server.startConnection(context.Background(), serverConn) }()
	hello := encodeEraFrame(t, 2, FrameHello, []byte(`{"protocol":2,"lane":"business","schema":"test.v1"}`))
	if _, err := clientConn.Write(hello); err != nil {
		t.Fatalf("write protocol-2 hello: %v", err)
	}
	var mismatch *ProtocolMismatchError
	if err := <-done; !errors.As(err, &mismatch) || mismatch.Theirs != 2 {
		t.Fatalf("startConnection() = %v, want ProtocolMismatchError{2, %d}", err, ProtocolVersion)
	}
	codec := NewCodec(clientConn)
	codec.legacy.Store(true)
	if ack := readAck(t, codec); !ack.Rejected || ack.Protocol != ProtocolVersion {
		t.Fatalf("ack = %+v, want a protocol-%d rejection", ack, ProtocolVersion)
	}
}

func legacyCall(t *testing.T, codec *Codec, id uint64, op Op) Response {
	t.Helper()
	if err := codec.WriteFrame(Frame{Kind: FrameRequest, Flags: FlagEnd, ID: id, Op: op}); err != nil {
		t.Fatalf("write %s: %v", op, err)
	}
	for {
		frame, err := codec.ReadFrame()
		if err != nil {
			t.Fatalf("read %s response: %v", op, err)
		}
		if frame.Kind != FrameResponse || frame.ID != id {
			continue
		}
		var response Response
		if err := json.Unmarshal(frame.Payload, &response); err != nil {
			t.Fatalf("decode %s response: %v", op, err)
		}
		return response
	}
}
