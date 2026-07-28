package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

const skewEcho = Op("test.build-skew")

func skewLadder(t *testing.T) Ladder {
	t.Helper()
	ladder, err := NewLadder(
		map[Op]time.Duration{skewEcho: 5 * time.Second},
		map[Op]time.Duration{skewEcho: 10 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	return ladder
}

func startSkewRuntime(t *testing.T, build string) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "daemonkit-build-skew-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	owner, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	reaper := func(name string) *proc.Reaper {
		return &proc.Reaper{
			Store: &proc.FileStore{Path: filepath.Join(dir, name)}, Generation: owner,
			Grace: 10 * time.Millisecond, Settlement: time.Second,
		}
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 1024, MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
	}, reaper("workers.db"))
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, reaper("children.db"))
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{WireBuild: build, Ladder: skewLadder(t), WriteTimeout: time.Second}
	server.Register(HandlerSpec{
		Op: skewEcho,
		Handler: func(_ context.Context, req Request) (any, error) {
			return req.WireBuild, nil
		},
	})
	socket := filepath.Join(dir, "runtime.sock")
	runtime, err := NewRuntime(RuntimeConfig{
		Socket: socket, RuntimeBuild: build, RuntimeProtocol: int(ProtocolVersion),
		Wire: server, TrustPolicy: roleTestPolicy(t, true),
		StopControlStore: &proc.FileStore{Path: filepath.Join(dir, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 2 * time.Second,
		Signals: make(chan os.Signal),
	})
	if err != nil {
		t.Fatal(err)
	}
	slot := daemon.NewPublicationSlot[string](runtime)
	activation, err := runtime.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := slot.Stage(activation, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close skew runtime: %v", err)
		}
	})
	return socket
}

func TestSessionSurvivesBuildSkew(t *testing.T) {
	const daemonBuild, clientBuild = "skew.daemon.v2", "skew.client.v1"
	socket := startSkewRuntime(t, daemonBuild)
	client, err := NewClient(t.Context(), ClientConfig{
		Dial: UnixDialer(socket), WireBuild: clientBuild, Role: trust.UnprotectedRole,
		Ladder: skewLadder(t), HandshakeTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("handshake across build skew: %v", err)
	}
	defer client.Close()

	identity := client.PeerWireIdentity()
	if identity.WireBuild != daemonBuild || identity.Protocol != ProtocolVersion {
		t.Fatalf("peer identity = %+v", identity)
	}
	result, err := client.Call(t.Context(), skewEcho, "", nil)
	if err != nil {
		t.Fatalf("call across build skew: %v", err)
	}
	if rejection := result.Rejection(); rejection != nil {
		t.Fatalf("call across build skew rejected: %v", rejection)
	}
	var observed string
	if err := json.Unmarshal(result.Response.Payload, &observed); err != nil {
		t.Fatal(err)
	}
	if observed != clientBuild {
		t.Fatalf("handler peer build = %q, want %q", observed, clientBuild)
	}
}

func TestReadClientHelloTypesProtocolMismatch(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	go func() {
		payload, err := json.Marshal(handshakeIdentity{
			Protocol: ProtocolVersion + 1, WireBuild: "peer.v9", Role: trust.UnprotectedRole,
		})
		if err == nil {
			_ = NewCodec(clientConn).WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload})
		}
	}()
	_, err := (&Server{WireBuild: "suite.v1"}).readClientHello(NewCodec(serverConn))
	var mismatch *ProtocolMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("readClientHello = %v, want *ProtocolMismatchError", err)
	}
	if mismatch.Theirs != ProtocolVersion+1 || mismatch.Ours != ProtocolVersion {
		t.Fatalf("mismatch = %+v", mismatch)
	}
	if !errors.Is(err, ErrProtocolVersion) {
		t.Fatalf("mismatch does not unwrap to ErrProtocolVersion: %v", err)
	}
}

func TestClientHandshakeTypesProtocolMismatchWithoutHanging(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	go func() {
		codec := NewCodec(serverConn)
		if _, err := codec.ReadFrame(); err != nil {
			return
		}
		payload, err := json.Marshal(handshakeAck{
			Protocol: ProtocolVersion + 1, WireBuild: "daemon.v9", Rejected: true,
			Code: ResponseCodePeerUntrusted, Reason: "another protocol",
		})
		if err == nil {
			_ = codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload})
		}
	}()
	done := make(chan error, 1)
	go func() {
		_, err := clientHandshake(NewCodec(clientConn), "client.v1", trust.UnprotectedRole)
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("clientHandshake did not answer a protocol mismatch")
	}
	var mismatch *ProtocolMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("clientHandshake = %v, want *ProtocolMismatchError", err)
	}
	if mismatch.Theirs != ProtocolVersion+1 || mismatch.Ours != ProtocolVersion {
		t.Fatalf("mismatch = %+v", mismatch)
	}
	if !isServicePeerTerminal(err) {
		t.Fatalf("protocol mismatch is retryable, so the dial ladder loops on it: %v", err)
	}
}

func TestClientHandshakeSurfacesPeerBuildRejection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	go func() {
		codec := NewCodec(serverConn)
		if _, err := codec.ReadFrame(); err != nil {
			return
		}
		payload, err := json.Marshal(handshakeAck{
			Protocol: ProtocolVersion, WireBuild: "daemon.v2", Rejected: true,
			Code: ResponseCodeBuildMismatch, Reason: ErrBuildMismatch.Error(),
		})
		if err == nil {
			_ = codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload})
		}
	}()
	_, err := clientHandshake(NewCodec(clientConn), "client.v1", trust.UnprotectedRole)
	var rejection *HandshakeRejectionError
	if !errors.As(err, &rejection) {
		t.Fatalf("clientHandshake = %v, want *HandshakeRejectionError", err)
	}
	if rejection.Code != ResponseCodeBuildMismatch {
		t.Fatalf("rejection code = %q", rejection.Code)
	}
	if !errors.Is(err, ErrBuildMismatch) {
		t.Fatalf("rejection does not unwrap to ErrBuildMismatch: %v", err)
	}
}
