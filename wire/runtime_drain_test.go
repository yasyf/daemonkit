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

func TestAwaitAcceptDistinguishesClosedIntakeFromUnexpectedFailure(t *testing.T) {
	server := &Server{}
	boom := errors.New("accept failed")
	failed := make(chan error, 1)
	failed <- boom
	if err := server.awaitAccept(t.Context(), failed); !errors.Is(err, boom) {
		t.Fatalf("unexpected accept failure = %v", err)
	}

	server.intakeClosed = true
	closed := make(chan error, 1)
	closed <- net.ErrClosed
	ctx, cancel := context.WithCancel(t.Context())
	deferred := make(chan error, 1)
	go func() { deferred <- server.awaitAccept(ctx, closed) }()
	select {
	case err := <-deferred:
		t.Fatalf("closed intake ended Serve before runtime cancellation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-deferred:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("closed intake result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed intake did not settle after runtime cancellation")
	}
}

func TestRuntimeDrainWaitsForTerminalAcknowledgementBeforeClosingSession(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "daemonkit-runtime-drain-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	owner, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	workers, err := worker.NewPool(worker.Config{
		Capacity: 2, QueueCapacity: 2, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 1024, MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
	}, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(dir, "workers.db")}, Generation: owner,
		Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	children, err := proc.NewManager(2, &proc.Reaper{
		Store: &proc.FileStore{Path: filepath.Join(dir, "children.db")}, Generation: owner,
		Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	const build = "runtime-drain.v1"
	server := &Server{WireBuild: build, WriteTimeout: time.Second}
	handlerStarted := make(chan struct{})
	server.Register(HandlerSpec{
		Op: "test.runtime-drain",
		Handler: func(ctx context.Context, _ Request) (any, error) {
			close(handlerStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	runtime, err := NewRuntime(RuntimeConfig{
		Socket: filepath.Join(dir, "runtime.sock"), RuntimeBuild: build, RuntimeProtocol: 1,
		Wire: server, TrustPolicy: roleTestPolicy(t, true),
		StopControlStore: &proc.FileStore{Path: filepath.Join(dir, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 2 * time.Second,
		Signals: make(chan os.Signal),
	})
	if err != nil {
		t.Fatal(err)
	}
	slot := daemon.NewPublicationSlot[string](runtime)
	activation, err := runtime.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := slot.Stage(activation, "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatal(err)
	}

	conn, codec, identity := openRuntimeDrainSession(t, filepath.Join(dir, "runtime.sock"), build)
	defer conn.Close()
	if err := codec.WriteFrame(Frame{
		Kind: FrameRequest, Flags: FlagEnd, ID: 1, Op: "test.runtime-drain",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		closeDone <- runtime.Close(ctx)
	}()
	response := readRuntimeDrainResponse(t, codec, 1)
	if !response.Ack || response.Rejected || response.Err == "" {
		t.Fatalf("terminal response = %+v", response)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before terminal acknowledgement: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := codec.WriteFrame(Frame{
		Kind: FrameRequest, Flags: FlagEnd, ID: 2, Op: "test.runtime-drain",
	}); err != nil {
		t.Fatal(err)
	}
	rejected := readRuntimeDrainResponse(t, codec, 2)
	if !rejected.Rejected || rejected.Code != ResponseCodeRuntimeDraining || rejected.Ack {
		t.Fatalf("late response = %+v", rejected)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while terminal acknowledgement remained held: %v", err)
	default:
	}
	if err := codec.WriteFrame(Frame{
		Kind: FrameAck, Flags: FlagEnd, ID: 1, Payload: identity.Session,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not settle after terminal acknowledgement")
	}
}

func openRuntimeDrainSession(t *testing.T, socket, build string) (net.Conn, *Codec, handshakeAck) {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	codec := NewCodec(conn)
	payload, err := json.Marshal(handshakeIdentity{
		Protocol: ProtocolVersion, WireBuild: build, Role: trust.UnprotectedRole,
	})
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload}); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	if frame.Kind != FrameHelloAck || frame.Flags != FlagEnd {
		conn.Close()
		t.Fatalf("handshake frame = %#v", frame)
	}
	var identity handshakeAck
	if err := decodeStrict(frame.Payload, &identity); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return conn, codec, identity
}

func readRuntimeDrainResponse(t *testing.T, codec *Codec, id uint64) Response {
	t.Helper()
	for {
		frame, err := codec.ReadFrame()
		if err != nil {
			t.Fatal(err)
		}
		if frame.Kind != FrameResponse || frame.ID != id {
			continue
		}
		var response Response
		if err := decodeStrict(frame.Payload, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
}
