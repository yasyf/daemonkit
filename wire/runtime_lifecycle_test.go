package wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/drain"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
)

type absentPeer struct{}

func (absentPeer) Health(context.Context) (daemon.Health, error) {
	return daemon.Health{}, daemon.ErrNoPeer
}
func (absentPeer) Shutdown(context.Context) error { return daemon.ErrNoPeer }
func (absentPeer) Handoff(context.Context) error  { return daemon.ErrNoPeer }

type settledWorkers struct{}

func (settledWorkers) Close()                     {}
func (settledWorkers) Cancel()                    {}
func (settledWorkers) Wait(context.Context) error { return nil }

type lifecycleCloser struct{ closed chan<- struct{} }

func (c lifecycleCloser) Close() error {
	if c.closed != nil {
		c.closed <- struct{}{}
	}
	return nil
}

func TestRuntimeLifecycleAckPrecedesShutdownOrHandoff(t *testing.T) {
	tests := []struct {
		name     string
		contract daemon.Contract
		op       wire.Op
		request  any
	}{
		{name: "shutdown", contract: daemon.RequestDaemon, op: lifeproto.OpShutdown, request: lifeproto.NewShutdownRequest()},
		{name: "handoff", contract: daemon.ResourceOwner, op: lifeproto.OpHandoff, request: lifeproto.NewHandoffRequest()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const maxFrame = 16 << 20
			dir, err := os.MkdirTemp("/tmp", "dkr-wire-")
			if err != nil {
				t.Fatalf("MkdirTemp: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			path := filepath.Join(dir, "runtime.sock")
			server := &wire.Server{Build: "server-test", MaxFrame: maxFrame}
			fillStarted := make(chan struct{})
			server.RegisterControl("fill", func(ctx context.Context, request wire.Request) (any, error) {
				if err := request.Session.PushEvent(ctx, wire.Event{
					Topic:   "block-writer",
					Payload: []byte(strings.Repeat("x", 8<<20)),
				}); err != nil {
					return nil, err
				}
				close(fillStarted)
				return true, nil
			})
			intake := &drain.Intake{}
			settled := make(chan struct{}, 2)
			cfg := daemon.RuntimeConfig{
				Socket:          path,
				Build:           "server-test",
				Protocol:        int(wire.ProtocolVersion),
				Peer:            absentPeer{},
				Contract:        test.contract,
				WaitMode:        daemon.SocketRelease,
				Admission:       intake,
				Server:          server,
				Workers:         settledWorkers{},
				State:           lifecycleCloser{},
				Resources:       lifecycleCloser{closed: settled},
				Activate:        func(context.Context) error { return nil },
				ShutdownTimeout: 3 * time.Second,
				Signals:         make(chan os.Signal),
			}
			if test.contract == daemon.ResourceOwner {
				cfg.Handoff = func(context.Context) error {
					settled <- struct{}{}
					return nil
				}
			}
			runtime, err := daemon.NewRuntime(cfg)
			if err != nil {
				t.Fatalf("NewRuntime: %v", err)
			}
			server.RegisterLifecycle(runtime)
			runDone := make(chan error, 1)
			go func() { runDone <- runtime.Run(context.Background()) }()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = runtime.Close(ctx)
			})

			conn := dialRuntime(t, path)
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
			helloAck, err := codec.ReadFrame()
			if err != nil {
				t.Fatalf("read hello ack: %v", err)
			}
			var serverIdentity wire.BuildIdentity
			if err := json.Unmarshal(helloAck.Payload, &serverIdentity); err != nil {
				t.Fatalf("decode server identity: %v", err)
			}
			if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameWindow, Sequence: 1}); err != nil {
				t.Fatalf("grant event window: %v", err)
			}
			if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameRequest, Flags: wire.FlagEnd, ID: 1, Op: "fill"}); err != nil {
				t.Fatalf("write fill: %v", err)
			}
			select {
			case <-fillStarted:
			case <-time.After(time.Second):
				t.Fatal("fill handler did not start")
			}
			payload, err := lifeproto.Encode(test.request)
			if err != nil {
				t.Fatalf("Encode lifecycle request: %v", err)
			}
			if err := codec.WriteFrame(wire.Frame{Kind: wire.FrameRequest, Flags: wire.FlagEnd, ID: 2, Op: test.op, Payload: payload}); err != nil {
				t.Fatalf("write lifecycle request: %v", err)
			}
			select {
			case <-settled:
				t.Fatal("resource settlement ran before the blocked lifecycle acknowledgment")
			case <-time.After(75 * time.Millisecond):
			}
			first := readNonWindowFrame(t, codec)
			if first.Kind != wire.FrameEvent || first.Op != "block-writer" {
				t.Fatalf("first frame = %#v", first)
			}
			var fillResponse, lifecycleAck bool
			for !fillResponse || !lifecycleAck {
				frame := readNonWindowFrame(t, codec)
				if frame.Kind != wire.FrameResponse {
					t.Fatalf("terminal frame = %#v", frame)
				}
				switch frame.ID {
				case 1:
					fillResponse = true
				case 2:
					lifecycleAck = true
				default:
					t.Fatalf("unexpected response = %#v", frame)
				}
				if err := codec.WriteFrame(wire.Frame{
					Kind: wire.FrameAck, Flags: wire.FlagEnd, ID: frame.ID, Payload: serverIdentity.Session,
				}); err != nil {
					t.Fatalf("ack response %d: %v", frame.ID, err)
				}
			}
			select {
			case <-settled:
			case <-time.After(time.Second):
				t.Fatal("resource settlement did not follow the lifecycle acknowledgment")
			}
			select {
			case err := <-runDone:
				if err != nil {
					t.Fatalf("Runtime.Run: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("runtime did not exit")
			}
		})
	}
}

func TestRuntimeHandoffResponseSurvivesImmediateSessionClosure(t *testing.T) {
	for iteration := range 100 {
		dir, err := os.MkdirTemp("/tmp", "dkr-handoff-")
		if err != nil {
			t.Fatalf("iteration %d: MkdirTemp: %v", iteration, err)
		}
		path := filepath.Join(dir, "runtime.sock")
		server := &wire.Server{Build: "server-test"}
		intake := &drain.Intake{}
		cfg := daemon.RuntimeConfig{
			Socket: path, Build: "server-test", Protocol: int(wire.ProtocolVersion),
			Peer: absentPeer{}, Contract: daemon.ResourceOwner, WaitMode: daemon.SocketRelease,
			Admission: intake, Server: server, Workers: settledWorkers{},
			State: lifecycleCloser{}, Resources: lifecycleCloser{},
			Activate: func(context.Context) error { return nil },
			Handoff:  func(context.Context) error { return nil },
			Signals:  make(chan os.Signal),
		}
		runtime, err := daemon.NewRuntime(cfg)
		if err != nil {
			t.Fatalf("iteration %d: NewRuntime: %v", iteration, err)
		}
		server.RegisterLifecycle(runtime)
		runDone := make(chan error, 1)
		go func() { runDone <- runtime.Run(context.Background()) }()
		client := newRuntimeClientWithDial(t, "server-test", func(ctx context.Context) (net.Conn, error) {
			conn, err := wire.UnixDialer(path)(ctx)
			if err != nil {
				return nil, err
			}
			return &failResponseWindowConn{Conn: conn, requestWritten: make(chan struct{})}, nil
		})
		payload, err := lifeproto.Encode(lifeproto.NewHandoffRequest())
		if err != nil {
			t.Fatalf("iteration %d: Encode: %v", iteration, err)
		}
		result, callErr := client.Call(context.Background(), wire.Op(lifeproto.OpHandoff), "", payload)
		_ = client.Close()
		if callErr != nil || result.Outcome != wire.Delivered || result.Response.Err != "" {
			t.Fatalf("iteration %d: Handoff = %#v, %v", iteration, result, callErr)
		}
		select {
		case runErr := <-runDone:
			if runErr != nil {
				t.Fatalf("iteration %d: Runtime.Run: %v", iteration, runErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: runtime did not exit", iteration)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("iteration %d: RemoveAll: %v", iteration, err)
		}
	}
}

func TestResourceOwnerSocketReleaseKeepsLifecycleAvailableWhileDraining(t *testing.T) {
	const (
		incumbentBuild = "v1.0.0"
		successorBuild = "v1.1.0"
	)
	dir, err := os.MkdirTemp("/tmp", "dkr-takeover-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "runtime.sock")
	server := &wire.Server{Build: incumbentBuild}
	server.RegisterConcurrent("work", func(context.Context, wire.Request) (any, error) {
		return true, nil
	})
	intake := &drain.Intake{}
	handoffStarted := make(chan struct{})
	releaseHandoff := make(chan struct{})
	runtime, err := daemon.NewRuntime(daemon.RuntimeConfig{
		Socket: path, Build: incumbentBuild, Protocol: int(wire.ProtocolVersion),
		Peer: absentPeer{}, Contract: daemon.ResourceOwner, WaitMode: daemon.SocketRelease,
		Admission: intake, Server: server, Workers: settledWorkers{},
		State: lifecycleCloser{}, Resources: lifecycleCloser{},
		Activate: func(context.Context) error { return nil },
		Handoff: func(ctx context.Context) error {
			close(handoffStarted)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseHandoff:
				return nil
			}
		},
		ShutdownTimeout: 3 * time.Second,
		Signals:         make(chan os.Signal),
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	server.RegisterLifecycle(runtime)
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	})

	business := newRuntimeClient(t, path, incumbentBuild)
	defer business.Close()
	observer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: successorBuild,
	}}
	defer observer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if _, err := observer.Health(ctx); err != nil {
		cancel()
		t.Fatalf("prime lifecycle session: %v", err)
	}
	cancel()

	takeoverPeer := &wire.LifecyclePeer{Config: wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: successorBuild,
	}}
	defer takeoverPeer.Close()
	type takeoverResult struct {
		outcome daemon.Outcome
		err     error
	}
	takeoverDone := make(chan takeoverResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		outcome, err := daemon.Run(ctx, daemon.TakeoverConfig{
			Self: successorBuild, Protocol: int(wire.ProtocolVersion), Peer: takeoverPeer,
			Contract: daemon.ResourceOwner, WaitMode: daemon.SocketRelease,
			WaitTimeout: time.Second,
		})
		takeoverDone <- takeoverResult{outcome: outcome, err: err}
	}()

	select {
	case <-handoffStarted:
	case <-time.After(time.Second):
		t.Fatal("handoff did not reach resource settlement")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	health, err := observer.Health(ctx)
	cancel()
	if err != nil {
		t.Fatalf("Health while draining: %v", err)
	}
	if !health.Draining {
		t.Fatal("Health while draining reported draining=false")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	result, err := business.Call(ctx, "work", "", nil)
	cancel()
	if err != nil {
		t.Fatalf("ordinary request while draining: %v", err)
	}
	if result.Outcome != wire.Rejected || result.Response.Reason != wire.ErrDraining.Error() {
		t.Fatalf("ordinary result = %#v, want rejected draining", result)
	}

	close(releaseHandoff)
	select {
	case result := <-takeoverDone:
		if result.err != nil {
			t.Fatalf("takeover: %v", result.err)
		}
		if result.outcome != daemon.Bind {
			t.Fatalf("takeover outcome = %v, want Bind", result.outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("takeover did not observe socket release")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Runtime.Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not exit after handoff")
	}
}

func readNonWindowFrame(t *testing.T, codec *wire.Codec) wire.Frame {
	t.Helper()
	for {
		frame, err := codec.ReadFrame()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if frame.Kind != wire.FrameWindow {
			return frame
		}
	}
}

func dialRuntime(t *testing.T, path string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		conn, err := net.Dial("unix", path)
		if err == nil {
			return conn
		}
		if !errors.Is(err, os.ErrNotExist) && time.Now().After(deadline) {
			t.Fatalf("Dial: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime socket did not appear: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func newRuntimeClient(t *testing.T, path, build string) *wire.Client {
	t.Helper()
	return newRuntimeClientWithDial(t, build, wire.UnixDialer(path))
}

func newRuntimeClientWithDial(t *testing.T, build string, dial wire.Dialer) *wire.Client {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		client, err := wire.NewClient(ctx, wire.ClientConfig{Dial: dial, Build: build})
		cancel()
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewClient: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}
