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
			if _, err := codec.ReadFrame(); err != nil {
				t.Fatalf("read hello ack: %v", err)
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
			first, err := codec.ReadFrame()
			if err != nil {
				t.Fatalf("read blocking event: %v", err)
			}
			if first.Kind != wire.FrameEvent || first.Op != "block-writer" {
				t.Fatalf("first frame = %#v", first)
			}
			var fillResponse, lifecycleAck bool
			for !fillResponse || !lifecycleAck {
				frame, err := codec.ReadFrame()
				if err != nil {
					t.Fatalf("read terminal response: %v", err)
				}
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
