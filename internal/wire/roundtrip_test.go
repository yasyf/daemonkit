package wire_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

const testSchema = "test.v1"

func startServer(t *testing.T, rt wire.Runtime, cfg wire.Config) (string, *wire.Server) {
	t.Helper()
	if cfg.Schemas == nil {
		cfg.Schemas = wire.Schemas{testSchema}
	}
	server, err := wire.NewServer(rt, cfg)
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}
	sock := filepath.Join(wiretest.SocketDir(t), "srv")
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
	return sock, server
}

func dialBusiness(t *testing.T, sock string) *wire.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Schema: testSchema,
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = client.Abort(nil) })
	return client
}

func TestBusinessRoundTrip(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(_ context.Context, req wire.Request) (any, error) {
		if req.Op != "test.echo.v1" {
			return nil, errors.New("unknown op")
		}
		if req.Schema != testSchema {
			t.Errorf("Request.Schema = %q, want %q", req.Schema, testSchema)
		}
		return json.RawMessage(req.Payload), nil
	})
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	result, err := client.Call(ctx, "test.echo.v1", []byte(`{"n":42}`))
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if result.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered", result.Outcome)
	}
	if string(result.Response.Payload) != `{"n":42}` {
		t.Fatalf("Payload = %s, want the echoed body", result.Response.Payload)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

func TestServerPushedEventsReachTheClient(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(ctx context.Context, req wire.Request) (any, error) {
		if err := req.Session.PushEvent(ctx, wire.Event{Topic: "topic.v1", Payload: []byte("ping")}); err != nil {
			return nil, err
		}
		return json.RawMessage("{}"), nil
	})
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Call(ctx, "any.op", []byte(`{}`)); err != nil {
		t.Fatalf("Call() = %v", err)
	}
	select {
	case event := <-client.Events():
		if event.Topic != "topic.v1" || !bytes.Equal(event.Payload, []byte("ping")) {
			t.Fatalf("event = %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("no event before deadline")
	}
}

func TestBidirectionalStreaming(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(_ context.Context, req wire.Request) (any, error) {
		var received [][]byte
		for chunk := range req.Chunks {
			if chunk.End && len(chunk.Payload) == 0 {
				continue
			}
			received = append(received, chunk.Payload)
		}
		out := make(chan []byte, len(received))
		for _, payload := range received {
			out <- payload
		}
		close(out)
		return wire.StreamResponse{Chunks: out, Value: map[string]int{"chunks": len(received)}}, nil
	})
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	call, err := client.Open(ctx, "stream.op", nil, false)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	for _, payload := range []string{"one", "two"} {
		if err := call.SendChunk(ctx, []byte(payload)); err != nil {
			t.Fatalf("SendChunk(%q) = %v", payload, err)
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		t.Fatalf("CloseSend() = %v", err)
	}
	var got []string
	for chunk := range call.Chunks() {
		if len(chunk.Payload) == 0 {
			continue
		}
		got = append(got, string(chunk.Payload))
	}
	result, err := call.Response(ctx)
	if err != nil {
		t.Fatalf("Response() = %v", err)
	}
	if result.Outcome != wire.Delivered || string(result.Response.Payload) != `{"chunks":2}` {
		t.Fatalf("result = %v %s", result.Outcome, result.Response.Payload)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("chunks = %q, want [one two] in order", got)
	}
}

func TestSchemaGatesProductDispatch(t *testing.T) {
	tests := []struct {
		name    string
		schemas wire.Schemas
		schema  string
		wantErr error
	}{
		{"foreign digest rejected", wire.Schemas{testSchema}, "foreign.v9", wire.ErrBuildMismatch},
		{"prior era accepted", wire.Schemas{testSchema, "old.v0"}, "old.v0", nil},
		{"own digest accepted", wire.Schemas{testSchema, "old.v0"}, testSchema, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := wiretest.NewStubRuntime()
			rt.SetHandle(func(context.Context, wire.Request) (any, error) { return json.RawMessage("{}"), nil })
			sock, _ := startServer(t, rt, wire.Config{Schemas: tt.schemas})
			client := dialSchema(t, sock, tt.schema)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := client.Call(ctx, "some.op", []byte("{}"))
			if err != nil {
				t.Fatalf("Call() = %v", err)
			}
			if tt.wantErr == nil {
				if result.Outcome != wire.Delivered {
					t.Fatalf("Call() = %v %+v, want delivery", result.Outcome, result.Response)
				}
				return
			}
			if result.Outcome != wire.Rejected || result.Response.Code != wire.ResponseCodeBuildMismatch {
				t.Fatalf("Call() = %v %+v, want a build_mismatch rejection", result.Outcome, result.Response)
			}
			if !errors.Is(result.Rejection(), tt.wantErr) {
				t.Fatalf("Rejection() = %v, want %v", result.Rejection(), tt.wantErr)
			}
		})
	}
}

func TestDrainVerbAnswersAForeignSchemaAndRaisesThePreamble(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	successor := dialSchema(t, sock, "successor.v2")

	result, err := successor.Call(ctx, "daemon.control.drain", []byte("{}"))
	if err != nil {
		t.Fatalf("drain Call() = %v", err)
	}
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		t.Fatalf("drain result = %v %+v", result.Outcome, result.Response)
	}
	select {
	case <-rt.Drained:
	default:
		t.Fatal("drain verb responded before Runtime.Drain ran")
	}

	_, err = wire.NewClient(ctx, wire.ClientConfig{Dial: wire.UnixDialer(sock), Schema: testSchema})
	if !errors.Is(err, wire.ErrDraining) {
		t.Fatalf("NewClient() against a draining server = %v, want ErrDraining via the preamble", err)
	}
}

func TestReservedOpsAreDenied(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := client.Call(ctx, "daemon.anything.else", []byte("{}"))
	if err != nil {
		t.Fatalf("Call() = %v", err)
	}
	if result.Outcome != wire.Rejected || result.Response.Code != wire.ResponseCodePermissionDenied {
		t.Fatalf("Call() = %v %+v, want permission_denied rejection", result.Outcome, result.Response)
	}
	if !errors.Is(result.Rejection(), wire.ErrPermissionDenied) {
		t.Fatalf("Rejection() = %v, want ErrPermissionDenied", result.Rejection())
	}
}

func TestSessionCapacityIsConcurrency(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{Concurrency: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	held := dialBusiness(t, sock)
	defer func() { _ = held.Abort(nil) }()
	if _, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Schema: testSchema,
	}); !errors.Is(err, wire.ErrSessionCapacity) {
		t.Fatalf("second NewClient() = %v, want ErrSessionCapacity", err)
	}
}

func TestPhaseGateTypedRejections(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetPhase(wire.PhaseStarting, nil)
	rt.SetHandle(func(context.Context, wire.Request) (any, error) {
		return json.RawMessage("{}"), nil
	})
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := client.Call(ctx, "some.op", []byte("{}"))
	if err != nil {
		t.Fatalf("Call() while starting = %v", err)
	}
	if result.Outcome != wire.Rejected || result.Response.Code != wire.ResponseCodeRuntimeStarting {
		t.Fatalf("starting-phase call = %v %+v, want runtime_starting rejection", result.Outcome, result.Response)
	}
	if !errors.Is(result.Rejection(), wire.ErrNotReady) {
		t.Fatalf("Rejection() = %v, want ErrNotReady", result.Rejection())
	}

	rt.SetPhase(wire.PhaseReady, nil)
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() after SetPhase(ready) = %v", err)
	}
	result, err = client.Call(ctx, "some.op", []byte("{}"))
	if err != nil || result.Outcome != wire.Delivered {
		t.Fatalf("ready-phase call = %v %v, want delivery", result.Outcome, err)
	}
}
