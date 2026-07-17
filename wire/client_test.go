package wire_test

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

// fakeServer accepts connections in a short-path dir and runs serve per conn.
// serve owns the framing: read the request, then reply (or not) however the test
// needs. It returns a Dialer that opens a fresh conn per call.
func fakeServer(t *testing.T, serve func(f *wire.Framing)) wire.Dialer {
	t.Helper()
	sock := filepath.Join(wiretest.SocketDir(t), "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() {
				defer conn.Close()
				serve(wire.NewFraming(conn))
			}()
		}
	}()
	return func(context.Context) (net.Conn, error) { return net.Dial("unix", sock) }
}

func TestOutcomeReplayable(t *testing.T) {
	tests := []struct {
		o    wire.Outcome
		want bool
		name string
	}{
		{wire.Delivered, false, "delivered"},
		{wire.PreSendFailure, true, "pre-send-failure"},
		{wire.Rejected, true, "rejected"},
		{wire.PostSendFailure, false, "post-send-failure"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.Replayable(); got != tt.want {
				t.Errorf("Replayable() = %v, want %v", got, tt.want)
			}
			if tt.o.String() != tt.name {
				t.Errorf("String() = %q, want %q", tt.o.String(), tt.name)
			}
		})
	}
}

func TestDoClassifiesDelivered(t *testing.T) {
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame()
		_ = f.WriteJSON(wire.Response{Payload: json.RawMessage(`"ok"`)})
	})
	conn, err := dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	res, err := wire.Do(conn, []byte(`{"op":"x"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered", res.Outcome)
	}
	var payload string
	if err := json.Unmarshal(res.Response.Payload, &payload); err != nil || payload != "ok" {
		t.Errorf("payload = %q (err %v), want ok", payload, err)
	}
}

func TestDoClassifiesRejected(t *testing.T) {
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame()
		_ = f.WriteJSON(wire.Response{Rejected: true, Reason: "pool full"})
	})
	conn, err := dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	res, err := wire.Do(conn, []byte(`{"op":"x"}`))
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if res.Outcome != wire.Rejected {
		t.Fatalf("Outcome = %v, want Rejected", res.Outcome)
	}
	if !res.Outcome.Replayable() {
		t.Error("a Rejected outcome must be Replayable")
	}
	if res.Response.Reason != "pool full" {
		t.Errorf("Reason = %q, want %q", res.Response.Reason, "pool full")
	}
}

func TestDoClassifiesPreSendFailure(t *testing.T) {
	client, _ := wiretest.Pair(t)
	_ = client.Close() // a dead conn: the write cannot land

	res, err := wire.Do(client, []byte(`{"op":"x"}`))
	if err == nil {
		t.Fatal("Do on a closed conn returned nil error")
	}
	if res.Outcome != wire.PreSendFailure {
		t.Fatalf("Outcome = %v, want PreSendFailure", res.Outcome)
	}
}

func TestDoClassifiesPostSendFailure(t *testing.T) {
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame() // consume the request, then hang up without replying
	})
	conn, err := dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	res, err := wire.Do(conn, []byte(`{"op":"x"}`))
	if err == nil {
		t.Fatal("Do with no reply returned nil error")
	}
	if res.Outcome != wire.PostSendFailure {
		t.Fatalf("Outcome = %v, want PostSendFailure", res.Outcome)
	}
	if res.Outcome.Replayable() {
		t.Error("a PostSendFailure must never be Replayable")
	}
}

func TestSendPreSendFailureOnDialError(t *testing.T) {
	dial := func(context.Context) (net.Conn, error) { return nil, net.ErrClosed }
	res, err := wire.Send(context.Background(), dial, []byte(`{"op":"x"}`))
	if err == nil {
		t.Fatal("Send with a failing dialer returned nil error")
	}
	if res.Outcome != wire.PreSendFailure {
		t.Fatalf("Outcome = %v, want PreSendFailure", res.Outcome)
	}
}

func TestCallRetriesReplayable(t *testing.T) {
	var attempts atomic.Int64
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame()
		if attempts.Add(1) == 1 {
			_ = f.WriteJSON(wire.Response{Rejected: true, Reason: "pool full"})
			return
		}
		_ = f.WriteJSON(wire.Response{Payload: json.RawMessage(`"ok"`)})
	})

	res, err := wire.Call(context.Background(), dial, []byte(`{"op":"x"}`), 3, nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if res.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered after a retry", res.Outcome)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("server attempts = %d, want 2 (one Rejected, one Delivered)", got)
	}
}

func TestCallProbesPostSendNeverRefires(t *testing.T) {
	var sends atomic.Int64
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame()
		sends.Add(1) // received the request, then hang up: outcome unknown
	})

	var probed atomic.Bool
	probe := func(context.Context) (wire.Result, error) {
		probed.Store(true)
		return wire.Result{Outcome: wire.Delivered, Response: wire.Response{Payload: json.RawMessage(`"probed"`)}}, nil
	}

	res, err := wire.Call(context.Background(), dial, []byte(`{"op":"x"}`), 3, probe)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if !probed.Load() {
		t.Error("probe was not invoked on a PostSendFailure")
	}
	if res.Outcome != wire.Delivered {
		t.Errorf("Outcome = %v, want the probe's Delivered result", res.Outcome)
	}
	if got := sends.Load(); got != 1 {
		t.Errorf("request fired %d times, want 1 (a PostSendFailure must never be re-fired)", got)
	}
}

func TestCallPostSendWithoutProbeReturnsUnknown(t *testing.T) {
	var sends atomic.Int64
	dial := fakeServer(t, func(f *wire.Framing) {
		_, _ = f.ReadFrame()
		sends.Add(1)
	})

	res, err := wire.Call(context.Background(), dial, []byte(`{"op":"x"}`), 3, nil)
	if err == nil {
		t.Fatal("Call with no probe returned nil error on PostSendFailure")
	}
	if res.Outcome != wire.PostSendFailure {
		t.Fatalf("Outcome = %v, want PostSendFailure", res.Outcome)
	}
	if got := sends.Load(); got != 1 {
		t.Errorf("request fired %d times, want 1", got)
	}
}
