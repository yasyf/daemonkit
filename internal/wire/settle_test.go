package wire_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

// TestInFlightCoversResponseWrite proves the admitted-work count outlives
// Runtime.Handle: a settlement that snapshots the moment Handle returns would
// tear the session down before the marshaled terminal reaches the peer.
func TestInFlightCoversResponseWrite(t *testing.T) {
	body := make([]byte, 3<<20)
	if _, err := rand.Read(body); err != nil {
		t.Fatalf("rand: %v", err)
	}
	returned := make(chan struct{})
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(context.Context, wire.Request) (any, error) {
		defer close(returned)
		return json.RawMessage(mustJSON(t, body)), nil
	})
	sock, server := startServer(t, rt, wire.Config{MaxFrame: 16 << 20})
	client := dialBusinessFrame(t, sock, 16<<20)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	call, err := client.Open(ctx, "test.echo.v1", nil, true)
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}

	<-returned
	if got := server.InFlight(); got == 0 {
		t.Fatal("InFlight() = 0 the instant Handle returned; the terminal is not written yet")
	}
	settleCtx, settleCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer settleCancel()
	for server.InFlight() > 0 {
		select {
		case <-settleCtx.Done():
			t.Fatal("admitted work never settled")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := server.Settle(settleCtx); err != nil {
		t.Fatalf("Settle() = %v", err)
	}

	result, err := call.Response(ctx)
	if err != nil {
		t.Fatalf("Response() = %v", err)
	}
	var echoed []byte
	if err := json.Unmarshal(result.Response.Payload, &echoed); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if !bytes.Equal(echoed, body) {
		t.Fatalf("payload = %d bytes, want %d", len(echoed), len(body))
	}
}

func TestClientCloseHonorsContextDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	blocked := make(chan struct{})
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(context.Context, wire.Request) (any, error) {
		close(blocked)
		<-release
		return json.RawMessage(`{}`), nil
	})
	sock, _ := startServer(t, rt, wire.Config{})
	client := dialBusiness(t, sock)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	if _, err := client.Open(ctx, "test.echo.v1", nil, true); err != nil {
		t.Fatalf("Open() = %v", err)
	}
	<-blocked

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer closeCancel()
	start := time.Now()
	err := client.Close(closeCtx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Close() with a pending call succeeded; want the ctx deadline")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Close() took %v, want the 100ms ctx deadline", elapsed)
	}
}

func dialBusinessFrame(t *testing.T, sock string, maxFrame int) *wire.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(sock), Authorize: wiretest.AuthorizeTestServer, Lane: wire.LaneBusiness, Schema: testSchema, MaxFrame: maxFrame,
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	t.Cleanup(func() { _ = client.Abort(nil) })
	return client
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return payload
}
