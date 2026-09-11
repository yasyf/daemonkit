package wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

const testIdle = 300 * time.Millisecond

func dialBusinessErr(ctx context.Context, sock string) (*wire.Client, error) {
	return wire.NewClient(ctx, wire.ClientConfig{
		Dial:      wire.UnixDialer(sock),
		Authorize: wiretest.AuthorizeTestServer,
		Lane:      wire.LaneBusiness,
		Schema:    testSchema,
	})
}

func TestIdleSessionReleasesItsSlot(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{Concurrency: 1, Idle: testIdle})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	idle := dialBusiness(t, sock)
	defer func() { _ = idle.Abort(nil) }()

	if _, err := dialBusinessErr(ctx, sock); !errors.Is(err, wire.ErrSessionCapacity) {
		t.Fatalf("second dial while the slot is held = %v, want ErrSessionCapacity", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		reclaimed, err := dialBusinessErr(ctx, sock)
		if err == nil {
			_ = reclaimed.Abort(nil)
			return
		}
		if !errors.Is(err, wire.ErrSessionCapacity) {
			t.Fatalf("dial after the idle window = %v, want success or ErrSessionCapacity", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the idle session never released its slot: a peer that sends nothing holds capacity forever")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestActiveSessionKeepsItsSlot(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{Concurrency: 1, Idle: testIdle})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	active := dialBusiness(t, sock)
	defer func() { _ = active.Abort(nil) }()

	for range 6 {
		time.Sleep(testIdle / 2)
		if _, err := active.Call(ctx, "some.op", []byte("{}")); err != nil {
			t.Fatalf("Call on a session kept busy inside the idle window = %v", err)
		}
	}
	if err := active.Failure(); err != nil {
		t.Fatalf("a session in continuous use was reclaimed: %v", err)
	}
}

func TestSlowRequestOutlivingTheIdleWindowSurvives(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	rt.SetHandle(func(ctx context.Context, _ wire.Request) (any, error) {
		select {
		case <-time.After(4 * testIdle):
			return json.RawMessage(`{"slow":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	sock, _ := startServer(t, rt, wire.Config{Concurrency: 1, Idle: testIdle})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client := dialBusiness(t, sock)
	defer func() { _ = client.Abort(nil) }()

	result, err := client.Call(ctx, "some.op", []byte("{}"))
	if err != nil {
		t.Fatalf("Call whose handler outruns the idle window = %v, want success", err)
	}
	if result.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered", result.Outcome)
	}
}

func TestIdleZeroKeepsTheSessionForever(t *testing.T) {
	rt := wiretest.NewStubRuntime()
	sock, _ := startServer(t, rt, wire.Config{Concurrency: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	idle := dialBusiness(t, sock)
	defer func() { _ = idle.Abort(nil) }()

	time.Sleep(2 * testIdle)

	if _, err := dialBusinessErr(ctx, sock); !errors.Is(err, wire.ErrSessionCapacity) {
		t.Fatalf("dial with Idle unset = %v, want ErrSessionCapacity (no reclaim without an idle window)", err)
	}
	if err := idle.Failure(); err != nil {
		t.Fatalf("session failed with Idle unset: %v", err)
	}
}
