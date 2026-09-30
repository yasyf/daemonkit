package daemonkit

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
)

const businessIdle = 100 * time.Millisecond

type countingProduct struct {
	businessProduct
	handled atomic.Int32
}

func (p *countingProduct) Handle(ctx context.Context, req Request) (Reply, error) {
	p.handled.Add(1)
	return p.businessProduct.Handle(ctx, req)
}

func TestBusinessReattachesPastAnIdleReclaimedSession(t *testing.T) {
	product := &countingProduct{}
	sock := serveBusinessProduct(t, wire.Config{Schemas: wire.Schemas{businessSchema}, Idle: businessIdle}, product)
	var attaches atomic.Int32
	var first atomic.Pointer[wire.Client]
	lane := capacityLane(t, func(ctx context.Context) (*wire.Client, error) {
		attaches.Add(1)
		session, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial:      wire.UnixDialer(sock),
			Authorize: wiretest.AuthorizeTestServer,
			Lane:      wire.LaneBusiness,
			Schema:    businessSchema,
		})
		if err == nil {
			first.CompareAndSwap(nil, session)
		}
		return session, err
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = lane.Close(closeCtx)
	})

	if _, err := lane.Call(ctx, echoOp, []byte("before")); err != nil {
		t.Fatalf("Call() = %v, want a reply", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for first.Load().Failure() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the daemon never reclaimed the idle session")
		}
		time.Sleep(businessIdle / 10)
	}

	reply, err := lane.Call(ctx, echoOp, []byte("after"))
	if err != nil {
		t.Fatalf("Call() past the idle reclaim = %v (undispatched=%v), want a reply on a fresh session", err, Undispatched(err))
	}
	if string(reply.Body) != "after" {
		t.Fatalf("reply = %q, want %q", reply.Body, "after")
	}
	if got := attaches.Load(); got != 2 {
		t.Fatalf("attaches = %d, want 2: one session before the reclaim, one after", got)
	}
	if got := product.handled.Load(); got != 2 {
		t.Fatalf("product handled %d requests, want 2: no Call is sent twice", got)
	}
}
