package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

const holdOp = "hold"

// sessionProduct captures each request's Session and, on holdOp, blocks past
// its own context until released — the shape of a handler wedged on work the
// peer's death cannot cancel.
type sessionProduct struct {
	sessions chan Session
	release  chan struct{}
	returned chan struct{}
}

func (p *sessionProduct) Handle(_ context.Context, req Request) (Reply, error) {
	switch req.Op {
	case echoOp:
		p.sessions <- req.Session
		return Reply{Body: req.Body}, nil
	case holdOp:
		p.sessions <- req.Session
		<-p.release
		close(p.returned)
		return Reply{}, errors.New("held")
	}
	return Reply{}, fmt.Errorf("unknown op %q", req.Op)
}

func (p *sessionProduct) Drain(Budget) error { return nil }

func (p *sessionProduct) Close(Budget) error { return nil }

// TestSessionDisconnectedFiresWhileAHandlerIsStillBlocked is the property
// fusekit's native-session supervision rides: a session whose peer vanishes
// mid-handler publishes Disconnected before the blocked handler returns and
// before Done settles.
func TestSessionDisconnectedFiresWhileAHandlerIsStillBlocked(t *testing.T) {
	product := &sessionProduct{
		sessions: make(chan Session, 1),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	sock := serveBusinessProduct(t, wire.Config{Schemas: wire.Schemas{businessSchema}}, product)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	lane, err := BusinessOverConn(ctx, conn, Contract{Schema: businessSchema})
	if err != nil {
		t.Fatalf("BusinessOverConn: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = lane.Close(closeCtx)
	})
	go func() { _, _ = lane.Call(ctx, holdOp, nil) }()

	var session Session
	select {
	case session = <-product.sessions:
	case <-time.After(10 * time.Second):
		t.Fatal("the hold handler never started")
	}
	select {
	case <-session.Disconnected():
		t.Fatal("Disconnected closed while the peer was still attached")
	default:
	}

	_ = conn.Close()

	select {
	case <-session.Disconnected():
	case <-time.After(10 * time.Second):
		t.Fatal("Disconnected did not close on transport loss")
	}
	select {
	case <-product.returned:
		t.Fatal("the handler returned before Disconnected was observed")
	default:
	}
	select {
	case <-session.Done():
		t.Fatal("Done closed while a handler was still in flight")
	default:
	}

	close(product.release)
	select {
	case <-session.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the handler settled")
	}
}

func TestSessionDisconnectedPrecedesDoneOnCleanClose(t *testing.T) {
	product := &sessionProduct{sessions: make(chan Session, 1)}
	sock := serveBusinessProduct(t, wire.Config{Schemas: wire.Schemas{businessSchema}}, product)
	lane := dialBusiness(t, sock, Contract{Schema: businessSchema})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := lane.Call(ctx, echoOp, []byte("ping")); err != nil {
		t.Fatalf("Call() = %v", err)
	}
	session := <-product.sessions
	select {
	case <-session.Disconnected():
		t.Fatal("Disconnected closed on a live session")
	case <-session.Done():
		t.Fatal("Done closed on a live session")
	default:
	}

	if err := lane.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	select {
	case <-session.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after a clean close")
	}
	select {
	case <-session.Disconnected():
	default:
		t.Fatal("Done closed with Disconnected still open")
	}
}
