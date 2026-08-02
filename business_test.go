package daemonkit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/internal/wire/wiretest"
	"github.com/yasyf/daemonkit/paths"
)

const (
	businessSchema = "test.v1"
	echoOp         = "echo"
	codedOp        = "coded"
	bareOp         = "bare"
	wrappedOp      = "wrapped"
)

type businessProduct struct{}

func (businessProduct) Handle(_ context.Context, req Request) (Reply, error) {
	switch req.Op {
	case echoOp:
		return Reply{Body: req.Body}, nil
	case codedOp:
		return Reply{}, &ProductError{Code: "quota_exhausted", Message: "no quota left"}
	case bareOp:
		return Reply{}, errors.New("the product said no")
	case wrappedOp:
		return Reply{}, fmt.Errorf("dispatch: %w", &ProductError{Code: "wrapped", Message: "inner"})
	default:
		return Reply{}, fmt.Errorf("unknown op %q", req.Op)
	}
}

func (businessProduct) Drain(Budget) error { return nil }

func (businessProduct) Close(Budget) error { return nil }

// serveBusiness stands up one real wire server over the root's own runtime, so
// a terminal crosses the exact encode the daemon uses, and returns its socket.
func serveBusiness(t *testing.T) string {
	t.Helper()
	rt := newServeRuntime(int(MaxDetail(0)))
	rt.ready(businessProduct{})
	server, err := wire.NewServer(rt, wire.Config{Schemas: wire.Schemas{businessSchema}})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sock := filepath.Join(wiretest.SocketDir(t), "srv")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})
	return sock
}

func dialBusiness(t *testing.T, sock string, contract Contract) *Business {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	lane, err := BusinessOverConn(ctx, conn, contract)
	if err != nil {
		t.Fatalf("BusinessOverConn: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = lane.Close(closeCtx)
	})
	return lane
}

func TestBusinessOverConnCarriesRepliesAndProductFailures(t *testing.T) {
	lane := dialBusiness(t, serveBusiness(t), Contract{Schema: businessSchema})
	tests := []struct {
		name            string
		op              string
		body            []byte
		wantBody        []byte
		wantCode        string
		wantMessage     string
		wantUndispatchd bool
	}{
		{name: "reply body round trips", op: echoOp, body: []byte("hello"), wantBody: []byte("hello")},
		{name: "empty reply body", op: echoOp, wantBody: nil},
		{
			name: "product code crosses", op: codedOp,
			wantCode: "quota_exhausted", wantMessage: "no quota left",
		},
		{name: "a plain error crosses as code \"\"", op: bareOp, wantMessage: "the product said no"},
		{
			name: "a wrapped product error carries its code", op: wrappedOp,
			wantCode: "wrapped", wantMessage: "inner",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reply, err := lane.Call(ctx, tt.op, tt.body)
			if tt.wantMessage == "" {
				if err != nil {
					t.Fatalf("Call() = %v, want a reply", err)
				}
				if !bytes.Equal(reply.Body, tt.wantBody) {
					t.Fatalf("Call() body = %q, want %q", reply.Body, tt.wantBody)
				}
				return
			}
			var product *ProductError
			if !errors.As(err, &product) {
				t.Fatalf("Call() = %v, want a *ProductError", err)
			}
			if product.Code != tt.wantCode || product.Message != tt.wantMessage {
				t.Fatalf("Call() = %+v, want code %q message %q", product, tt.wantCode, tt.wantMessage)
			}
			if Undispatched(err) {
				t.Error("Undispatched() = true for a delivered product failure")
			}
		})
	}
}

func TestBusinessRefusesAnOversizeBodyBeforeSending(t *testing.T) {
	lane := dialBusiness(t, serveBusiness(t), Contract{Schema: businessSchema, MaxFrame: 8 << 10})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := make([]byte, int(maxFramedBytes(8<<10))+1)
	if _, err := lane.Call(ctx, echoOp, body); !errors.Is(err, ErrOversize) {
		t.Fatalf("Call() = %v, want ErrOversize", err)
	} else if !Undispatched(err) {
		t.Error("Undispatched() = false for a body that was never written")
	}
	fits := make([]byte, int(maxFramedBytes(8<<10)))
	if _, err := lane.Call(ctx, echoOp, fits); err != nil {
		t.Fatalf("Call() at the bound = %v, want a reply", err)
	}
}

func TestBusinessOverConnClosesOnItsSingleSessionsTerminalFailure(t *testing.T) {
	sock := serveBusiness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	lane, err := BusinessOverConn(ctx, conn, Contract{Schema: businessSchema})
	if err != nil {
		t.Fatalf("BusinessOverConn: %v", err)
	}
	if _, err := lane.Call(ctx, echoOp, []byte("first")); err != nil {
		t.Fatalf("Call() = %v, want a reply", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close transport: %v", err)
	}
	if _, err := lane.Call(ctx, echoOp, []byte("second")); err == nil {
		t.Fatal("Call() over a severed transport succeeded")
	}
	_, closed := lane.Call(ctx, echoOp, []byte("third"))
	if !errors.Is(closed, ErrLaneClosed) {
		t.Fatalf("Call() = %v, want ErrLaneClosed", closed)
	}
	if !Undispatched(closed) {
		t.Error("Undispatched() = false for a lane that refused before writing")
	}
}

// TestBusinessSurvivesATypedRejectionOnItsSingleSession is the refusal/failure
// disjunction reaching retirement: a rejection is the server's own typed answer
// on a session it proves alive by answering, so it must not spend the one
// session a caller-authenticated lane will ever have — ErrNotReady stays what
// Call documents it as, retryable, instead of becoming ErrLaneClosed.
func TestBusinessSurvivesATypedRejectionOnItsSingleSession(t *testing.T) {
	rt := newServeRuntime(int(MaxDetail(0)))
	server, err := wire.NewServer(rt, wire.Config{Schemas: wire.Schemas{businessSchema}})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sock := filepath.Join(wiretest.SocketDir(t), "srv")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serveCtx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	})

	ctx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer callCancel()
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

	_, notReady := lane.Call(ctx, echoOp, []byte("early"))
	if !errors.Is(notReady, ErrNotReady) {
		t.Fatalf("Call() before ready = %v, want ErrNotReady", notReady)
	}
	if !Undispatched(notReady) {
		t.Error("Undispatched() = false for a rejection the server proved undispatched")
	}
	rt.ready(businessProduct{})
	reply, err := lane.Call(ctx, echoOp, []byte("after"))
	if err != nil {
		t.Fatalf("Call() after ready = %v, want the session a rejection never impeached", err)
	}
	if string(reply.Body) != "after" {
		t.Fatalf("Call() body = %q, want %q", reply.Body, "after")
	}
}

func TestBusinessConfigBoundary(t *testing.T) {
	deadlined := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), time.Second)
	}
	t.Run("zero Business names its constructors", func(t *testing.T) {
		ctx, cancel := deadlined()
		defer cancel()
		_, err := new(Business).Call(ctx, echoOp, nil)
		if err == nil || !Undispatched(err) {
			t.Fatalf("Call() = %v, want an undispatched config refusal", err)
		}
		for _, want := range []string{"Client.Business", "BusinessOverConn"} {
			if !bytes.Contains([]byte(err.Error()), []byte(want)) {
				t.Errorf("Call() = %v, want it to name %s", err, want)
			}
		}
	})
	t.Run("a lane with no schema refuses", func(t *testing.T) {
		ctx, cancel := deadlined()
		defer cancel()
		client, err := Open(Daemon{Label: "com.yasyf.daemonkit.business", Trust: Trust{Serving: ServingSameUser()}})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if _, err := client.Business().Call(ctx, echoOp, nil); err == nil || errors.Is(err, ErrAbsent) {
			t.Fatalf("Call() = %v, want a schema refusal before any dial", err)
		}
		if _, err := BusinessOverConn(ctx, nil, Contract{}); err == nil {
			t.Fatal("BusinessOverConn() accepted a Contract naming no schema")
		}
	})
	t.Run("Call requires a deadline", func(t *testing.T) {
		if _, err := new(Business).Call(context.Background(), echoOp, nil); err == nil {
			t.Fatal("Call() accepted a context without a deadline")
		}
		if err := new(Business).Close(context.Background()); err == nil {
			t.Fatal("Close() accepted a context without a deadline")
		}
	})
}

func TestUndispatchedReadsTheTransportsOwnOutcome(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"a foreign error proves nothing", errors.New("something"), false},
		{"a bare sentinel proves nothing", ErrUntrusted, false},
		{"a bare sentinel the lane does return proves nothing", ErrLaneClosed, false},
		{"never written", refused(ErrAbsent), true},
		{"an untrusted peer no byte reached", refused(ErrUntrusted), true},
		{"rejected", &callError{outcome: wire.Rejected, err: ErrNotReady}, true},
		{"delivered", &callError{outcome: wire.Delivered, err: &ProductError{Message: "no"}}, false},
		{"sent, no terminal", &callError{outcome: wire.PostSendFailure, err: errors.New("lost")}, false},
		{"delivery unknown", &callError{outcome: wire.DeliveryUnknown, err: errors.New("lost")}, false},
		{"wrapped", fmt.Errorf("outer: %w", refused(ErrOversize)), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Undispatched(tt.err); got != tt.want {
				t.Fatalf("Undispatched(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestClientBusinessPerformsNoIO(t *testing.T) {
	client, err := Open(Daemon{
		Label:   "com.yasyf.daemonkit.business",
		Schemas: []Schema{businessSchema},
		Trust:   Trust{Serving: ServingSameUser()},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	lane := client.Business()
	if lane.session != nil {
		t.Fatal("Business() acquired a session before the first Call")
	}
	if lane.contract.Schema != businessSchema {
		t.Fatalf("Business() contract schema = %q, want %q", lane.contract.Schema, businessSchema)
	}
}

// forgedDrainPreamble is the two bytes a draining server emits instead of a
// hello ack. Any same-UID process can write them; nothing above the trust gate
// may believe them.
var forgedDrainPreamble = []byte{0x44, 0x52}

// TestBusinessAttachWritesNothingToAnUnjudgedSquatter is
// internal/wire's TestAuthorizeRefusalPreemptsAForgedDrain driven from the
// consumer's own entry point. A same-UID process unlinks the daemon's socket,
// binds the path first, and answers with a forged drain preamble; the client
// that attaches must write it nothing at all — the wire hello included — and
// must surface the authorize refusal rather than any daemon state the squatter
// offered it.
func TestBusinessAttachWritesNothingToAnUnjudgedSquatter(t *testing.T) {
	shortHome(t)
	const label = "dksquat"
	socket, err := paths.Socket(label)
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatalf("mkdir socket dir: %v", err)
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unlink socket: %v", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("squat listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	type squat struct {
		accepted bool
		received []byte
	}
	observed := make(chan squat, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			observed <- squat{}
			return
		}
		defer conn.Close()
		_, _ = conn.Write(forgedDrainPreamble)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		observed <- squat{accepted: true, received: buf[:n]}
	}()

	client, err := Open(Daemon{
		Label:   label,
		Schemas: []Schema{businessSchema},
		Trust: Trust{Serving: ServingSigned(Requirement{
			TeamID:            "SXKCTF23Q2",
			SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
		})},
	})
	if err != nil {
		t.Fatalf("Open() = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = client.Business().Call(ctx, echoOp, []byte("never"))
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Call() = %v, want ErrUntrusted for a peer that cannot prove the deployed identity", err)
	}
	forgeable := []struct {
		name string
		err  error
	}{
		{"ErrDraining", ErrDraining},
		{"wire.ErrBuildMismatch", wire.ErrBuildMismatch},
		{"wire.ErrHandshake", wire.ErrHandshake},
		{"ErrAbsent", ErrAbsent},
		{"ErrNotReady", ErrNotReady},
	}
	for _, forged := range forgeable {
		if errors.Is(err, forged.err) {
			t.Errorf("Call() = %v, want no %s a squatter that was never judged could forge", err, forged.name)
		}
	}
	if !Undispatched(err) {
		t.Error("Undispatched() = false for a request no byte of which was written")
	}
	squatter := <-observed
	if !squatter.accepted {
		t.Fatal("the client never reached the squatter holding the socket path")
	}
	if len(squatter.received) != 0 {
		t.Fatalf("the client wrote %#x to an unjudged peer, want nothing", squatter.received)
	}
}

// TestBusinessTrustSetRefusesAPeerNoDisjunctNames is Trust.Business reaching
// admission as a set rather than as a first element: a real daemon states two
// bundles, and a client neither names is refused as untrusted — the same
// verdict internal/trust's TestAnyOfIsADisjunctionOverFullRequirements pins
// element by element, observed from the consumer's Call.
func TestBusinessTrustSetRefusesAPeerNoDisjunctNames(t *testing.T) {
	shortHome(t)
	host, extension := hostAndExtension()
	d := Daemon{
		Label:    "dkbizset",
		Schemas:  []Schema{businessSchema},
		Shutdown: Grace(5 * time.Second),
		Trust:    Trust{Business: Requirements{host, extension}},
	}
	if (Requirements{host, extension}).Digest() != (Requirements{extension, host}).Digest() {
		t.Fatal("the stated set and its reverse are two policies")
	}
	done := make(chan error, 1)
	go func() {
		_, err := Serve(context.Background(), d, func(Ctx) (Product, error) { return &stubProduct{}, nil })
		done <- err
	}()
	socket, err := paths.Socket(string(d.Label))
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	_, err = openClient(t, d).Business().Call(ctx, echoOp, []byte("never"))
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Call() = %v, want ErrUntrusted from a set no disjunct of which names this peer", err)
	}
	if !Undispatched(err) {
		t.Error("Undispatched() = false for a handshake the server rejected")
	}
	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
}
