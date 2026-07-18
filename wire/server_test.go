package wire_test

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

type tenv struct {
	Op     string `json:"op"`
	Tenant string `json:"tenant,omitempty"`
}

func testRouter(frame []byte) (wire.Op, string, error) {
	var e tenv
	if err := json.Unmarshal(frame, &e); err != nil {
		return "", "", err
	}
	return wire.Op(e.Op), e.Tenant, nil
}

func frameBytes(t *testing.T, op, tenant string) []byte {
	t.Helper()
	b, err := json.Marshal(tenv{Op: op, Tenant: tenant})
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return b
}

type testServer struct {
	dial     func() net.Conn
	cancel   context.CancelFunc
	done     chan error
	waitOnce sync.Once
	runErr   error
}

func (ts *testServer) wait() error {
	ts.waitOnce.Do(func() { ts.runErr = <-ts.done })
	return ts.runErr
}

func startServer(t *testing.T, s *wire.Server) *testServer {
	t.Helper()
	sock := filepath.Join(wiretest.SocketDir(t), "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s.Listener = ln
	if s.Router == nil {
		s.Router = testRouter
	}
	ctx, cancel := context.WithCancel(context.Background())
	ts := &testServer{cancel: cancel, done: make(chan error, 1)}
	go func() { ts.done <- s.Run(ctx) }()
	ts.dial = func() net.Conn {
		c, derr := net.Dial("unix", sock)
		if derr != nil {
			t.Fatalf("dial: %v", derr)
		}
		return c
	}
	t.Cleanup(func() {
		cancel()
		_ = ts.wait()
	})
	return ts
}

// call sends one request and returns the classified result.
func call(t *testing.T, ts *testServer, op, tenant string) wire.Result {
	t.Helper()
	c := ts.dial()
	defer c.Close()
	res, err := wire.Do(c, frameBytes(t, op, tenant))
	if err != nil && res.Outcome == wire.Delivered {
		t.Fatalf("Do(%s): %v", op, err)
	}
	return res
}

func TestRegisterReservedOpPanics(t *testing.T) {
	for _, op := range []wire.Op{"health", "shutdown", "hello", "handoff"} {
		t.Run(string(op), func(t *testing.T) {
			s := &wire.Server{}
			defer func() {
				if recover() == nil {
					t.Errorf("RegisterControl(%q) did not panic", op)
				}
			}()
			s.RegisterControl(op, func(context.Context, wire.Request) (any, error) { return nil, nil })
		})
	}
}

func TestRegisterDuplicateOpPanics(t *testing.T) {
	s := &wire.Server{}
	h := func(context.Context, wire.Request) (any, error) { return nil, nil }
	s.RegisterControl("ping", h)
	defer func() {
		if recover() == nil {
			t.Error("duplicate registration did not panic")
		}
	}()
	s.RegisterConcurrent("ping", h)
}

func TestRunBootHooksInOrder(t *testing.T) {
	calls := make(chan string, 3)
	s := &wire.Server{
		OpenStore: func() error {
			calls <- "OpenStore"
			return nil
		},
		BootReconcile: func(context.Context) error {
			calls <- "BootReconcile"
			return nil
		},
		StartRealtimePlanes: func() error {
			calls <- "StartRealtimePlanes"
			return nil
		},
	}
	s.RegisterControl("ping", func(context.Context, wire.Request) (any, error) { return nil, nil })
	ts := startServer(t, s)

	if res := call(t, ts, "ping", ""); res.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered", res.Outcome)
	}
	ts.cancel()
	if err := ts.wait(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	close(calls)
	var got []string
	for call := range calls {
		got = append(got, call)
	}
	if want := []string{"OpenStore", "BootReconcile", "StartRealtimePlanes"}; !slices.Equal(got, want) {
		t.Errorf("boot hook order = %v, want %v", got, want)
	}
}

func TestTrustRejectionShortCircuits(t *testing.T) {
	var ran atomic.Bool
	s := &wire.Server{Trust: func(wire.Peer) error { return wire.ErrUntrustedPeer }}
	s.RegisterControl("ping", func(context.Context, wire.Request) (any, error) {
		ran.Store(true)
		return "pong", nil
	})
	ts := startServer(t, s)

	res := call(t, ts, "ping", "")
	if res.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered (a trust-denied error reply)", res.Outcome)
	}
	if res.Response.Err == "" {
		t.Error("trust-denied reply carried no Err")
	}
	if ran.Load() {
		t.Error("handler ran despite a trust denial")
	}
}

func TestControlHandlerDelivers(t *testing.T) {
	s := &wire.Server{Version: "v1.2.3"}
	s.RegisterControl("echo", func(_ context.Context, req wire.Request) (any, error) {
		return string(req.Frame), nil
	})
	ts := startServer(t, s)

	res := call(t, ts, "echo", "")
	if res.Outcome != wire.Delivered {
		t.Fatalf("Outcome = %v, want Delivered", res.Outcome)
	}
	if res.Response.Version != "v1.2.3" {
		t.Errorf("Version = %q, want v1.2.3", res.Response.Version)
	}
	var got string
	if err := json.Unmarshal(res.Response.Payload, &got); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if want := string(frameBytes(t, "echo", "")); got != want {
		t.Errorf("payload = %q, want %q", got, want)
	}
}

func TestUnknownOpReplyErr(t *testing.T) {
	ts := startServer(t, &wire.Server{})
	res := call(t, ts, "nope", "")
	if res.Outcome != wire.Delivered || res.Response.Err == "" {
		t.Fatalf("unknown op: Outcome=%v Err=%q, want a Delivered error reply", res.Outcome, res.Response.Err)
	}
}

func TestConcurrentPoolRejectionProvesNonDispatch(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	var execs atomic.Int64
	s := &wire.Server{Workers: 1, Backlog: 0}
	s.RegisterConcurrent("work", func(context.Context, wire.Request) (any, error) {
		execs.Add(1)
		started <- struct{}{}
		<-release
		return "done", nil
	})
	ts := startServer(t, s)

	// Occupy the one worker; the handler blocks until release.
	go func() {
		c := ts.dial()
		defer c.Close()
		_, _ = wire.Do(c, frameBytes(t, "work", ""))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first request was not admitted despite an idle worker")
	}

	// The pool is full: this request must be rejected without executing.
	res := call(t, ts, "work", "")
	if res.Outcome != wire.Rejected {
		t.Fatalf("Outcome = %v, want Rejected", res.Outcome)
	}
	if !res.Response.Rejected {
		t.Error("Response.Rejected = false, want true")
	}
	if res.Response.Reason == "" {
		t.Error("Rejected reply carried no Reason")
	}
	if got := execs.Load(); got != 1 {
		t.Errorf("handler executions = %d, want 1 (the rejected request must never run)", got)
	}
}

func TestExclusiveSerialization(t *testing.T) {
	var active, maxActive atomic.Int64
	s := &wire.Server{}
	s.RegisterExclusive("x", func(context.Context, wire.Request) (any, error) {
		n := active.Add(1)
		for {
			m := maxActive.Load()
			if n <= m || maxActive.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		active.Add(-1)
		return nil, nil
	})
	ts := startServer(t, s)

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := ts.dial()
			defer c.Close()
			_, _ = wire.Do(c, frameBytes(t, "x", ""))
		}()
	}
	wg.Wait()
	if got := maxActive.Load(); got != 1 {
		t.Errorf("max concurrent exclusive handlers = %d, want 1", got)
	}
}

func TestPerTenantSerializationWithoutCrossTenantBlocking(t *testing.T) {
	aGate := make(chan struct{})
	aIn := make(chan struct{}, 1)
	bIn := make(chan struct{})
	var aActive, aMax atomic.Int64
	var bRan atomic.Bool

	s := &wire.Server{Workers: 8, Backlog: 8}
	handler := func(_ context.Context, req wire.Request) (any, error) {
		switch req.Tenant {
		case "A":
			n := aActive.Add(1)
			for {
				m := aMax.Load()
				if n <= m || aMax.CompareAndSwap(m, n) {
					break
				}
			}
			select {
			case aIn <- struct{}{}:
			default:
			}
			<-aGate
			aActive.Add(-1)
		case "B":
			bRan.Store(true)
			close(bIn)
		}
		return nil, nil
	}
	s.RegisterConcurrent("op", handler)
	ts := startServer(t, s)

	var wg sync.WaitGroup
	fire := func(tenant string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := ts.dial()
			defer c.Close()
			_, _ = wire.Do(c, frameBytes(t, "op", tenant))
		}()
	}

	fire("A") // first A grabs the tenant gate and blocks on aGate
	<-aIn
	fire("A") // second A must wait on the same tenant gate
	fire("B") // different tenant: must run despite A holding its gate

	select {
	case <-bIn:
	case <-time.After(2 * time.Second):
		t.Fatal("tenant B blocked behind tenant A — cross-tenant serialization")
	}

	close(aGate)
	wg.Wait()
	if got := aMax.Load(); got != 1 {
		t.Errorf("max concurrent tenant-A handlers = %d, want 1", got)
	}
	if !bRan.Load() {
		t.Error("tenant B never ran")
	}
}

// TestConcurrentTenantBurstDoesNotStarveWorkers: a same-tenant burst larger than
// the worker pool must not occupy every worker — a second tenant still runs.
// With the gate acquired on a pool worker, the waiting A-requests would fill both
// workers and B could never be dequeued.
func TestConcurrentTenantBurstDoesNotStarveWorkers(t *testing.T) {
	aGate := make(chan struct{})
	aIn := make(chan struct{}, 1)
	bRan := make(chan struct{})

	s := &wire.Server{Workers: 2, Backlog: 8}
	handler := func(_ context.Context, req wire.Request) (any, error) {
		switch req.Tenant {
		case "A":
			select {
			case aIn <- struct{}{}:
			default:
			}
			<-aGate
		case "B":
			close(bRan)
		}
		return nil, nil
	}
	s.RegisterConcurrent("op", handler)
	ts := startServer(t, s)

	var wg sync.WaitGroup
	fire := func(tenant string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := ts.dial()
			defer c.Close()
			_, _ = wire.Do(c, frameBytes(t, "op", tenant))
		}()
	}

	fire("A") // grabs the tenant gate, occupies one worker, blocks on aGate
	<-aIn
	for i := 0; i < 4; i++ {
		fire("A") // same tenant: must NOT each seize a worker waiting on the gate
	}
	time.Sleep(150 * time.Millisecond) // let the waiters reach the gate
	fire("B")                          // different tenant: a worker must be free

	select {
	case <-bRan:
	case <-time.After(2 * time.Second):
		t.Fatal("tenant B starved: a same-tenant burst occupied every worker")
	}

	close(aGate)
	wg.Wait()
}

func TestDisconnectCancelsHandlerCtx(t *testing.T) {
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	s := &wire.Server{}
	s.RegisterControl("hang", func(ctx context.Context, _ wire.Request) (any, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})
	ts := startServer(t, s)

	c := ts.dial()
	if err := wire.NewFraming(c).WriteFrame(frameBytes(t, "hang", "")); err != nil {
		t.Fatalf("write: %v", err)
	}
	<-entered
	_ = c.Close() // peer disconnects mid-handler

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler ctx was not cancelled when the peer disconnected")
	}
}

func TestShutdownSettlesQueuedWork(t *testing.T) {
	const n = 4
	routed := make(chan struct{}, n)
	gate := make(chan struct{})
	var ran atomic.Int64

	s := &wire.Server{
		Workers: 1,
		Backlog: n,
		Router: func(frame []byte) (wire.Op, string, error) {
			op, tenant, err := testRouter(frame)
			routed <- struct{}{}
			return op, tenant, err
		},
	}
	s.RegisterConcurrent("work", func(context.Context, wire.Request) (any, error) {
		<-gate
		ran.Add(1)
		return nil, nil
	})
	ts := startServer(t, s)

	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := ts.dial()
			defer c.Close()
			_, _ = wire.Do(c, frameBytes(t, "work", ""))
		}()
	}
	// Every request is accepted and routed: one runs (blocked on gate), the rest queue.
	for range n {
		<-routed
	}

	ts.cancel() // begin shutdown while work is queued
	close(gate) // let the queued handlers settle

	if err := ts.wait(); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if got := ran.Load(); got != n {
		t.Errorf("settled handlers = %d, want %d (queued work dropped on shutdown)", got, n)
	}
	wg.Wait()
}

func TestOnActivityFiresPerAdmittedRequest(t *testing.T) {
	var activity atomic.Int64
	s := &wire.Server{}
	s.OnActivity(func() { activity.Add(1) })
	s.RegisterControl("ping", func(context.Context, wire.Request) (any, error) { return nil, nil })
	ts := startServer(t, s)

	const n = 3
	for range n {
		if res := call(t, ts, "ping", ""); res.Outcome != wire.Delivered {
			t.Fatalf("Outcome = %v, want Delivered", res.Outcome)
		}
	}
	if got := activity.Load(); got != n {
		t.Errorf("activity fires = %d, want %d", got, n)
	}
}
