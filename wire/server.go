package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// ErrUntrustedPeer is the floor trust verdict: a peer whose uid differs from the
// server process's own, refused when no Trust hook overrides the check.
var ErrUntrustedPeer = errors.New("wire: untrusted peer")

// defaultWorkers bounds concurrent-handler parallelism when Workers is unset.
const defaultWorkers = 8

// requestGrace bounds the initial request read when RequestTimeout is unset, so a
// client that connects but never sends a frame cannot pin a connection goroutine
// past shutdown.
const requestGrace = 15 * time.Second

// disconnectPoke is the read-deadline interval the per-request disconnect watcher
// polls at.
const disconnectPoke = 25 * time.Millisecond

// writeGrace bounds a response write when WriteTimeout is unset, so a stuck write
// cannot stall shutdown.
const writeGrace = 10 * time.Second

// reservedOps are the lifecycle op names the daemon layer owns; registering any
// of them panics so a consumer handler can never shadow one.
var reservedOps = map[Op]struct{}{
	"health":   {},
	"shutdown": {},
	"hello":    {},
	"handoff":  {},
}

type class int

const (
	classControl class = iota
	classConcurrent
	classExclusive
)

type entry struct {
	class class
	h     Handler
}

type job struct {
	ctx  context.Context
	req  Request
	h    Handler
	done chan result
}

type result struct {
	val any
	err error
}

// Server dispatches LF-framed JSON requests over an already-bound net.Listener.
// It never binds the listener itself. Registration and the OnActivity/hook
// wiring happen before Run; the handler table is read-only once Run starts.
//
// Dispatch disciplines: control handlers run inline on the accepting goroutine;
// concurrent handlers run in a bounded worker pool that replies Rejected when
// full; exclusive handlers serialize on one shared mutex. Orthogonally, a request
// with a non-empty tenant key serializes against same-tenant requests without
// blocking other tenants.
type Server struct {
	// Listener is the pre-bound listener to accept on. Required; the Server never
	// binds it and does not close it beyond intake-stop on shutdown.
	Listener net.Listener
	// Router extracts each frame's op and tenant key. Required.
	Router Router
	// Version stamps every Response verbatim; it is the CONSUMER's version, never
	// wire's own.
	Version string
	// Trust gates every request before any handler, keyed on the peer's OS
	// credentials. nil applies the same-uid floor (ErrUntrustedPeer otherwise).
	Trust func(Peer) error
	// Ladder bounds each op's server-side handling; an op absent from the ladder
	// runs without a deadline. The zero Ladder imposes none.
	Ladder Ladder
	// Workers caps concurrent-handler parallelism; <=0 uses defaultWorkers.
	Workers int
	// Backlog is the concurrent-pool queue depth beyond the running workers;
	// requests past Workers+Backlog get a Rejected reply.
	Backlog int
	// RequestTimeout bounds reading the request frame; <=0 uses requestGrace.
	RequestTimeout time.Duration
	// WriteTimeout bounds writing the response frame; <=0 uses writeGrace.
	WriteTimeout time.Duration
	// OpenStore, BootReconcile, StartRealtimePlanes run in that order before Accept.
	OpenStore           func() error
	BootReconcile       func(ctx context.Context) error
	StartRealtimePlanes func() error
	// CloseResources releases consumer resources last on shutdown (and on a
	// startup error after OpenStore succeeded).
	CloseResources func() error
	// Log receives accept-loop diagnostics; nil uses slog.Default.
	Log *slog.Logger

	handlers   map[Op]entry
	onActivity func()

	cancelServe context.CancelFunc

	queue       chan job
	slots       chan struct{}
	exclusiveMu sync.Mutex
	tenants     *tenantGates

	connWG       sync.WaitGroup
	poolWG       sync.WaitGroup
	closeOnce    sync.Once
	shutdownOnce sync.Once
}

// RegisterControl registers op as a control handler that runs inline on the
// accepting goroutine, outside the pool. Panics on a reserved or duplicate op.
func (s *Server) RegisterControl(op Op, h Handler) { s.register(op, classControl, h) }

// RegisterConcurrent registers op as a pooled handler; requests past the pool's
// Workers+Backlog capacity get a Rejected reply. Panics on a reserved or
// duplicate op.
func (s *Server) RegisterConcurrent(op Op, h Handler) { s.register(op, classConcurrent, h) }

// RegisterExclusive registers op as an exclusive handler; all exclusive handlers
// serialize on one shared mutex. Panics on a reserved or duplicate op.
func (s *Server) RegisterExclusive(op Op, h Handler) { s.register(op, classExclusive, h) }

func (s *Server) register(op Op, c class, h Handler) {
	if _, ok := reservedOps[op]; ok {
		panic(fmt.Sprintf("wire: op %q is a reserved lifecycle op", op))
	}
	if s.handlers == nil {
		s.handlers = map[Op]entry{}
	}
	if _, dup := s.handlers[op]; dup {
		panic(fmt.Sprintf("wire: op %q already registered", op))
	}
	s.handlers[op] = entry{class: c, h: h}
}

// OnActivity registers f, invoked once per admitted request (never for a Rejected
// non-dispatch or a trust denial). Set it before Run.
func (s *Server) OnActivity(f func()) { s.onActivity = f }

// Run opens the store, boot-reconciles, starts the realtime planes, then accepts
// until ctx is cancelled or the listener closes. Shutdown — reached on ctx
// cancel, listener close, or a startup error after OpenStore — stops intake,
// cancels the serve context, waits for admitted running and queued handlers to
// settle, then closes consumer resources. Run returns ctx.Err (nil on a clean
// listener close) or the startup error that aborted it.
func (s *Server) Run(ctx context.Context) error {
	if s.Listener == nil {
		return errors.New("wire: Server.Listener is required")
	}
	if s.Router == nil {
		return errors.New("wire: Server.Router is required")
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	serveCtx, cancelServe := context.WithCancel(ctx)
	s.cancelServe = cancelServe
	s.tenants = newTenantGates()
	workers := s.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	s.queue = make(chan job, s.Backlog)
	s.slots = make(chan struct{}, workers+s.Backlog)
	for range workers {
		s.poolWG.Add(1)
		go s.worker()
	}

	if s.OpenStore != nil {
		if err := s.OpenStore(); err != nil {
			s.shutdown(false)
			return fmt.Errorf("wire: open store: %w", err)
		}
	}
	if s.BootReconcile != nil {
		if err := s.BootReconcile(serveCtx); err != nil {
			s.shutdown(true)
			return fmt.Errorf("wire: boot reconcile: %w", err)
		}
	}
	if s.StartRealtimePlanes != nil {
		if err := s.StartRealtimePlanes(); err != nil {
			s.shutdown(true)
			return fmt.Errorf("wire: start realtime planes: %w", err)
		}
	}

	go func() {
		<-serveCtx.Done()
		s.closeListener()
	}()
	s.acceptLoop(serveCtx)
	s.shutdown(true)
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			s.Log.Warn("wire: accept", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			s.handle(ctx, conn)
		}()
	}
}

func (s *Server) closeListener() {
	s.closeOnce.Do(func() { _ = s.Listener.Close() })
}

// shutdown runs the documented teardown: stop intake, cancel the serve context,
// wait for admitted running and queued handlers to settle (their connection
// goroutines block until the pool drains), then close consumer resources.
// closeResources is false only on the OpenStore-failure path, where nothing was
// opened to close.
func (s *Server) shutdown(closeResources bool) {
	s.shutdownOnce.Do(func() {
		s.closeListener()
		s.cancelServe()
		s.connWG.Wait()
		close(s.queue)
		s.poolWG.Wait()
		if closeResources && s.CloseResources != nil {
			if err := s.CloseResources(); err != nil {
				s.Log.Warn("wire: close resources", "err", err)
			}
		}
	})
}

func (s *Server) worker() {
	defer s.poolWG.Done()
	for j := range s.queue {
		val, err := s.invoke(j.ctx, j.req, j.h)
		<-s.slots
		j.done <- result{val: val, err: err}
	}
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return
	}
	f := NewFraming(conn)
	f.ReadTimeout = s.requestTimeout()
	f.WriteTimeout = s.writeTimeout()

	peer, err := PeerFromConn(unix)
	if err != nil {
		_ = f.WriteJSON(s.errResp(fmt.Sprintf("peer: %v", err)))
		return
	}
	// Unblock the pre-frame read on shutdown: an idle connection would otherwise
	// hold connWG until its read deadline, delaying teardown.
	readDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-readDone:
		}
	}()
	frame, err := f.ReadFrame()
	close(readDone)
	if err != nil {
		return
	}
	// Trust gates the handler, checked after the frame read so a denial consumes
	// the client's request and delivers the rejection instead of racing a close.
	if terr := s.trust(peer); terr != nil {
		_ = f.WriteJSON(s.errResp(terr.Error()))
		return
	}
	op, tenant, rerr := s.Router(frame)
	if rerr != nil {
		_ = f.WriteJSON(s.errResp(fmt.Sprintf("route: %v", rerr)))
		return
	}
	e, ok := s.handlers[op]
	if !ok {
		_ = f.WriteJSON(s.errResp(fmt.Sprintf("unknown op %q", op)))
		return
	}

	reqCtx, cancel := s.requestContext(ctx, op)
	defer cancel()
	stop := s.watchDisconnect(reqCtx, cancel, conn)

	req := Request{Op: op, Tenant: tenant, Peer: peer, Frame: frame}
	resp := s.dispatch(reqCtx, e, req)

	stop()
	_ = f.WriteJSON(resp)
}

// requestContext derives the per-request context from the serve context,
// bounding it by the op's server deadline when the ladder carries one.
func (s *Server) requestContext(ctx context.Context, op Op) (context.Context, context.CancelFunc) {
	if server, _, ok := s.Ladder.Deadlines(op); ok {
		return context.WithTimeout(ctx, server)
	}
	return context.WithCancel(ctx)
}

func (s *Server) dispatch(ctx context.Context, e entry, req Request) Response {
	// Gate the tenant on the connection goroutine, before any pool worker, so a
	// same-tenant burst blocks its own connections, not other tenants' workers.
	if req.Tenant != "" {
		release, err := s.tenants.acquire(ctx, req.Tenant)
		if err != nil {
			return s.respond(nil, err)
		}
		defer release()
	}
	switch e.class {
	case classControl:
		val, err := s.invoke(ctx, req, e.h)
		return s.respond(val, err)
	case classExclusive:
		s.exclusiveMu.Lock()
		defer s.exclusiveMu.Unlock()
		val, err := s.invoke(ctx, req, e.h)
		return s.respond(val, err)
	case classConcurrent:
		j := job{ctx: ctx, req: req, h: e.h, done: make(chan result, 1)}
		select {
		case s.slots <- struct{}{}:
		default:
			return s.rejected("concurrent pool at capacity")
		}
		// Admitted work always enqueues and drains, ctx cancellation included.
		s.queue <- j
		r := <-j.done
		return s.respond(r.val, r.err)
	default:
		return s.errResp(fmt.Sprintf("unhandled dispatch class %d", e.class))
	}
}

// invoke fires the activity signal once, then runs the handler. The tenant gate
// is held by the caller so it never occupies a pool worker while waiting.
func (s *Server) invoke(ctx context.Context, req Request, h Handler) (any, error) {
	if s.onActivity != nil {
		s.onActivity()
	}
	return h(ctx, req)
}

func (s *Server) respond(val any, err error) Response {
	if err != nil {
		return Response{Version: s.Version, Err: err.Error()}
	}
	payload, merr := json.Marshal(val)
	if merr != nil {
		return Response{Version: s.Version, Err: fmt.Sprintf("wire: marshal response: %v", merr)}
	}
	return Response{Version: s.Version, Payload: payload}
}

func (s *Server) rejected(reason string) Response {
	return Response{Version: s.Version, Rejected: true, Reason: reason}
}

func (s *Server) errResp(msg string) Response {
	return Response{Version: s.Version, Err: "wire: " + msg}
}

// trust enforces the same-effective-UID floor on every connection before any
// stricter check, then runs the optional Trust callback. The floor is never
// replaced by a non-nil callback — a callback augments it, never bypasses it.
func (s *Server) trust(p Peer) error {
	if p.UID != os.Geteuid() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, p.UID, os.Geteuid())
	}
	if s.Trust != nil {
		return s.Trust(p)
	}
	return nil
}

func (s *Server) requestTimeout() time.Duration {
	if s.RequestTimeout > 0 {
		return s.RequestTimeout
	}
	return requestGrace
}

func (s *Server) writeTimeout() time.Duration {
	if s.WriteTimeout > 0 {
		return s.WriteTimeout
	}
	return writeGrace
}

// watchDisconnect starts a goroutine that polls conn's read side while the
// handler runs and cancels the request context the moment the peer goes away.
// Detection is a short-read-deadline poke: a repeated one-byte read that times
// out while the peer is present and returns EOF/reset once it disconnects (a
// one-request-per-connection peer sends nothing more after its frame). The
// returned stop drains the watcher and clears the read deadline before the reply
// write. cancel doubles as the op-deadline cancel, so a fired deadline stops the
// watcher too.
func (s *Server) watchDisconnect(ctx context.Context, cancel context.CancelFunc, conn net.Conn) func() {
	stopped := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		buf := make([]byte, 1)
		for {
			select {
			case <-stopped:
				return
			case <-ctx.Done():
				return
			default:
			}
			_ = conn.SetReadDeadline(time.Now().Add(disconnectPoke))
			_, err := conn.Read(buf)
			switch {
			case err == nil:
				continue
			case errors.Is(err, os.ErrDeadlineExceeded):
				continue
			default:
				cancel()
				return
			}
		}
	}()
	return func() {
		close(stopped)
		_ = conn.SetReadDeadline(time.Now())
		<-exited
		_ = conn.SetReadDeadline(time.Time{})
	}
}

// tenantGates serializes requests sharing a tenant key without blocking other
// tenants. Each key maps to a capacity-1 channel used as a mutex, reference
// counted so an idle key drops out of the map.
type tenantGates struct {
	mu    sync.Mutex
	gates map[string]*tenantGate
}

type tenantGate struct {
	ch   chan struct{}
	refs int
}

func newTenantGates() *tenantGates {
	return &tenantGates{gates: map[string]*tenantGate{}}
}

func (t *tenantGates) acquire(ctx context.Context, key string) (func(), error) {
	t.mu.Lock()
	g := t.gates[key]
	if g == nil {
		g = &tenantGate{ch: make(chan struct{}, 1)}
		t.gates[key] = g
	}
	g.refs++
	t.mu.Unlock()

	select {
	case g.ch <- struct{}{}:
		return func() { t.release(key, g, true) }, nil
	case <-ctx.Done():
		t.release(key, g, false)
		return nil, ctx.Err()
	}
}

func (t *tenantGates) release(key string, g *tenantGate, held bool) {
	if held {
		<-g.ch
	}
	t.mu.Lock()
	g.refs--
	if g.refs == 0 {
		delete(t.gates, key)
	}
	t.mu.Unlock()
}
