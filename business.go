package daemonkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
)

// Business is the product dispatch lane of one daemon: unary,
// concurrency-safe, never replaying. Obtained three ways: Client.Business
// (the daemon's socket), Child.Business (a spawned child's handoff
// socketpair), and BusinessOverConn (a transport the caller hands over). The
// zero Business refuses Call with a config error naming all three.
type Business struct {
	mu       sync.Mutex
	contract Contract
	// attach acquires one fresh session and the pid of the process accepting
	// it, zero when the transport names none; nil on a single-session lane.
	attach  func(context.Context) (*wire.Client, int, error)
	session *wire.Client
	peer    int
	single  bool
	closed  bool
}

// Contract pins the application protocol both ends of a session speak when no
// Daemon value is in scope to say so.
type Contract struct {
	Schema      Schema
	MaxFrame    Bytes // 4 MiB when zero
	Concurrency int   // in-flight requests; 8 when zero
}

func (c Contract) validate() error {
	if c.Schema == "" {
		return errors.New("daemonkit: business lane names no schema (Daemon.Schemas[0] or Contract.Schema)")
	}
	return nil
}

// adopt reconciles a contract against the limits one handoff conveyed: zero
// adopts, equal agrees, and any other value refuses before a session exists,
// so the two ends of one handoff cannot skew.
func (c Contract) adopt(conveyed Limits) (Limits, error) {
	if c.MaxFrame != 0 && c.MaxFrame != conveyed.MaxFrame {
		return Limits{}, fmt.Errorf(
			"daemonkit: Contract.MaxFrame %d disagrees with the %d the spawn conveyed",
			c.MaxFrame, conveyed.MaxFrame,
		)
	}
	if c.Concurrency != 0 && c.Concurrency != conveyed.Concurrency {
		return Limits{}, fmt.Errorf(
			"daemonkit: Contract.Concurrency %d disagrees with the %d the spawn conveyed",
			c.Concurrency, conveyed.Concurrency,
		)
	}
	return conveyed, nil
}

func sessionLimits(limits Limits) wire.SessionLimits {
	return wire.SessionLimits{Concurrency: limits.Concurrency, MaxFrame: int(limits.MaxFrame)}
}

// ProductError is a product failure carried across the wire with the stable,
// product-chosen Code consumers match on. A Handle error that is not a
// *ProductError crosses as Code "". Wrapping around one does not cross: the
// Code and Message are carried verbatim and the wrapping is the daemon's own.
type ProductError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Error names the code before the message, or the message alone when the
// product chose no code.
func (e *ProductError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// attachAttempts bounds a momentarily full slot table. Capacity waits
// capacityBackoff between tries; a peer freeing a slot is what ends the wait,
// so a caller starved past the bound is told rather than held.
const (
	attachAttempts  = 3
	capacityBackoff = 25 * time.Millisecond
)

// businessEnvelope is one terminal on the business lane. A product failure is
// delivered data rather than a session error, so the wire's own Err field
// keeps meaning the session's failure and the two never have to be told apart
// at the far end. Body keeps the field name of the Reply it carries, so the
// success terminal decodes unchanged in a peer of an earlier era.
type businessEnvelope struct {
	Body  []byte        `json:"Body"`
	Error *ProductError `json:"error,omitempty"`
}

// callError carries the transport's own outcome classification beside the
// failure it explains, so Undispatched reads a proof instead of a message.
type callError struct {
	outcome wire.Outcome
	err     error
}

func (e *callError) Error() string { return e.err.Error() }

func (e *callError) Unwrap() error { return e.err }

// refused types a failure that provably never reached the wire.
func refused(err error) error { return &callError{outcome: wire.PreSendFailure, err: err} }

func remoteError(terminal *wire.TerminalError) *RemoteError {
	return &RemoteError{Message: terminal.Message, Err: terminal.Unwrap()}
}

// Business prepares the lane and performs no I/O, so it exists before the
// daemon does. The session is acquired on first Call and re-acquired after
// any session failure; a typed rejection is the peer's own answer on a session
// it proves alive by answering, so it retires nothing, and neither does a Call
// the caller's own deadline ends. The first acquisition takes over the
// session the client's own Health, Ensure, or WaitReady left open, so a
// command that converges and then calls opens one connection.
func (c *Client) Business() *Business {
	contract := c.daemon.contract()
	return &Business{
		contract: contract,
		attach: func(ctx context.Context) (*wire.Client, int, error) {
			if session, peer, ok := c.link.surrender(); ok {
				return session, peer, nil
			}
			return attachSocket(ctx, c.daemon)
		},
	}
}

func (d Daemon) contract() Contract {
	return Contract{Schema: d.ownSchema(), MaxFrame: d.MaxFrame, Concurrency: d.Concurrency}
}

// attachSocket dials the daemon's socket and completes the handshake under
// the daemon's own schema, reading the accepting process's pid off the
// connection the session then uses.
func attachSocket(ctx context.Context, d Daemon) (*wire.Client, int, error) {
	el, err := d.Label.element()
	if err != nil {
		return nil, 0, err
	}
	socket, err := el.socket()
	if err != nil {
		return nil, 0, fmt.Errorf("daemonkit: derive socket path: %w", err)
	}
	var peer int
	dial := wire.UnixDialer(socket)
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: func(ctx context.Context) (net.Conn, error) {
			conn, err := dial(ctx)
			if err != nil {
				return nil, err
			}
			creds, err := proc.PeerCredentials(conn.(*net.UnixConn))
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			peer = creds.PID
			return conn, nil
		},
		Schema:      string(d.ownSchema()),
		MaxFrame:    int(d.MaxFrame),
		Concurrency: d.Concurrency,
	})
	if err != nil {
		return nil, 0, err
	}
	return session, peer, nil
}

// Business attaches the lane to a spawned child over its ChannelHandoff
// socketpair. The pair was fd-passed to a process this parent exec'd, and the
// attach nonce proves the far end inherited fd 3 from that exec. Single-session: a terminal failure is terminal and a later
// Call returns ErrLaneClosed. Handoff-only — a Child spawned on any other
// channel is a named refusal — and mutually exclusive with Child.Conn.
// contract's limits must be zero or equal to the Cmd.Limits the spawn
// conveyed; disagreement refuses.
func (c *Child) Business(ctx context.Context, contract Contract) (*Business, error) {
	if err := contract.validate(); err != nil {
		return nil, err
	}
	if c.channel != ChannelHandoff {
		return nil, fmt.Errorf(
			"daemonkit: Child.Business rides the ChannelHandoff socketpair; this child was spawned on channel %d",
			c.channel,
		)
	}
	limits, err := contract.adopt(c.limits)
	if err != nil {
		return nil, err
	}
	conn, err := c.takeChannel()
	if err != nil {
		return nil, err
	}
	session, err := wire.NewSpawnedClient(ctx, wire.SpawnedClientConfig{
		Conn:   conn,
		Nonce:  c.nonce,
		Schema: string(contract.Schema),
		Limits: sessionLimits(limits),
	})
	if err != nil {
		_ = conn.Close()
		return nil, classifyWire(err)
	}
	return &Business{
		contract: Contract{Schema: contract.Schema, MaxFrame: limits.MaxFrame, Concurrency: limits.Concurrency},
		session:  session,
		single:   true,
	}, nil
}

// BusinessOverConn attaches the lane over a transport the caller hands over —
// an ssh pipe, a spawned child's stdio conn, an in-process test pair. Limit
// agreement between the ends is the caller's to arrange, and the daemon-side
// Request.Caller names the immediate proxy process, never the originator. A
// product that needs the true caller carries it in the payload.
// Single-session: a terminal failure is terminal and a later Call returns
// ErrLaneClosed.
func BusinessOverConn(ctx context.Context, conn net.Conn, contract Contract) (*Business, error) {
	if err := contract.validate(); err != nil {
		return nil, err
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:        func(context.Context) (net.Conn, error) { return conn, nil },
		Schema:      string(contract.Schema),
		MaxFrame:    int(contract.MaxFrame),
		Concurrency: contract.Concurrency,
	})
	if err != nil {
		return nil, classifyWire(err)
	}
	return &Business{contract: contract, session: session, single: true}, nil
}

// Call sends one request and returns its terminal reply. ctx must carry a
// deadline; the deadline rides the wire into the product handler's ctx.
// Refusal and failure are disjoint branches: ErrAbsent (proven no-listener),
// ErrNotReady (retryable; wait with Client.WaitReady), ErrDraining,
// ErrProtocolMismatch, and ErrOversize are typed refusals that provably never
// dispatched, as is a daemon refusing this lane's schema; *ProductError is the product's own delivered
// failure and *RemoteError the daemon session's, unwrapping to
// context.DeadlineExceeded when the conveyed deadline ended on that side;
// anything else is transport loss or the caller's own expired context, with
// delivery unknown.
func (b *Business) Call(ctx context.Context, op string, body []byte) (Reply, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Reply{}, errors.New("daemonkit: Call requires a context deadline")
	}
	if err := b.contract.validate(); err != nil {
		return Reply{}, refused(err)
	}
	if limit := maxFramedBytes(b.contract.MaxFrame); Bytes(len(body)) > limit {
		return Reply{}, refused(fmt.Errorf("%w: %d bytes over the %d the session carries", ErrOversize, len(body), limit))
	}
	session, _, err := b.acquire(ctx)
	if err != nil {
		return Reply{}, refused(err)
	}
	result, err := session.Call(ctx, wire.Op(op), body)
	if retiring(session, result.Outcome) {
		b.retire(session)
	}
	if rejection := result.Rejection(); rejection != nil {
		return Reply{}, &callError{outcome: result.Outcome, err: classifyWire(rejection)}
	}
	if err != nil {
		return Reply{}, &callError{outcome: result.Outcome, err: err}
	}
	if terminal := result.Terminal(); terminal != nil {
		return Reply{}, &callError{
			outcome: result.Outcome,
			err:     fmt.Errorf("daemonkit: business session: %w", remoteError(terminal)),
		}
	}
	var envelope businessEnvelope
	if err := json.Unmarshal(result.Response.Payload, &envelope); err != nil {
		return Reply{}, &callError{
			outcome: result.Outcome,
			err:     fmt.Errorf("daemonkit: decode business terminal: %w", err),
		}
	}
	if envelope.Error != nil {
		return Reply{}, &callError{outcome: result.Outcome, err: envelope.Error}
	}
	return Reply{Body: envelope.Body}, nil
}

// Close releases whatever session the lane holds and refuses every later Call
// with ErrLaneClosed. Like every other verb it refuses a context without a
// deadline, and it honors the one it is given.
func (b *Business) Close(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("daemonkit: Close requires a context deadline")
	}
	b.mu.Lock()
	session := b.session
	b.session = nil
	b.closed = true
	b.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close(ctx)
}

// acquire returns the lane's live session and the pid of the process that
// accepted it, attaching one when the lane holds none.
func (b *Business) acquire(ctx context.Context) (*wire.Client, int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.session != nil {
		if b.single || b.session.Failure() == nil {
			return b.session, b.peer, nil
		}
		_ = b.session.Abort(nil)
		b.session = nil
	}
	if b.closed {
		return nil, 0, ErrLaneClosed
	}
	if b.attach == nil {
		return nil, 0, errors.New("daemonkit: zero Business (Client.Business, Child.Business, or BusinessOverConn)")
	}
	for attempt := 1; ; attempt++ {
		session, peer, err := b.attach(ctx)
		if err == nil {
			b.session, b.peer = session, peer
			return session, peer, nil
		}
		denial := classifyWire(err)
		if !errors.Is(denial, ErrSessionCapacity) || attempt == attachAttempts || !sleepUntil(ctx, capacityBackoff) {
			return nil, 0, denial
		}
	}
}

// surrender hands the lane's live session to whoever takes it, leaving the
// lane to attach afresh on its next use.
func (b *Business) surrender() (*wire.Client, int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	session, peer := b.session, b.peer
	if session == nil || session.Failure() != nil {
		return nil, 0, false
	}
	b.session, b.peer = nil, 0
	return session, peer, true
}

// health reads the daemon's report on the lane's session. A session the lane
// already held may have outlived its peer before the transport noticed, so a
// failure on one is retired and asked once more on a fresh attach.
func (b *Business) health(ctx context.Context) (wire.HealthReport, error) {
	_, _, report, err := b.ask(ctx, func(session *wire.Client, _ int) (proc.Identity, wire.HealthReport, error) {
		report, err := session.Health(ctx)
		return proc.Identity{}, report, err
	})
	return report, err
}

// pinned reads the daemon's report on the lane's session and pins the kernel
// process instance answering it, retrying a held session once as health does.
func (b *Business) pinned(ctx context.Context) (*wire.Client, proc.Identity, wire.HealthReport, error) {
	return b.ask(ctx, func(session *wire.Client, peer int) (proc.Identity, wire.HealthReport, error) {
		return assemblePin(ctx, os.Getpid(), peer, session)
	})
}

func (b *Business) ask(
	ctx context.Context,
	read func(*wire.Client, int) (proc.Identity, wire.HealthReport, error),
) (*wire.Client, proc.Identity, wire.HealthReport, error) {
	for attempt := 0; ; attempt++ {
		session, peer, err := b.acquire(ctx)
		if err != nil {
			return nil, proc.Identity{}, wire.HealthReport{}, err
		}
		pinned, report, err := read(session, peer)
		if err == nil {
			return session, pinned, report, nil
		}
		b.retire(session)
		if attempt > 0 || session.Failure() == nil {
			return nil, proc.Identity{}, wire.HealthReport{}, classifyWire(err)
		}
	}
}

func sleepUntil(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

// retiring asks the transport whether it is broken instead of reading it off
// an outcome that conflates two causes: a caller's own expired context
// produces the same PreSendFailure and PostSendFailure a severed socket does,
// on a peer that is still answering. DeliveryUnknown is the one outcome that
// answers by itself — a half-written frame desyncs the stream whoever caused
// it, and the writer publishes that state before it fails the session.
func retiring(session *wire.Client, outcome wire.Outcome) bool {
	return session.Failure() != nil || outcome == wire.DeliveryUnknown
}

// retire drops the session a Call just failed on, so the next Call attaches
// to whoever accepts then. A single-session lane has no second one to
// acquire: its retirement closes it.
func (b *Business) retire(session *wire.Client) {
	b.mu.Lock()
	current := b.session == session
	if current {
		b.session = nil
		if b.single {
			b.closed = true
		}
	}
	b.mu.Unlock()
	if current {
		_ = session.Abort(nil)
	}
}

// Undispatched reports whether err proves the request never reached product
// dispatch, and may therefore be resent without risk of double execution. The
// proof is the transport's own outcome classification (a rejected or
// never-written request), not an error-string taxonomy; typed refusals are
// undispatched by construction. It is a safety predicate: true guarantees a
// safe resend, false means unknown — never "dispatched".
func Undispatched(err error) bool {
	var call *callError
	if !errors.As(err, &call) {
		return false
	}
	return call.outcome.Replayable()
}

// handleBusiness is the one terminal encode both business servers share, so a
// daemon session and a spawned session put the same bytes on the wire and one
// Call decodes either.
func handleBusiness(ctx context.Context, req wire.Request, handle Handler) (any, error) {
	reply, err := handle(ctx, requestOf(req))
	if err != nil {
		return businessEnvelope{Error: productError(err)}, nil
	}
	return businessEnvelope{Body: reply.Body}, nil
}

func requestOf(req wire.Request) Request {
	return Request{
		Op:      string(req.Op),
		Body:    req.Payload,
		Caller:  Caller{UID: uint32(req.Peer.UID), PID: req.Peer.PID}, //nolint:gosec // kernel UIDs are non-negative
		Session: Session{id: req.Session.ID(), done: req.Session.Done(), disconnected: req.Session.Disconnected()},
	}
}

// productError is what one Handle failure crosses as. A product that chose a
// Code keeps it; anything else crosses as Code "" carrying the error's own
// text.
func productError(err error) *ProductError {
	var carried *ProductError
	if errors.As(err, &carried) {
		return &ProductError{Code: carried.Code, Message: carried.Message}
	}
	return &ProductError{Message: err.Error()}
}
