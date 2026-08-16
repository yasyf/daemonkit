package daemonkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/yasyf/daemonkit/internal/wire"
)

// Business is the product dispatch lane of one daemon: unary,
// concurrency-safe, never replaying. Obtained three ways, each naming its
// authentication: Client.Business (kernel-verified against Trust.Serving),
// Child.Business (directional confinement over a spawned child's handoff
// socketpair), and BusinessOverConn (the caller's own transport
// authentication, by name). The zero Business refuses Call with a config error
// naming all three.
type Business struct {
	mu       sync.Mutex
	contract Contract
	// attach acquires one fresh kernel-verified session; nil on the lane whose
	// single session the caller authenticated and handed over.
	attach  func(context.Context) (*wire.Client, error)
	session *wire.Client
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

// peerGoneAttempts bounds the daemon-restart race: a serving peer whose
// execution generation ends mid-attach is re-attached this many times before
// ErrPeerGone surfaces.
const peerGoneAttempts = 3

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
// the caller's own deadline ends. Each acquisition runs
// the same-EUID floor and the
// Trust.Serving verify on the live socket peer's audit token before any byte,
// the wire hello included, is written; there is no path to dispatch, or to the
// handshake, that skips it. Nothing survives a retirement but immutable
// config: no memoized peer, no verified-PID cache — every acquisition verifies
// whoever accepts now. A peer that exits mid-attach is the normal
// daemon-restart race: acquisition retries it a bounded number of times before
// surfacing ErrPeerGone. ErrUntrusted — a live peer that failed the
// requirement — is never retried.
func (c *Client) Business() *Business {
	d := c.daemon
	contract := Contract{Schema: d.ownSchema(), MaxFrame: d.MaxFrame, Concurrency: d.Concurrency}
	return &Business{
		contract: contract,
		attach: func(ctx context.Context) (*wire.Client, error) {
			return attachBusiness(ctx, d, contract)
		},
	}
}

func attachBusiness(ctx context.Context, d Daemon, contract Contract) (*wire.Client, error) {
	el, err := d.Label.element()
	if err != nil {
		return nil, err
	}
	socket, err := el.socket()
	if err != nil {
		return nil, fmt.Errorf("daemonkit: derive socket path: %w", err)
	}
	return wire.NewClient(ctx, wire.ClientConfig{
		Dial:        wire.UnixDialer(socket),
		Lane:        wire.LaneBusiness,
		Schema:      string(contract.Schema),
		MaxFrame:    int(contract.MaxFrame),
		Concurrency: contract.Concurrency,
		Authorize: func(conn net.Conn) error {
			_, err := authorizeServer(conn.(*net.UnixConn), d.Trust.Serving)
			return err
		},
	})
}

// Business attaches the lane to a spawned child over its ChannelHandoff
// socketpair. Its property is directional confinement, not peer identity: the
// pair was fd-passed to a process this parent exec'd, and the attach nonce
// proves the far end inherited fd 3 from that exec — no kernel verification of
// the child runs here, and none could name a different process. What runs
// behind the descriptor is whatever Cmd.Path held, under the Cmd.Exec posture
// the spawn stated. Single-session: a terminal failure is terminal and a later
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

// BusinessOverConn attaches the lane over a transport the caller authenticated
// itself — an ssh pipe, a spawned child's stdio conn, an in-process test pair.
// It is the explicit waiver: no kernel peer credentials exist here,
// Trust.Serving cannot run, limit agreement between the ends is the caller's
// to arrange, and the daemon-side Request.Caller names the immediate proxy
// process, never the originator. A product that needs the true caller carries
// it in the payload. Single-session: a terminal failure is terminal and a
// later Call returns ErrLaneClosed.
func BusinessOverConn(ctx context.Context, conn net.Conn, contract Contract) (*Business, error) {
	if err := contract.validate(); err != nil {
		return nil, err
	}
	session, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial:        func(context.Context) (net.Conn, error) { return conn, nil },
		Authorize:   authorizeCallerAuthenticated,
		Lane:        wire.LaneBusiness,
		Schema:      string(contract.Schema),
		MaxFrame:    int(contract.MaxFrame),
		Concurrency: contract.Concurrency,
	})
	if err != nil {
		return nil, classifyWire(err)
	}
	return &Business{contract: contract, session: session, single: true}, nil
}

// authorizeCallerAuthenticated is BusinessOverConn's named waiver: the caller
// authenticated the transport before handing it over, and there are no kernel
// peer credentials on it to judge.
func authorizeCallerAuthenticated(net.Conn) error { return nil }

// Call sends one request and returns its terminal reply. ctx must carry a
// deadline; the deadline rides the wire into the product handler's ctx.
// Refusal and failure are disjoint branches: ErrAbsent (proven no-listener),
// ErrNotReady (retryable; wait with Client.WaitReady), ErrDraining,
// ErrUntrusted, ErrPeerGone, ErrNoVerifier, and ErrOversize are typed refusals
// that provably never dispatched; *ProductError is the product's own delivered
// failure and *RemoteError the daemon session's, unwrapping to
// context.DeadlineExceeded when the conveyed deadline ended on that side;
// anything else is transport loss or the caller's own expired context, with
// delivery unknown.
func (b *Business) Call(ctx context.Context, op string, body []byte) (Reply, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Reply{}, errors.New("daemonkit: Call requires a context deadline")
	}
	if limit := maxFramedBytes(b.contract.MaxFrame); Bytes(len(body)) > limit {
		return Reply{}, refused(fmt.Errorf("%w: %d bytes over the %d the session carries", ErrOversize, len(body), limit))
	}
	session, err := b.acquire(ctx)
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

func (b *Business) acquire(ctx context.Context) (*wire.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.session != nil {
		return b.session, nil
	}
	if b.closed {
		return nil, ErrLaneClosed
	}
	if b.attach == nil {
		return nil, errors.New("daemonkit: zero Business (Client.Business, Child.Business, or BusinessOverConn)")
	}
	if err := b.contract.validate(); err != nil {
		return nil, err
	}
	var denial error
	for range peerGoneAttempts {
		session, err := b.attach(ctx)
		if err == nil {
			b.session = session
			return session, nil
		}
		denial = classifyWire(err)
		if !errors.Is(denial, ErrPeerGone) {
			return nil, denial
		}
	}
	return nil, denial
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

// retire drops the session a Call just failed on, so the next Call verifies
// whoever accepts then. A lane whose single session the caller authenticated
// has no second one to acquire: its retirement closes it.
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
// safe resend, false means unknown — never "dispatched" — and it never turns
// ErrUntrusted into a retry.
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
		Caller:  Caller{UID: uint32(req.Peer.UID), PID: req.Peer.Token.PID()}, //nolint:gosec // kernel UIDs are non-negative
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
