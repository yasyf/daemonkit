package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrCallDone means a request stream was used after terminal settlement.
	ErrCallDone = errors.New("wire: call already settled")
	// ErrCancelSettlement means a canceled request never produced a terminal response.
	ErrCancelSettlement = errors.New("wire: canceled call did not settle")
	// ErrClientAbort means the local owner intentionally tore down the session.
	ErrClientAbort = errors.New("wire: client aborted session")
	// ErrClientClosing means the local owner began graceful session closure.
	ErrClientClosing = errors.New("wire: client closing session")
)

const defaultCancelSettlementTimeout = 5 * time.Second

// Outcome classifies what the persistent transport proved about one request.
type Outcome int

const (
	// Delivered means a complete terminal response arrived.
	Delivered Outcome = iota
	// PreSendFailure means the request frame was not written completely.
	PreSendFailure
	// Rejected proves the server did not dispatch the request.
	Rejected
	// PostSendFailure means the request was sent but no terminal response
	// arrived; never auto-replay it.
	PostSendFailure
	// DeliveryUnknown means the transport accepted the request for writing but
	// could not prove whether the peer observed it. It is never replayable.
	DeliveryUnknown
)

// String names the outcome for diagnostics.
func (o Outcome) String() string {
	switch o {
	case Delivered:
		return "delivered"
	case PreSendFailure:
		return "pre-send-failure"
	case Rejected:
		return "rejected"
	case PostSendFailure:
		return "post-send-failure"
	case DeliveryUnknown:
		return "delivery-unknown"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Replayable reports whether the server proved non-dispatch.
func (o Outcome) Replayable() bool { return o == PreSendFailure || o == Rejected }

// Result pairs the transport outcome with its terminal response.
type Result struct {
	Outcome  Outcome
	Response Response
}

// Rejection returns a typed error for a rejected result and nil otherwise.
func (r Result) Rejection() error {
	if r.Outcome != Rejected || !r.Response.Rejected {
		return nil
	}
	return &RejectionError{Code: r.Response.Code, Reason: r.Response.Reason}
}

// OpenError reports whether a request frame was committed before Open failed.
type OpenError struct {
	Outcome Outcome
	Err     error
}

// Error describes the failed request setup.
func (e *OpenError) Error() string {
	return fmt.Sprintf("wire: open %s: %v", e.Outcome, e.Err)
}

// Unwrap returns the request setup failure.
func (e *OpenError) Unwrap() error { return e.Err }

// Dialer opens one persistent session connection.
type Dialer func(ctx context.Context) (net.Conn, error)

// ClientConfig configures one persistent multiplexed client session.
type ClientConfig struct {
	Dial Dialer
	// Authorize judges the process accepting the dialed connection, between
	// Dial and the hello write, so no handshake byte reaches an unjudged peer
	// and nothing a socket squatter answers — a forged drain preamble, a forged
	// build mismatch — is believed before the peer proves itself. Required: a
	// constructor whose connection needs no judging passes a named waiver. It
	// runs against the exact connection the session uses, so per-connection
	// state is established by Dial and Authorize together and nothing but this
	// immutable config survives to a replacement connection.
	Authorize func(net.Conn) error
	// Lane selects control or business.
	Lane Lane
	// Schema is the business lane's RPC-schema digest; empty on control.
	Schema string
	// Nonce attaches a spawned session; empty everywhere else.
	Nonce []byte
	// Concurrency derives the stream and event buffers; 8 when zero.
	Concurrency int
	// MaxFrame caps each encoded frame; DefaultMaxFrame when zero, and at
	// most maxFrameCeiling so no legal length prefix reads as the drain
	// preamble.
	MaxFrame                int
	HandshakeTimeout        time.Duration
	WriteTimeout            time.Duration
	CancelSettlementTimeout time.Duration
}

// UnaryClient is a unary request transport.
type UnaryClient interface {
	Call(context.Context, Op, []byte) (Result, error)
	WireBuild() string
}

// Client is one persistent, concurrent protocol-2 session.
type Client struct {
	conn   net.Conn
	codec  *Codec
	schema string
	peer   WireIdentity

	ctx    context.Context
	cancel context.CancelFunc

	nextID   atomic.Uint64
	outbound chan outboundFrame
	events   *boundedStream[Event]
	eventOut chan Event

	phaseMu      sync.Mutex
	phase        PhaseSnapshot
	phaseChanged chan struct{}

	mu          sync.Mutex
	pending     map[uint64]*ClientCall
	pendingDone chan struct{}
	err         error

	writerMu     sync.RWMutex
	writerClosed bool

	loopWG                  sync.WaitGroup
	closeOnce               sync.Once
	closeErr                error
	failOnce                sync.Once
	closing                 atomic.Bool
	goAwayStarted           atomic.Bool
	goAwayOnce              sync.Once
	goAway                  chan struct{}
	streamCap               int
	cancelSettlementTimeout time.Duration
}

type outboundFrame struct {
	frame Frame
	ctx   context.Context
	done  chan frameSendResult
}

type frameSendState uint8

const (
	frameNotSent frameSendState = iota
	frameDeliveryUnknown
	frameCommitted
)

type frameSendResult struct {
	state frameSendState
	err   error
}

// ClientCall is one in-flight request on a Client.
type ClientCall struct {
	client         *Client
	id             uint64
	responseStream bool
	chunks         chan Chunk
	inbound        *boundedStream[Chunk]
	ready          chan struct{}
	deliveryDone   chan struct{}
	deliveryOnce   sync.Once
	sendMu         sync.Mutex

	mu              sync.Mutex
	terminal        callResult
	sendSequence    streamSequence
	sendEnded       bool
	canceled        bool
	receiveSequence streamSequence
	receiveEnded    bool
	cancelOnce      sync.Once
	finishOnce      sync.Once
}

type callResult struct {
	result Result
	err    error
}

// NewClient dials and completes the mandatory exact-protocol handshake,
// including the drain-preamble peek, before returning.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if err := validateClientConfig(config); err != nil {
		return nil, err
	}
	concurrency := config.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	conn, err := config.Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("wire: dial: %w", err)
	}
	if err := config.Authorize(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("wire: authorize accepting peer: %w", err)
	}
	codec := NewCodec(conn)
	if config.MaxFrame > 0 {
		codec.MaxFrame = config.MaxFrame
	}
	handshakeDeadline := time.Now().Add(durationOr(config.HandshakeTimeout, defaultHandshakeTimeout))
	if err := codec.SetDeadline(earlierDeadline(ctx, handshakeDeadline)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	peer, err := clientHandshake(codec, helloIdentity{
		Protocol: ProtocolVersion, Lane: config.Lane, Schema: config.Schema, Nonce: config.Nonce,
	})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := codec.ClearDeadline(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	codec.WriteTimeout = durationOr(config.WriteTimeout, defaultWriteTimeout)
	clientCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Client{
		conn:                    conn,
		codec:                   codec,
		schema:                  config.Schema,
		peer:                    peer,
		ctx:                     clientCtx,
		cancel:                  cancel,
		outbound:                make(chan outboundFrame, 4*concurrency),
		events:                  newBoundedStream[Event](concurrency),
		eventOut:                make(chan Event),
		phase:                   PhaseSnapshot{Phase: peer.Phase},
		phaseChanged:            make(chan struct{}),
		pending:                 make(map[uint64]*ClientCall),
		pendingDone:             make(chan struct{}),
		goAway:                  make(chan struct{}),
		streamCap:               concurrency,
		cancelSettlementTimeout: durationOr(config.CancelSettlementTimeout, defaultCancelSettlementTimeout),
	}
	close(c.pendingDone)
	c.loopWG.Add(3)
	go c.writeLoop()
	go c.readLoop(clientCtx)
	go c.deliverEvents()
	return c, nil
}

func validateClientConfig(config ClientConfig) error {
	if config.Dial == nil {
		return errors.New("wire: Dial is required")
	}
	if config.Authorize == nil {
		return errors.New("wire: Authorize is required; judge the accepting peer or pass a named waiver")
	}
	if !config.Lane.valid() {
		return fmt.Errorf("wire: invalid lane %q", config.Lane)
	}
	if config.Lane == LaneBusiness && config.Schema == "" {
		return errors.New("wire: Schema is required on the business lane")
	}
	if config.Lane == LaneControl && (config.Schema != "" || len(config.Nonce) != 0) {
		return errors.New("wire: control lane carries no schema or nonce")
	}
	if config.MaxFrame > maxFrameCeiling {
		return fmt.Errorf("wire: MaxFrame %d admits a frame whose length prefix opens with the drain preamble; the cap stays at or below %#x", config.MaxFrame, maxFrameCeiling)
	}
	return nil
}

// PeerWireIdentity returns the server identity established by the handshake.
func (c *Client) PeerWireIdentity() WireIdentity { return c.peer }

// WireBuild returns the schema identity presented by this session.
func (c *Client) WireBuild() string { return c.schema }

// Failure returns the error that broke this session's transport, or nil while
// it is still usable. A caller's own expired context is not one of them:
// responses are demultiplexed by request id, so a terminal that arrives after
// its caller gave up settles that id and is discarded, leaving the frame
// stream in step for the next request.
func (c *Client) Failure() error { return c.sessionErr() }

// Events returns the bounded server-pushed event stream.
func (c *Client) Events() <-chan Event { return c.eventOut }

// WaitReady blocks until the server's phase stream reports PhaseReady. It
// returns typed terminal errors: RuntimeFailedError on PhaseFailed and
// ErrDraining on PhaseDraining.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		c.phaseMu.Lock()
		snapshot := c.phase
		changed := c.phaseChanged
		c.phaseMu.Unlock()
		switch snapshot.Phase {
		case PhaseReady:
			return nil
		case PhaseFailed:
			return &RuntimeFailedError{Snapshot: snapshot}
		case PhaseDraining:
			return ErrDraining
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return c.sessionErr()
		}
	}
}

// Call sends a unary request without response streaming and waits for its
// terminal response.
func (c *Client) Call(ctx context.Context, op Op, payload []byte) (Result, error) {
	call, err := c.open(ctx, op, payload, true, false)
	if err != nil {
		outcome := PreSendFailure
		var openErr *OpenError
		if errors.As(err, &openErr) {
			outcome = openErr.Outcome
		}
		return Result{Outcome: outcome}, err
	}
	return call.Response(ctx)
}

// Open starts a request that may return a streamed response. endInput marks
// payload as the complete request body; pass false to follow it with SendChunk
// and CloseSend.
func (c *Client) Open(ctx context.Context, op Op, payload []byte, endInput bool) (*ClientCall, error) {
	return c.open(ctx, op, payload, endInput, true)
}

func (c *Client) open(
	ctx context.Context,
	op Op,
	payload []byte,
	endInput bool,
	responseStream bool,
) (*ClientCall, error) {
	if op == "" {
		return nil, &OpenError{Outcome: PreSendFailure, Err: errors.New("wire: operation is required")}
	}
	id := c.nextID.Add(1)
	call := &ClientCall{
		client:         c,
		id:             id,
		responseStream: responseStream,
		chunks:         make(chan Chunk),
		inbound:        newBoundedStream[Chunk](c.streamCap),
		ready:          make(chan struct{}),
		deliveryDone:   make(chan struct{}),
		sendEnded:      endInput,
	}
	if err := c.addPending(call); err != nil {
		return nil, &OpenError{Outcome: PreSendFailure, Err: err}
	}
	callCtx, cancel := context.WithCancel(ctx)
	flags := FrameFlags(0)
	if endInput {
		flags |= FlagEnd
	}
	frame := Frame{Kind: FrameRequest, Flags: flags, ID: id, Op: op, Payload: append([]byte(nil), payload...)}
	if deadline, ok := ctx.Deadline(); ok {
		frame.DeadlineUnixMilli = deadline.UnixMilli()
	}
	state, err := c.sendFrame(callCtx, frame)
	if err != nil {
		cancel()
		c.removePending(id)
		outcome := PreSendFailure
		switch state {
		case frameDeliveryUnknown:
			outcome = DeliveryUnknown
		case frameCommitted:
			outcome = PostSendFailure
		}
		return nil, &OpenError{Outcome: outcome, Err: fmt.Errorf("wire: send request: %w", err)}
	}
	if responseStream {
		go call.deliverChunks()
	} else {
		close(call.chunks)
	}
	go func() {
		defer cancel()
		select {
		case <-call.ready:
		case <-callCtx.Done():
			call.cancel(callCtx)
		case <-c.ctx.Done():
		}
	}()
	return call, nil
}

// ID returns the session-unique request identifier.
func (c *ClientCall) ID() uint64 { return c.id }

// Chunks returns ordered response-stream chunks. It closes before Response returns.
func (c *ClientCall) Chunks() <-chan Chunk { return c.chunks }

// SendChunk appends one ordered request-stream chunk.
func (c *ClientCall) SendChunk(ctx context.Context, payload []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	sequence, err := c.takeSendSequence(false)
	if err != nil {
		return err
	}
	_, err = c.client.sendFrame(ctx, Frame{Kind: FrameStream, ID: c.id, Sequence: sequence, Payload: append([]byte(nil), payload...)})
	return err
}

// CloseSend emits the final request-stream marker exactly once.
func (c *ClientCall) CloseSend(ctx context.Context) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	sequence, err := c.takeSendSequence(true)
	if err != nil {
		return err
	}
	_, err = c.client.sendFrame(ctx, Frame{Kind: FrameStream, Flags: FlagEnd, ID: c.id, Sequence: sequence})
	return err
}

func (c *ClientCall) takeSendSequence(end bool) (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		return 0, ErrCallDone
	default:
	}
	if c.canceled {
		return 0, ErrCallDone
	}
	if c.sendEnded {
		return 0, errors.New("wire: request stream already ended")
	}
	sequence, err := c.sendSequence.take()
	if err != nil {
		return 0, err
	}
	if end {
		c.sendEnded = true
	}
	return sequence, nil
}

// Response waits for the terminal response. Context cancellation sends one
// cancel frame and reports an unknown post-send outcome.
func (c *ClientCall) Response(ctx context.Context) (Result, error) {
	select {
	case <-c.ready:
		return c.terminalResult()
	default:
	}
	select {
	case <-c.ready:
		return c.terminalResult()
	case <-ctx.Done():
		c.cancel(ctx)
		return Result{Outcome: PostSendFailure}, ctx.Err()
	case <-c.client.ctx.Done():
		select {
		case <-c.ready:
			return c.terminalResult()
		default:
			return Result{Outcome: PostSendFailure}, c.client.sessionErr()
		}
	}
}

// Cancel requests cancellation without closing the session.
func (c *ClientCall) Cancel() { c.cancel(c.client.ctx) }

func (c *ClientCall) terminalResult() (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminal.result, c.terminal.err
}

func (c *ClientCall) cancel(parent context.Context) {
	c.cancelOnce.Do(func() {
		c.mu.Lock()
		c.canceled = true
		c.sendEnded = true
		c.mu.Unlock()
		c.stopDelivery()
		go func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), defaultWriteTimeout)
			_, err := c.client.sendFrame(ctx, Frame{Kind: FrameCancel, Flags: FlagEnd, ID: c.id})
			cancel()
			if err != nil {
				if errors.Is(err, ErrClientClosing) {
					return
				}
				c.client.fail(fmt.Errorf("wire: cancel request: %w", err))
				return
			}
			timer := time.NewTimer(c.client.cancelSettlementTimeout)
			defer timer.Stop()
			select {
			case <-c.ready:
			case <-timer.C:
				c.client.fail(ErrCancelSettlement)
			case <-c.client.ctx.Done():
			}
		}()
	})
}

func (c *ClientCall) deliverChunks() {
	defer close(c.chunks)
	for {
		select {
		case chunk, ok := <-c.inbound.channel():
			if !ok {
				return
			}
			select {
			case c.chunks <- chunk:
			case <-c.deliveryDone:
				return
			case <-c.client.ctx.Done():
				return
			}
		case <-c.deliveryDone:
			return
		case <-c.client.ctx.Done():
			return
		}
	}
}

func (c *ClientCall) stopDelivery() {
	c.deliveryOnce.Do(func() { close(c.deliveryDone) })
}

func (c *Client) deliverEvents() {
	defer c.loopWG.Done()
	defer close(c.eventOut)
	for {
		select {
		case event, ok := <-c.events.channel():
			if !ok {
				return
			}
			select {
			case c.eventOut <- event:
			case <-c.ctx.Done():
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Close gracefully terminates the session after a GoAway acknowledgement,
// bounded by ctx: every wait it makes — for pending calls to settle, for the
// go-away write, for the peer's answer — ends when ctx does, and the session
// is torn down instead. A ctx without a deadline waits indefinitely.
func (c *Client) Close(ctx context.Context) error { return c.close(ctx) }

// Abort tears down the local session without a GoAway exchange. Pending calls
// fail with ErrClientAbort and the supplied cause.
func (c *Client) Abort(cause error) error {
	c.closeOnce.Do(func() {
		if cause == nil {
			cause = ErrClientAbort
		} else {
			cause = errors.Join(ErrClientAbort, cause)
		}
		c.closeErr = cause
		c.fail(cause)
	})
	c.loopWG.Wait()
	return nil
}

func (c *Client) close(parent context.Context) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closing.Store(true)
		pendingDone := c.pendingDone
		c.mu.Unlock()
		select {
		case <-pendingDone:
		case <-parent.Done():
			c.closeErr = fmt.Errorf("wire: await pending calls: %w", parent.Err())
			c.fail(c.closeErr)
			return
		}
		c.writerMu.Lock()
		c.goAwayStarted.Store(true)
		c.writerMu.Unlock()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.codec.WriteTimeout)
		defer cancel()
		if _, err := c.sendFrameState(ctx, Frame{Kind: FrameGoAway, Flags: FlagEnd}, true); err != nil {
			c.closeErr = fmt.Errorf("wire: send go-away: %w", err)
		} else {
			select {
			case <-c.goAway:
			case <-ctx.Done():
				c.closeErr = fmt.Errorf("wire: await go-away acknowledgement: %w", ctx.Err())
			case <-c.ctx.Done():
				c.closeErr = fmt.Errorf("wire: await go-away acknowledgement: %w", c.sessionErr())
			}
		}
		c.fail(io.EOF)
	})
	c.loopWG.Wait()
	return c.closeErr
}

func (c *Client) writeLoop() {
	defer c.loopWG.Done()
	defer c.closeWriter()
	for {
		var outgoing outboundFrame
		select {
		case outgoing = <-c.outbound:
		case <-c.ctx.Done():
			return
		}
		result := frameSendResult{state: frameNotSent, err: outgoing.ctx.Err()}
		if result.err == nil {
			var started, committed bool
			started, committed, result.err = c.codec.writeFrame(outgoing.frame)
			switch {
			case committed:
				result.state = frameCommitted
			case started:
				result.state = frameDeliveryUnknown
			}
		}
		outgoing.done <- result
		if result.err != nil && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
			if errors.Is(result.err, ErrFrameTooLarge) || errors.Is(result.err, ErrInvalidFrame) {
				continue
			}
			c.fail(fmt.Errorf("wire: write: %w", result.err))
			return
		}
	}
}

func (c *Client) closeWriter() {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	c.writerClosed = true
	for {
		select {
		case outgoing := <-c.outbound:
			outgoing.done <- frameSendResult{state: frameDeliveryUnknown, err: c.sessionErr()}
		default:
			return
		}
	}
}

func (c *Client) readLoop(ctx context.Context) {
	defer c.loopWG.Done()
	for {
		frame, err := c.codec.ReadFrame()
		if err != nil {
			c.fail(fmt.Errorf("wire: read: %w", err))
			return
		}
		switch frame.Kind {
		case FrameResponse:
			if err := c.receiveResponse(frame); err != nil {
				c.fail(err)
				return
			}
		case FrameStream:
			if err := c.receiveStream(frame); err != nil {
				c.fail(err)
				return
			}
		case FrameEvent:
			if err := c.events.offer(Event{Topic: string(frame.Op), Payload: frame.Payload}); err != nil {
				if errors.Is(err, errStreamClosed) && ctx.Err() != nil {
					return
				}
				c.fail(err)
				return
			}
		case FrameLifecycle:
			if err := c.receiveLifecycle(frame); err != nil {
				c.fail(err)
				return
			}
		case FrameGoAway:
			if !c.closing.Load() {
				c.fail(io.EOF)
				return
			}
			c.goAwayOnce.Do(func() { close(c.goAway) })
			return
		default:
			c.fail(fmt.Errorf("%w: server frame kind %d", ErrInvalidFrame, frame.Kind))
			return
		}
	}
}

func (c *Client) receiveLifecycle(frame Frame) error {
	var snapshot PhaseSnapshot
	if err := decodeStrict(frame.Payload, &snapshot); err != nil {
		return fmt.Errorf("%w: lifecycle snapshot: %w", ErrInvalidFrame, err)
	}
	if snapshot.Phase == "" {
		return fmt.Errorf("%w: empty lifecycle phase", ErrInvalidFrame)
	}
	c.phaseMu.Lock()
	c.phase = snapshot
	changed := c.phaseChanged
	c.phaseChanged = make(chan struct{})
	c.phaseMu.Unlock()
	close(changed)
	return nil
}

func (c *Client) receiveResponse(frame Frame) error {
	var response Response
	if err := decodeStrict(frame.Payload, &response); err != nil {
		return fmt.Errorf("%w: decode response: %w", ErrInvalidFrame, err)
	}
	c.mu.Lock()
	call := c.pending[frame.ID]
	c.mu.Unlock()
	var ackErr error
	if response.Ack {
		_, ackErr = c.sendFrame(c.ctx, Frame{
			Kind: FrameAck, Flags: FlagEnd, ID: frame.ID, Payload: c.peer.Session,
		})
	}
	if call == nil {
		return ackErr
	}
	call.mu.Lock()
	call.receiveEnded = true
	call.mu.Unlock()
	call.inbound.close()
	outcome := Delivered
	if response.Rejected {
		outcome = Rejected
	}
	call.finish(callResult{result: Result{Outcome: outcome, Response: response}})
	c.removePending(frame.ID)
	return ackErr
}

func (c *Client) receiveStream(frame Frame) error {
	c.mu.Lock()
	call := c.pending[frame.ID]
	c.mu.Unlock()
	if call == nil {
		return nil
	}
	if !call.responseStream {
		return ErrFlowControl
	}
	call.mu.Lock()
	if call.receiveEnded {
		call.mu.Unlock()
		return ErrStreamOrder
	}
	expected, err := call.receiveSequence.take()
	if err != nil {
		call.mu.Unlock()
		return err
	}
	if frame.Sequence != expected {
		call.mu.Unlock()
		return ErrStreamOrder
	}
	end := frame.Flags&FlagEnd != 0
	if end {
		call.receiveEnded = true
	}
	call.mu.Unlock()
	chunk := Chunk{Sequence: frame.Sequence, Payload: append([]byte(nil), frame.Payload...), End: end}
	// Waiting here propagates bounded consumer pressure to the socket.
	if err := call.inbound.offer(chunk); err != nil {
		if errors.Is(err, errStreamClosed) {
			return nil
		}
		return err
	}
	if end {
		call.inbound.close()
	}
	return nil
}

func (c *Client) sendFrame(ctx context.Context, frame Frame) (frameSendState, error) {
	return c.sendFrameState(ctx, frame, false)
}

func (c *Client) sendFrameState(ctx context.Context, frame Frame, duringClose bool) (frameSendState, error) {
	done := make(chan frameSendResult, 1)
	c.writerMu.RLock()
	if c.writerClosed {
		c.writerMu.RUnlock()
		return frameNotSent, c.sessionErr()
	}
	if c.goAwayStarted.Load() && !duringClose {
		c.writerMu.RUnlock()
		return frameNotSent, ErrClientClosing
	}
	select {
	case c.outbound <- outboundFrame{frame: frame, ctx: ctx, done: done}:
	case <-ctx.Done():
		c.writerMu.RUnlock()
		return frameNotSent, ctx.Err()
	case <-c.ctx.Done():
		c.writerMu.RUnlock()
		return frameNotSent, c.sessionErr()
	}
	c.writerMu.RUnlock()
	result := <-done
	return result.state, result.err
}

func (c *Client) fail(err error) {
	c.failOnce.Do(func() {
		c.mu.Lock()
		c.err = err
		pending := c.pending
		c.pending = make(map[uint64]*ClientCall)
		if len(pending) > 0 {
			close(c.pendingDone)
		}
		c.mu.Unlock()
		c.cancel()
		_ = c.conn.Close()
		c.events.close()
		for _, call := range pending {
			call.mu.Lock()
			call.receiveEnded = true
			call.mu.Unlock()
			call.inbound.close()
			call.stopDelivery()
			call.finish(callResult{result: Result{Outcome: PostSendFailure}, err: err})
		}
	})
}

func (c *ClientCall) finish(terminal callResult) {
	c.finishOnce.Do(func() {
		c.mu.Lock()
		c.terminal = terminal
		c.receiveEnded = true
		c.sendEnded = true
		c.mu.Unlock()
		c.inbound.close()
		close(c.ready)
	})
}

func (c *Client) removePending(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pending[id]; ok {
		delete(c.pending, id)
		if len(c.pending) == 0 {
			close(c.pendingDone)
		}
	}
}

func (c *Client) addPending(call *ClientCall) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() {
		return ErrClientClosing
	}
	if c.err != nil {
		return c.err
	}
	select {
	case <-c.ctx.Done():
		return net.ErrClosed
	default:
	}
	if len(c.pending) == 0 {
		c.pendingDone = make(chan struct{})
	}
	c.pending[call.id] = call
	return nil
}

func (c *Client) sessionErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	select {
	case <-c.ctx.Done():
		return net.ErrClosed
	default:
		return nil
	}
}
