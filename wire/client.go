package wire

import (
	"context"
	"encoding/json"
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

// Dialer opens one persistent session connection.
type Dialer func(ctx context.Context) (net.Conn, error)

// ClientConfig configures one persistent multiplexed client session.
type ClientConfig struct {
	Dial                    Dialer
	Build                   string
	Ladder                  Ladder
	MaxFrame                int
	OutboundQueue           int
	StreamQueue             int
	EventQueue              int
	HandshakeTimeout        time.Duration
	WriteTimeout            time.Duration
	CancelSettlementTimeout time.Duration
}

// Client is one persistent, concurrent v4 session.
type Client struct {
	conn   net.Conn
	codec  *Codec
	build  string
	peer   BuildIdentity
	ladder Ladder

	ctx    context.Context
	cancel context.CancelFunc

	nextID   atomic.Uint64
	outbound chan outboundFrame
	events   *boundedStream[Event]
	eventOut chan Event

	mu      sync.Mutex
	pending map[uint64]*ClientCall
	err     error

	loopWG                  sync.WaitGroup
	closeOnce               sync.Once
	failOnce                sync.Once
	streamCap               int
	streamWindow            uint32
	cancelSettlementTimeout time.Duration
}

type outboundFrame struct {
	frame Frame
	ctx   context.Context
	done  chan error
}

// ClientCall is one in-flight request on a Client.
type ClientCall struct {
	client       *Client
	id           uint64
	chunks       chan Chunk
	inbound      *boundedStream[Chunk]
	ready        chan struct{}
	deliveryDone chan struct{}
	deliveryOnce sync.Once
	sendCredits  *creditWindow
	sendMu       sync.Mutex

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

// NewClient dials and completes the mandatory exact-v4 handshake before returning.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if config.Dial == nil {
		return nil, errors.New("wire: Dial is required")
	}
	if config.Build == "" {
		return nil, errors.New("wire: Build is required")
	}
	streamCap := positiveOr(config.StreamQueue, defaultStreamQueue)
	eventCap := positiveOr(config.EventQueue, defaultStreamQueue)
	streamWindow, err := uint32Length("stream queue", streamCap)
	if err != nil {
		return nil, errors.New("wire: stream queue exceeds protocol window")
	}
	eventWindow, err := uint32Length("event queue", eventCap)
	if err != nil {
		return nil, errors.New("wire: event queue exceeds protocol window")
	}
	conn, err := config.Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("wire: dial: %w", err)
	}
	codec := NewCodec(conn)
	if config.MaxFrame > 0 {
		codec.MaxFrame = config.MaxFrame
	}
	if err := codec.SetDeadline(earlierDeadline(ctx, durationOr(config.HandshakeTimeout, defaultHandshakeTimeout))); err != nil {
		_ = conn.Close()
		return nil, err
	}
	peer, err := clientHandshake(codec, config.Build)
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
		build:                   config.Build,
		peer:                    peer,
		ladder:                  config.Ladder,
		ctx:                     clientCtx,
		cancel:                  cancel,
		outbound:                make(chan outboundFrame, positiveOr(config.OutboundQueue, defaultOutboundQueue)),
		events:                  newBoundedStream[Event](eventCap),
		eventOut:                make(chan Event),
		pending:                 make(map[uint64]*ClientCall),
		streamCap:               streamCap,
		streamWindow:            streamWindow,
		cancelSettlementTimeout: durationOr(config.CancelSettlementTimeout, defaultCancelSettlementTimeout),
	}
	if err := codec.WriteFrame(Frame{Kind: FrameWindow, Sequence: eventWindow}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("wire: grant event window: %w", err)
	}
	c.loopWG.Add(3)
	go c.writeLoop()
	go c.readLoop(clientCtx)
	go c.deliverEvents()
	return c, nil
}

// PeerBuild returns the server identity established by the handshake.
func (c *Client) PeerBuild() BuildIdentity { return c.peer }

// Events returns the bounded server-pushed event stream.
func (c *Client) Events() <-chan Event { return c.eventOut }

// Call sends a unary request and waits for its terminal response.
func (c *Client) Call(ctx context.Context, op Op, tenant string, payload []byte) (Result, error) {
	call, err := c.Open(ctx, op, tenant, payload, true)
	if err != nil {
		return Result{Outcome: PreSendFailure}, err
	}
	return call.Response(ctx)
}

// Open starts a request. endInput marks payload as the complete request body;
// pass false to follow it with SendChunk and CloseSend.
func (c *Client) Open(ctx context.Context, op Op, tenant string, payload []byte, endInput bool) (*ClientCall, error) {
	if op == "" {
		return nil, errors.New("wire: operation is required")
	}
	if err := c.sessionErr(); err != nil {
		return nil, err
	}
	id := c.nextID.Add(1)
	call := &ClientCall{
		client:       c,
		id:           id,
		chunks:       make(chan Chunk),
		inbound:      newBoundedStream[Chunk](c.streamCap),
		ready:        make(chan struct{}),
		deliveryDone: make(chan struct{}),
		sendCredits:  newCreditWindow(),
		sendEnded:    endInput,
		receiveEnded: false,
	}
	c.mu.Lock()
	c.pending[id] = call
	c.mu.Unlock()
	callCtx, cancel := c.callContext(ctx, op)
	deadline, _ := callCtx.Deadline()
	flags := FrameFlags(0)
	if endInput {
		flags |= FlagEnd
	}
	frame := Frame{Kind: FrameRequest, Flags: flags, ID: id, Op: op, Tenant: tenant, Payload: append([]byte(nil), payload...)}
	if !deadline.IsZero() {
		frame.DeadlineUnixMilli = deadline.UnixMilli()
	}
	if err := c.sendFrame(callCtx, frame); err != nil {
		cancel()
		c.removePending(id)
		return nil, fmt.Errorf("wire: send request: %w", err)
	}
	if err := c.sendFrame(callCtx, Frame{Kind: FrameWindow, ID: id, Sequence: c.streamWindow}); err != nil {
		cancel()
		err = fmt.Errorf("wire: grant response window: %w", err)
		c.fail(err)
		return nil, err
	}
	go call.deliverChunks()
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
	c.mu.Lock()
	select {
	case <-c.ready:
		c.mu.Unlock()
		return ErrCallDone
	default:
	}
	if c.canceled {
		c.mu.Unlock()
		return ErrCallDone
	}
	if c.sendEnded {
		c.mu.Unlock()
		return errors.New("wire: request stream already ended")
	}
	c.mu.Unlock()
	if err := c.sendCredits.acquire(ctx); err != nil {
		if errors.Is(err, errStreamClosed) {
			return ErrCallDone
		}
		return err
	}
	c.mu.Lock()
	if c.canceled || c.sendEnded {
		c.mu.Unlock()
		return ErrCallDone
	}
	sequence, err := c.sendSequence.take()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	frame := Frame{Kind: FrameStream, ID: c.id, Sequence: sequence, Payload: append([]byte(nil), payload...)}
	if err := c.client.sendFrame(ctx, frame); err != nil {
		return err
	}
	return nil
}

// CloseSend emits the final request-stream marker exactly once.
func (c *ClientCall) CloseSend(ctx context.Context) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	select {
	case <-c.ready:
		c.mu.Unlock()
		return ErrCallDone
	default:
	}
	if c.canceled {
		c.mu.Unlock()
		return ErrCallDone
	}
	if c.sendEnded {
		c.mu.Unlock()
		return errors.New("wire: request stream already ended")
	}
	c.mu.Unlock()
	if err := c.sendCredits.acquire(ctx); err != nil {
		if errors.Is(err, errStreamClosed) {
			return ErrCallDone
		}
		return err
	}
	c.mu.Lock()
	if c.canceled || c.sendEnded {
		c.mu.Unlock()
		return ErrCallDone
	}
	sequence, err := c.sendSequence.take()
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.sendEnded = true
	c.mu.Unlock()
	if err := c.client.sendFrame(ctx, Frame{Kind: FrameStream, Flags: FlagEnd, ID: c.id, Sequence: sequence}); err != nil {
		return err
	}
	return nil
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
		c.sendCredits.close()
		c.stopDelivery()
		go func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), defaultWriteTimeout)
			err := c.client.sendFrame(ctx, Frame{Kind: FrameCancel, Flags: FlagEnd, ID: c.id})
			cancel()
			if err != nil {
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
				if err := c.client.sendFrame(c.client.ctx, Frame{Kind: FrameWindow, ID: c.id, Sequence: 1}); err != nil {
					c.client.fail(fmt.Errorf("wire: return response credit: %w", err))
					return
				}
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
				if err := c.sendFrame(c.ctx, Frame{Kind: FrameWindow, Sequence: 1}); err != nil {
					c.fail(fmt.Errorf("wire: return event credit: %w", err))
					return
				}
			case <-c.ctx.Done():
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Close terminates the session and all pending calls.
func (c *Client) Close() error { return c.close(context.Background()) }

func (c *Client) close(parent context.Context) error {
	c.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), c.codec.WriteTimeout)
		_ = c.sendFrame(ctx, Frame{Kind: FrameGoAway, Flags: FlagEnd})
		cancel()
		c.fail(io.EOF)
	})
	c.loopWG.Wait()
	return nil
}

func clientHandshake(codec *Codec, build string) (BuildIdentity, error) {
	payload, err := json.Marshal(BuildIdentity{Protocol: ProtocolVersion, Build: build})
	if err != nil {
		return BuildIdentity{}, err
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload}); err != nil {
		return BuildIdentity{}, fmt.Errorf("%w: write hello: %w", ErrHandshake, err)
	}
	frame, err := codec.ReadFrame()
	if err != nil {
		return BuildIdentity{}, fmt.Errorf("%w: read acknowledge: %w", ErrHandshake, err)
	}
	if frame.Kind != FrameHelloAck || frame.ID != 0 || frame.Sequence != 0 || frame.Flags != FlagEnd || frame.Op != "" || frame.Tenant != "" {
		return BuildIdentity{}, fmt.Errorf("%w: invalid acknowledge", ErrHandshake)
	}
	var identity BuildIdentity
	if err := decodeStrict(frame.Payload, &identity); err != nil {
		return BuildIdentity{}, fmt.Errorf("%w: identity: %w", ErrHandshake, err)
	}
	if identity.Protocol != ProtocolVersion {
		return BuildIdentity{}, fmt.Errorf("%w: identity got %d", ErrProtocolVersion, identity.Protocol)
	}
	if identity.Build == "" {
		return BuildIdentity{}, fmt.Errorf("%w: empty server build", ErrHandshake)
	}
	if len(identity.Session) != sessionGenerationBytes {
		return BuildIdentity{}, fmt.Errorf("%w: invalid session generation", ErrHandshake)
	}
	return identity, nil
}

func (c *Client) writeLoop() {
	defer c.loopWG.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case outgoing := <-c.outbound:
			err := outgoing.ctx.Err()
			if err == nil {
				err = c.codec.WriteFrame(outgoing.frame)
			}
			outgoing.done <- err
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					c.fail(fmt.Errorf("wire: write: %w", err))
					return
				}
			}
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
			if frame.ID != 0 || frame.Op == "" || frame.Flags != FlagEnd {
				c.fail(fmt.Errorf("%w: event frame", ErrInvalidFrame))
				return
			}
			if err := c.events.offer(Event{Topic: string(frame.Op), Payload: frame.Payload}); err != nil {
				if errors.Is(err, errStreamClosed) && ctx.Err() != nil {
					return
				}
				c.fail(err)
				return
			}
		case FrameWindow:
			if err := c.receiveWindow(frame); err != nil {
				c.fail(err)
				return
			}
		case FrameGoAway:
			c.fail(io.EOF)
			return
		default:
			c.fail(fmt.Errorf("%w: server frame kind %d", ErrInvalidFrame, frame.Kind))
			return
		}
	}
}

func (c *Client) receiveResponse(frame Frame) error {
	if frame.ID == 0 || frame.Flags != FlagEnd || frame.Op != "" || frame.Tenant != "" {
		return fmt.Errorf("%w: response frame", ErrInvalidFrame)
	}
	var response Response
	if err := decodeStrict(frame.Payload, &response); err != nil {
		return fmt.Errorf("wire: decode response: %w", err)
	}
	call := c.removePending(frame.ID)
	var ackErr error
	if response.Ack {
		ackErr = c.sendFrame(c.ctx, Frame{
			Kind: FrameAck, Flags: FlagEnd, ID: frame.ID, Payload: c.peer.Session,
		})
	}
	if call == nil {
		return ackErr
	}
	call.mu.Lock()
	if !call.receiveEnded {
		call.receiveEnded = true
	}
	call.mu.Unlock()
	call.inbound.close()
	outcome := Delivered
	if response.Rejected {
		outcome = Rejected
	}
	call.finish(callResult{result: Result{Outcome: outcome, Response: response}})
	return ackErr
}

func (c *Client) receiveStream(frame Frame) error {
	if frame.ID == 0 || frame.Op != "" || frame.Tenant != "" {
		return fmt.Errorf("%w: response stream frame", ErrInvalidFrame)
	}
	c.mu.Lock()
	call := c.pending[frame.ID]
	c.mu.Unlock()
	if call == nil {
		return nil
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
	chunk := Chunk{Sequence: frame.Sequence, Payload: append([]byte(nil), frame.Payload...), End: frame.Flags&FlagEnd != 0}
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

func (c *Client) receiveWindow(frame Frame) error {
	if frame.ID == 0 || frame.Flags != 0 || frame.Sequence == 0 || frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
		return fmt.Errorf("%w: request stream window", ErrInvalidFrame)
	}
	c.mu.Lock()
	call := c.pending[frame.ID]
	c.mu.Unlock()
	if call == nil {
		return nil
	}
	err := call.sendCredits.grant(frame.Sequence)
	if errors.Is(err, errStreamClosed) {
		return nil
	}
	return err
}

func (c *Client) sendFrame(ctx context.Context, frame Frame) error {
	done := make(chan error, 1)
	select {
	case c.outbound <- outboundFrame{frame: frame, ctx: ctx, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return c.sessionErr()
	}
	select {
	case err := <-done:
		return err
	case <-c.ctx.Done():
		return c.sessionErr()
	}
}

func (c *Client) fail(err error) {
	c.failOnce.Do(func() {
		c.mu.Lock()
		c.err = err
		pending := c.pending
		c.pending = make(map[uint64]*ClientCall)
		c.mu.Unlock()
		c.cancel()
		_ = c.conn.Close()
		c.events.close()
		for _, call := range pending {
			call.mu.Lock()
			if !call.receiveEnded {
				call.receiveEnded = true
			}
			call.mu.Unlock()
			call.inbound.close()
			call.sendCredits.close()
			call.stopDelivery()
			call.finish(callResult{result: Result{Outcome: PostSendFailure}, err: err})
		}
	})
}

func (c *ClientCall) finish(terminal callResult) {
	c.finishOnce.Do(func() {
		c.mu.Lock()
		c.terminal = terminal
		if !c.receiveEnded {
			c.receiveEnded = true
		}
		c.sendEnded = true
		c.mu.Unlock()
		c.inbound.close()
		c.sendCredits.close()
		close(c.ready)
	})
}

func (c *Client) removePending(id uint64) *ClientCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[id]
	delete(c.pending, id)
	return call
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

func (c *Client) callContext(ctx context.Context, op Op) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok {
		if _, client, found := c.ladder.Deadlines(op); !found || time.Until(deadline) <= client {
			return ctx, func() {}
		}
	}
	if _, client, ok := c.ladder.Deadlines(op); ok {
		return context.WithTimeout(ctx, client)
	}
	return ctx, func() {}
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
