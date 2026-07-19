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
	// PostSendFailure means the request was sent but no terminal response arrived.
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

// Client is one persistent, concurrent v2 session.
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
	events   chan Event

	mu      sync.Mutex
	pending map[uint64]*ClientCall
	err     error

	loopWG                  sync.WaitGroup
	closeOnce               sync.Once
	failOnce                sync.Once
	streamCap               int
	cancelSettlementTimeout time.Duration
}

type outboundFrame struct {
	frame Frame
	ctx   context.Context
	done  chan error
}

// ClientCall is one in-flight request on a Client.
type ClientCall struct {
	client *Client
	id     uint64
	chunks chan Chunk
	ready  chan struct{}

	mu           sync.Mutex
	terminal     callResult
	nextSend     uint32
	sendEnded    bool
	canceled     bool
	nextReceive  uint32
	receiveEnded bool
	cancelOnce   sync.Once
	finishOnce   sync.Once
}

type callResult struct {
	result Result
	err    error
}

// NewClient dials and completes the mandatory exact-v2 handshake before returning.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if config.Dial == nil {
		return nil, errors.New("wire: Dial is required")
	}
	if config.Build == "" {
		return nil, errors.New("wire: Build is required")
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
		events:                  make(chan Event, positiveOr(config.EventQueue, defaultStreamQueue)),
		pending:                 make(map[uint64]*ClientCall),
		streamCap:               positiveOr(config.StreamQueue, defaultStreamQueue),
		cancelSettlementTimeout: durationOr(config.CancelSettlementTimeout, defaultCancelSettlementTimeout),
	}
	c.loopWG.Add(2)
	go c.writeLoop()
	go c.readLoop(clientCtx)
	return c, nil
}

// PeerBuild returns the server identity established by the handshake.
func (c *Client) PeerBuild() BuildIdentity { return c.peer }

// Events returns the bounded server-pushed event stream.
func (c *Client) Events() <-chan Event { return c.events }

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
		chunks:       make(chan Chunk, c.streamCap),
		ready:        make(chan struct{}),
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
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		return ErrCallDone
	default:
	}
	if c.canceled {
		return ErrCallDone
	}
	if c.sendEnded {
		return errors.New("wire: request stream already ended")
	}
	frame := Frame{Kind: FrameStream, ID: c.id, Sequence: c.nextSend, Payload: append([]byte(nil), payload...)}
	if err := c.client.sendFrame(ctx, frame); err != nil {
		return err
	}
	c.nextSend++
	return nil
}

// CloseSend emits the final request-stream marker exactly once.
func (c *ClientCall) CloseSend(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
		return ErrCallDone
	default:
	}
	if c.canceled {
		return ErrCallDone
	}
	if c.sendEnded {
		return errors.New("wire: request stream already ended")
	}
	if err := c.client.sendFrame(ctx, Frame{Kind: FrameStream, Flags: FlagEnd, ID: c.id, Sequence: c.nextSend}); err != nil {
		return err
	}
	c.nextSend++
	c.sendEnded = true
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
	defer close(c.events)
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
			if err := c.receiveStream(ctx, frame); err != nil {
				c.fail(err)
				return
			}
		case FrameEvent:
			if frame.ID != 0 || frame.Op == "" || frame.Flags != FlagEnd {
				c.fail(fmt.Errorf("%w: event frame", ErrInvalidFrame))
				return
			}
			select {
			case c.events <- Event{Topic: string(frame.Op), Payload: frame.Payload}:
			default:
				c.fail(ErrQueueFull)
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
	call := c.removePending(frame.ID)
	if call == nil {
		return nil
	}
	var response Response
	if err := decodeStrict(frame.Payload, &response); err != nil {
		return fmt.Errorf("wire: decode response: %w", err)
	}
	call.mu.Lock()
	if !call.receiveEnded {
		call.receiveEnded = true
		close(call.chunks)
	}
	call.mu.Unlock()
	outcome := Delivered
	if response.Rejected {
		outcome = Rejected
	}
	call.finish(callResult{result: Result{Outcome: outcome, Response: response}})
	return nil
}

func (c *Client) receiveStream(ctx context.Context, frame Frame) error {
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
	if call.receiveEnded || frame.Sequence != call.nextReceive {
		call.mu.Unlock()
		return ErrStreamOrder
	}
	call.nextReceive++
	chunk := Chunk{Sequence: frame.Sequence, Payload: append([]byte(nil), frame.Payload...), End: frame.Flags&FlagEnd != 0}
	select {
	case call.chunks <- chunk:
	default:
		call.mu.Unlock()
		c.removePending(frame.ID)
		call.finish(callResult{result: Result{Outcome: PostSendFailure}, err: ErrQueueFull})
		call.cancel(ctx)
		return nil
	}
	if chunk.End {
		call.receiveEnded = true
		close(call.chunks)
	}
	call.mu.Unlock()
	return nil
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
		for _, call := range pending {
			call.mu.Lock()
			if !call.receiveEnded {
				call.receiveEnded = true
				close(call.chunks)
			}
			call.mu.Unlock()
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
			close(c.chunks)
		}
		c.sendEnded = true
		c.mu.Unlock()
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
