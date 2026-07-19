package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// AcceptedSession is a server-authenticated persistent client session.
type AcceptedSession struct{ s *session }

// Peer returns the OS identity captured once from the accepted socket.
func (s *AcceptedSession) Peer() Peer { return s.s.peer }

// Build returns the client build supplied by the mandatory handshake.
func (s *AcceptedSession) Build() string { return s.s.build }

// PushEvent enqueues a server-pushed event with bounded backpressure.
func (s *AcceptedSession) PushEvent(ctx context.Context, event Event) error {
	if event.Topic == "" {
		return errors.New("wire: event topic is required")
	}
	if err := s.s.eventCredits.acquire(ctx); err != nil {
		return err
	}
	return s.s.enqueue(ctx, Frame{Kind: FrameEvent, Flags: FlagEnd, Op: Op(event.Topic), Payload: event.Payload})
}

type session struct {
	server       *Server
	conn         net.Conn
	codec        *Codec
	ctx          context.Context
	cancel       context.CancelFunc
	peer         Peer
	build        string
	admit        func() (func(), error)
	accepted     *AcceptedSession
	outbound     chan sessionOutbound
	eventCredits *creditWindow

	mu        sync.Mutex
	active    map[uint64]*requestState
	seen      map[uint64]struct{}
	watermark uint64

	requestWG sync.WaitGroup
	writerWG  sync.WaitGroup
	closeOnce sync.Once
}

type sessionOutbound struct {
	frame Frame
	done  chan error
}

type requestState struct {
	cancel          context.CancelFunc
	chunks          chan Chunk
	inbound         *boundedStream[Chunk]
	responseCredits *creditWindow
	deliveryDone    chan struct{}
	deliveryOnce    sync.Once

	mu            sync.Mutex
	inputSequence streamSequence
	inputEnded    bool
	transportErr  error
}

func (s *requestState) close() {
	s.cancel()
	s.mu.Lock()
	s.inputEnded = true
	s.mu.Unlock()
	s.inbound.close()
	s.responseCredits.close()
	s.deliveryOnce.Do(func() { close(s.deliveryDone) })
}

func (s *requestState) error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportErr
}

func (s *session) run(ctx context.Context) error {
	s.writerWG.Add(1)
	go s.writeLoop()
	err := s.readLoop(ctx)
	s.close()
	s.closeRequestInputs()
	s.requestWG.Wait()
	s.writerWG.Wait()
	return err
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.eventCredits.close()
		_ = s.conn.Close()
		s.mu.Lock()
		states := make([]*requestState, 0, len(s.active))
		for _, state := range s.active {
			states = append(states, state)
		}
		s.mu.Unlock()
		for _, state := range states {
			state.close()
		}
	})
}

func (s *session) writeLoop() {
	defer s.writerWG.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case outgoing := <-s.outbound:
			err := s.codec.WriteFrame(outgoing.frame)
			if outgoing.done != nil {
				outgoing.done <- err
			}
			if err != nil {
				s.close()
				return
			}
		}
	}
}

func (s *session) readLoop(ctx context.Context) error {
	for {
		frame, err := s.codec.ReadFrame()
		if err != nil {
			return err
		}
		switch frame.Kind {
		case FrameRequest:
			if err := s.receiveRequest(ctx, frame); err != nil {
				return err
			}
		case FrameCancel:
			if err := s.receiveCancel(frame); err != nil {
				return err
			}
		case FrameStream:
			if err := s.receiveStream(frame); err != nil {
				return err
			}
		case FrameWindow:
			if err := s.receiveWindow(frame); err != nil {
				return err
			}
		case FrameGoAway:
			return io.EOF
		default:
			return fmt.Errorf("%w: client frame kind %d", ErrInvalidFrame, frame.Kind)
		}
	}
}

func (s *session) receiveRequest(ctx context.Context, frame Frame) error {
	if frame.ID == 0 || frame.Op == "" || frame.Sequence != 0 {
		return fmt.Errorf("%w: request frame", ErrInvalidFrame)
	}
	s.mu.Lock()
	if frame.ID <= s.watermark {
		s.mu.Unlock()
		return ErrDuplicateID
	}
	if _, duplicate := s.seen[frame.ID]; duplicate {
		s.mu.Unlock()
		return ErrDuplicateID
	}
	queueLimit, err := uint64Length("inbound queue", s.server.inboundQueue())
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if frame.ID-s.watermark > queueLimit {
		s.mu.Unlock()
		return s.sendRejected(ctx, frame.ID, ErrQueueFull.Error())
	}
	s.seen[frame.ID] = struct{}{}
	for {
		next := s.watermark + 1
		if _, ok := s.seen[next]; !ok {
			break
		}
		delete(s.seen, next)
		s.watermark = next
	}
	if len(s.active) >= s.server.inboundQueue() {
		s.mu.Unlock()
		return s.sendRejected(ctx, frame.ID, ErrQueueFull.Error())
	}
	entry, ok := s.server.lookup(frame.Op)
	if !ok {
		s.mu.Unlock()
		return s.sendError(ctx, frame.ID, fmt.Errorf("wire: unknown op %q", frame.Op))
	}
	requestCtx, cancel := s.server.requestContext(ctx, frame)
	state := &requestState{
		cancel:          cancel,
		chunks:          make(chan Chunk),
		inbound:         newBoundedStream[Chunk](s.server.streamQueue()),
		responseCredits: newCreditWindow(),
		deliveryDone:    make(chan struct{}),
	}
	if frame.Flags&FlagEnd != 0 {
		state.inputEnded = true
		state.inbound.close()
	}
	s.active[frame.ID] = state
	s.mu.Unlock()
	if err := s.enqueue(ctx, Frame{Kind: FrameWindow, ID: frame.ID, Sequence: s.server.streamWindow}); err != nil {
		state.close()
		s.removeRequest(frame.ID)
		return err
	}

	s.requestWG.Add(2)
	go s.deliverRequestChunks(frame.ID, state)
	go s.execute(ctx, requestCtx, frame, entry, state)
	return nil
}

func (s *session) execute(sessionCtx, requestCtx context.Context, frame Frame, entry entry, state *requestState) {
	defer s.requestWG.Done()
	defer state.close()
	defer s.removeRequest(frame.ID)

	if !entry.lifecycle {
		if s.build != s.server.Build {
			if err := s.sendRejected(sessionCtx, frame.ID, ErrBuildMismatch.Error()); err != nil {
				s.close()
			}
			return
		}
		if s.server.isDraining() {
			if err := s.sendRejected(sessionCtx, frame.ID, ErrDraining.Error()); err != nil {
				s.close()
			}
			return
		}
	}
	done, err := s.admit()
	if err != nil {
		if err := s.sendRejected(sessionCtx, frame.ID, err.Error()); err != nil {
			s.close()
		}
		return
	}
	if done == nil {
		if err := s.sendError(sessionCtx, frame.ID, errors.New("wire: admission returned nil completion")); err != nil {
			s.close()
		}
		return
	}
	defer done()

	req := Request{
		ID:      frame.ID,
		Op:      frame.Op,
		Tenant:  frame.Tenant,
		Peer:    s.peer,
		Build:   s.build,
		Payload: append([]byte(nil), frame.Payload...),
		Chunks:  state.chunks,
		Session: s.accepted,
	}
	value, err := s.server.dispatch(requestCtx, entry, req)
	if requestErr := requestCtx.Err(); requestErr != nil {
		err = requestErr
	}
	transportErr := state.error()
	if transportErr != nil {
		err = transportErr
	}
	if errors.Is(err, ErrQueueFull) {
		if err := s.sendRejected(sessionCtx, frame.ID, err.Error()); err != nil {
			s.close()
		}
		return
	}
	if err := s.sendValue(requestCtx, sessionCtx, frame.ID, state, value, err); err != nil {
		s.close()
	}
}

func (s *session) sendValue(
	requestCtx, responseCtx context.Context,
	id uint64,
	state *requestState,
	value any,
	handlerErr error,
) error {
	var stream *StreamResponse
	switch typed := value.(type) {
	case StreamResponse:
		stream = &typed
	case *StreamResponse:
		stream = typed
	}
	if stream != nil {
		sequence := streamSequence{}
		for {
			if err := state.responseCredits.acquire(requestCtx); err != nil {
				handlerErr = err
				stream = nil
				break
			}
			select {
			case <-requestCtx.Done():
				handlerErr = requestCtx.Err()
				stream = nil
			case payload, ok := <-stream.Chunks:
				if !ok {
					value = stream.Value
					stream = nil
					break
				}
				current, err := sequence.take()
				if err != nil {
					return err
				}
				if err := s.enqueue(requestCtx, Frame{Kind: FrameStream, ID: id, Sequence: current, Payload: payload}); err != nil {
					return err
				}
			}
			if stream == nil {
				break
			}
		}
	}
	response := Response{}
	if handlerErr != nil {
		response.Err = handlerErr.Error()
	} else {
		payload, err := json.Marshal(value)
		if err != nil {
			response.Err = fmt.Sprintf("wire: marshal response: %v", err)
		} else {
			response.Payload = payload
		}
	}
	return s.sendResponse(responseCtx, id, response)
}

func (s *session) receiveCancel(frame Frame) error {
	if frame.ID == 0 || frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
		return fmt.Errorf("%w: cancel frame", ErrInvalidFrame)
	}
	s.mu.Lock()
	state := s.active[frame.ID]
	s.mu.Unlock()
	if state != nil {
		state.close()
	}
	return nil
}

func (s *session) receiveStream(frame Frame) error {
	if frame.ID == 0 || frame.Op != "" || frame.Tenant != "" {
		return fmt.Errorf("%w: stream frame", ErrInvalidFrame)
	}
	s.mu.Lock()
	state := s.active[frame.ID]
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	if state.inputEnded {
		state.transportErr = ErrStreamOrder
		state.mu.Unlock()
		state.close()
		return nil
	}
	expected, err := state.inputSequence.take()
	if err != nil {
		state.transportErr = err
		state.mu.Unlock()
		state.close()
		return nil
	}
	if frame.Sequence != expected {
		state.transportErr = ErrStreamOrder
		state.mu.Unlock()
		state.close()
		return nil
	}
	end := frame.Flags&FlagEnd != 0
	if end {
		state.inputEnded = true
	}
	state.mu.Unlock()
	chunk := Chunk{Sequence: frame.Sequence, Payload: append([]byte(nil), frame.Payload...), End: end}
	// Waiting here propagates bounded handler pressure to the socket.
	if err := state.inbound.offer(chunk); err != nil {
		if errors.Is(err, errStreamClosed) {
			return nil
		}
		return err
	}
	if end {
		state.inbound.close()
	}
	return nil
}

func (s *session) receiveWindow(frame Frame) error {
	if frame.Flags != 0 || frame.Sequence == 0 || frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
		return fmt.Errorf("%w: response or event window", ErrInvalidFrame)
	}
	if frame.ID == 0 {
		return s.eventCredits.grant(frame.Sequence)
	}
	s.mu.Lock()
	state := s.active[frame.ID]
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	err := state.responseCredits.grant(frame.Sequence)
	if errors.Is(err, errStreamClosed) {
		return nil
	}
	return err
}

func (s *session) deliverRequestChunks(id uint64, state *requestState) {
	defer s.requestWG.Done()
	defer close(state.chunks)
	for {
		select {
		case chunk, ok := <-state.inbound.channel():
			if !ok {
				return
			}
			select {
			case state.chunks <- chunk:
				if err := s.enqueue(s.ctx, Frame{Kind: FrameWindow, ID: id, Sequence: 1}); err != nil {
					s.close()
					return
				}
			case <-state.deliveryDone:
				return
			case <-s.ctx.Done():
				return
			}
		case <-state.deliveryDone:
			return
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *session) removeRequest(id uint64) {
	s.mu.Lock()
	delete(s.active, id)
	s.mu.Unlock()
}

func (s *session) closeRequestInputs() {
	s.mu.Lock()
	states := make([]*requestState, 0, len(s.active))
	for _, state := range s.active {
		states = append(states, state)
	}
	s.mu.Unlock()
	for _, state := range states {
		state.close()
	}
}

func (s *session) sendError(ctx context.Context, id uint64, err error) error {
	return s.sendResponse(ctx, id, Response{Err: err.Error()})
}

func (s *session) sendRejected(ctx context.Context, id uint64, reason string) error {
	return s.sendResponse(ctx, id, Response{Rejected: true, Reason: reason})
}

func (s *session) sendResponse(ctx context.Context, id uint64, response Response) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("wire: marshal envelope: %w", err)
	}
	return s.enqueueAndWait(ctx, Frame{Kind: FrameResponse, Flags: FlagEnd, ID: id, Payload: payload})
}

func (s *session) enqueue(ctx context.Context, frame Frame) error {
	select {
	case s.outbound <- sessionOutbound{frame: frame}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *session) enqueueAndWait(ctx context.Context, frame Frame) error {
	done := make(chan error, 1)
	select {
	case s.outbound <- sessionOutbound{frame: frame, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}
