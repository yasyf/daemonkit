package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
)

var errPeerGoAway = errors.New("wire: peer requested session close")

const reservedOpPrefix = "daemon."

// AcceptedSession is a server-authenticated persistent client session.
type AcceptedSession struct{ s *session }

// ID returns this session's identifier, unique and monotonic within the
// serving process.
func (s *AcceptedSession) ID() uint64 { return s.s.id }

// Peer returns the kernel identity captured once from the accepted socket.
func (s *AcceptedSession) Peer() trust.Peer { return s.s.peer }

// WireBuild returns the business schema the peer presented at attach.
func (s *AcceptedSession) WireBuild() string { return s.s.schema }

// Done closes after this exact authenticated session is fully settled and
// removed from the server.
func (s *AcceptedSession) Done() <-chan struct{} { return s.s.done }

// Disconnected closes when transport intake ends, before admitted requests
// necessarily settle. It is stable for the lifetime of this session.
func (s *AcceptedSession) Disconnected() <-chan struct{} { return s.s.disconnected }

// PushEvent enqueues a server-pushed event with bounded backpressure.
func (s *AcceptedSession) PushEvent(ctx context.Context, event Event) error {
	if event.Topic == "" {
		return errors.New("wire: event topic is required")
	}
	return s.s.enqueue(ctx, Frame{Kind: FrameEvent, Flags: FlagEnd, Op: Op(event.Topic), Payload: event.Payload})
}

type session struct {
	server       *Server
	id           uint64
	conn         net.Conn
	codec        *Codec
	ctx          context.Context
	cancel       context.CancelFunc
	peer         trust.Peer
	lane         Lane
	schema       string
	generation   []byte
	accepted     *AcceptedSession
	outbound     chan sessionOutbound
	requestsDone chan struct{}
	writerDone   chan struct{}
	disconnected chan struct{}
	done         chan struct{}
	writerErr    error

	mu        sync.Mutex
	active    map[uint64]*requestState
	seen      map[uint64]struct{}
	watermark uint64

	requestWG      sync.WaitGroup
	writerWG       sync.WaitGroup
	closeOnce      sync.Once
	disconnectOnce sync.Once
	peerGoAway     atomic.Bool

	handoffMu       sync.Mutex
	handoffAttempts int
	handoffNonces   map[[brokerHandoffNonceBytes]byte]struct{}
}

type sessionOutbound struct {
	frame       Frame
	done        chan error
	beforeWrite func()
}

type requestState struct {
	cancel       context.CancelFunc
	chunks       chan Chunk
	inbound      *boundedStream[Chunk]
	deliveryDone chan struct{}
	deliveryOnce sync.Once
	terminalAck  chan struct{}
	settled      chan struct{}
	settledOnce  sync.Once

	mu            sync.Mutex
	inputSequence streamSequence
	inputEnded    bool
	transportErr  error
	terminalSent  bool
	terminalAcked bool
	sidecar       frameSidecar
}

func (s *requestState) close() {
	s.cancel()
	var sidecar frameSidecar
	s.mu.Lock()
	s.inputEnded = true
	s.sidecar, sidecar = nil, s.sidecar
	s.mu.Unlock()
	if sidecar != nil {
		_ = sidecar.close()
	}
	s.inbound.close()
	s.deliveryOnce.Do(func() { close(s.deliveryDone) })
}

func (s *requestState) takeSidecar() frameSidecar {
	s.mu.Lock()
	defer s.mu.Unlock()
	sidecar := s.sidecar
	s.sidecar = nil
	return sidecar
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
	if errors.Is(err, errPeerGoAway) {
		s.peerGoAway.Store(true)
		s.stop()
		s.requestWG.Wait()
		close(s.requestsDone)
		s.writerWG.Wait()
		if s.writerErr != nil {
			_ = s.conn.Close()
			return s.writerErr
		}
		if err := s.codec.WriteFrame(Frame{Kind: FrameGoAway, Flags: FlagEnd}); err != nil {
			_ = s.conn.Close()
			return err
		}
		_ = s.conn.Close()
		return nil
	}
	s.close()
	s.requestWG.Wait()
	close(s.requestsDone)
	s.writerWG.Wait()
	return err
}

func (s *session) disconnect() {
	s.disconnectOnce.Do(func() { close(s.disconnected) })
}

func (s *session) close() {
	s.stop()
	_ = s.conn.Close()
}

func (s *session) closeOnRequestError() {
	if !s.peerGoAway.Load() {
		s.close()
	}
}

func (s *session) stop() {
	s.closeOnce.Do(func() {
		s.cancel()
		for _, state := range s.snapshotStates() {
			state.close()
		}
		s.disconnect()
	})
}

func (s *session) snapshotStates() []*requestState {
	s.mu.Lock()
	states := make([]*requestState, 0, len(s.active))
	for _, state := range s.active {
		states = append(states, state)
	}
	s.mu.Unlock()
	return states
}

func (s *session) writeLoop() {
	defer s.writerWG.Done()
	defer close(s.writerDone)
	var terminalErr error
	for {
		if terminalErr != nil {
			select {
			case outgoing := <-s.outbound:
				completeSessionOutbound(outgoing, terminalErr)
			case <-s.requestsDone:
				for {
					select {
					case outgoing := <-s.outbound:
						completeSessionOutbound(outgoing, terminalErr)
					default:
						return
					}
				}
			}
			continue
		}
		select {
		case <-s.ctx.Done():
			terminalErr = s.ctx.Err()
		case outgoing := <-s.outbound:
			if outgoing.beforeWrite != nil {
				outgoing.beforeWrite()
			}
			err := s.codec.WriteFrame(outgoing.frame)
			completeSessionOutbound(outgoing, err)
			if err != nil {
				s.writerErr = err
				s.close()
				terminalErr = err
			}
		}
	}
}

func completeSessionOutbound(outgoing sessionOutbound, err error) {
	if outgoing.done != nil {
		outgoing.done <- err
	}
}

func (s *session) settleTerminalRequests(ctx context.Context) error {
	states := make([]*requestState, 0)
	for _, state := range s.snapshotStates() {
		state.mu.Lock()
		terminalSent := state.terminalSent
		state.mu.Unlock()
		if terminalSent {
			states = append(states, state)
		}
	}
	for _, state := range states {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-state.settled:
		}
	}
	return nil
}

func (s *session) cancelRequests() {
	for _, state := range s.snapshotStates() {
		state.cancel()
	}
}

func (s *session) readLoop(ctx context.Context) error {
	for {
		frame, sidecar, err := s.codec.readFrameWithSidecar()
		if err != nil {
			return err
		}
		switch frame.Kind {
		case FrameRequest:
			if err := s.receiveRequest(ctx, frame, sidecar); err != nil {
				return err
			}
		case FrameCancel:
			if sidecar != nil {
				_ = sidecar.close()
				return errInvalidFrameSidecar
			}
			if err := s.receiveCancel(frame); err != nil {
				return err
			}
		case FrameStream:
			if sidecar != nil {
				_ = sidecar.close()
				return errInvalidFrameSidecar
			}
			if err := s.receiveStream(frame); err != nil {
				return err
			}
		case FrameAck:
			if sidecar != nil {
				_ = sidecar.close()
				return errInvalidFrameSidecar
			}
			if err := s.receiveAck(frame); err != nil {
				return err
			}
		case FrameGoAway:
			if sidecar != nil {
				_ = sidecar.close()
				return errInvalidFrameSidecar
			}
			return errPeerGoAway
		default:
			if sidecar != nil {
				_ = sidecar.close()
			}
			return fmt.Errorf("%w: client frame kind %d", ErrInvalidFrame, frame.Kind)
		}
	}
}

func (s *session) receiveRequest(ctx context.Context, frame Frame, sidecar frameSidecar) (err error) {
	defer func() {
		if sidecar != nil {
			err = errors.Join(err, sidecar.close())
		}
	}()
	if frame.ID == 0 || frame.Op == "" || frame.Sequence != 0 {
		return fmt.Errorf("%w: request frame", ErrInvalidFrame)
	}
	if frame.Op == brokerHandoffOp && (frame.Flags != FlagEnd || sidecar == nil) {
		return fmt.Errorf("%w: invalid broker handoff request", errInvalidFrameSidecar)
	}
	if frame.Op != brokerHandoffOp && sidecar != nil {
		return fmt.Errorf("%w: descriptor on operation %q", errInvalidFrameSidecar, frame.Op)
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
	queueLimit := s.server.inboundQueue()
	if queueLimit < 0 {
		panic("wire: negative inbound queue")
	}
	if frame.ID-s.watermark > uint64(queueLimit) {
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
	if len(s.active) >= queueLimit {
		s.mu.Unlock()
		return s.sendRejected(ctx, frame.ID, ErrQueueFull.Error())
	}
	requestCtx, cancel := requestContext(ctx, frame)
	state := &requestState{
		cancel:       cancel,
		chunks:       make(chan Chunk),
		inbound:      newBoundedStream[Chunk](s.server.concurrency),
		deliveryDone: make(chan struct{}),
		terminalAck:  make(chan struct{}),
		settled:      make(chan struct{}),
		sidecar:      sidecar,
	}
	sidecar = nil
	if frame.Flags&FlagEnd != 0 {
		state.inputEnded = true
		state.inbound.close()
	}
	s.server.admitted.Add(1)
	s.active[frame.ID] = state
	s.mu.Unlock()

	s.requestWG.Add(2)
	go s.deliverRequestChunks(state)
	go s.execute(ctx, requestCtx, frame, state)
	return nil
}

func requestContext(parent context.Context, frame Frame) (context.Context, context.CancelFunc) {
	if frame.DeadlineUnixMilli > 0 {
		return context.WithDeadline(parent, time.UnixMilli(frame.DeadlineUnixMilli))
	}
	return context.WithCancel(parent)
}

func (s *session) execute(sessCtx, requestCtx context.Context, frame Frame, state *requestState) {
	defer func() {
		state.close()
		s.removeRequest(frame.ID)
		state.settledOnce.Do(func() { close(state.settled) })
		s.server.admitted.Add(-1)
		s.requestWG.Done()
	}()
	value, err := s.dispatch(requestCtx, frame, state)
	if requestErr := requestCtx.Err(); requestErr != nil {
		err = requestErr
	}
	if transportErr := state.error(); transportErr != nil {
		err = transportErr
	}
	if code, rejected := rejectionCode(err); rejected {
		if err := s.sendAdmittedRejectedCode(sessCtx, frame.ID, state, code, err.Error()); err != nil {
			s.closeOnRequestError()
			return
		}
		if err := s.waitTerminalAck(sessCtx, state); err != nil {
			s.closeOnRequestError()
		}
		return
	}
	if err := s.sendValue(requestCtx, sessCtx, frame.ID, state, value, err); err != nil {
		s.closeOnRequestError()
	}
}

// dispatch routes one request: control verbs match before any runtime
// dispatch, so Runtime.Handle never sees a daemon.-prefixed op.
func (s *session) dispatch(requestCtx context.Context, frame Frame, state *requestState) (any, error) {
	switch {
	case frame.Op == healthOp:
		return s.server.executeHealth()
	case frame.Op == brokerHandoffOp:
		if s.lane != LaneControl {
			return nil, ErrPermissionDenied
		}
		return s.executeBrokerHandoff(frame, state)
	case frame.Op == drainControlOp:
		if s.lane != LaneControl {
			return nil, ErrPermissionDenied
		}
		return s.server.executeDrain(requestCtx)
	case strings.HasPrefix(string(frame.Op), reservedOpPrefix):
		return nil, ErrPermissionDenied
	case s.lane != LaneBusiness:
		return nil, ErrPermissionDenied
	default:
		if err := s.server.gatePhase(); err != nil {
			return nil, err
		}
		select {
		case s.server.handleSem <- struct{}{}:
		case <-requestCtx.Done():
			return nil, requestCtx.Err()
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
		defer func() { <-s.server.handleSem }()
		return s.server.rt.Handle(requestCtx, Request{
			ID:      frame.ID,
			Op:      frame.Op,
			Peer:    s.peer,
			Schema:  s.schema,
			Payload: append([]byte(nil), frame.Payload...),
			Chunks:  state.chunks,
			Session: s.accepted,
		})
	}
}

func rejectionCode(err error) (ResponseCode, bool) {
	switch {
	case errors.Is(err, ErrNotReady):
		return ResponseCodeRuntimeStarting, true
	case errors.Is(err, ErrDraining):
		return ResponseCodeRuntimeDraining, true
	case errors.Is(err, ErrPermissionDenied):
		return ResponseCodePermissionDenied, true
	case errors.Is(err, ErrHandoffReplay):
		return ResponseCodeHandoffReplay, true
	case errors.Is(err, ErrSessionCapacity):
		return ResponseCodeSessionCapacity, true
	default:
		return "", false
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
	if stream != nil && handlerErr == nil {
		sequence := streamSequence{}
		for stream != nil {
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
		}
	}
	response := Response{Ack: true}
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
	if err := s.sendAdmittedResponse(responseCtx, id, state, response); err != nil {
		return err
	}
	return s.waitTerminalAck(responseCtx, state)
}

func (s *session) receiveCancel(frame Frame) error {
	s.mu.Lock()
	state := s.active[frame.ID]
	s.mu.Unlock()
	if state != nil {
		state.close()
	}
	return nil
}

func (s *session) receiveStream(frame Frame) error {
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

func (s *session) receiveAck(frame Frame) error {
	if !bytes.Equal(frame.Payload, s.generation) {
		return fmt.Errorf("%w: acknowledgement session generation", ErrInvalidFrame)
	}
	s.mu.Lock()
	state := s.active[frame.ID]
	s.mu.Unlock()
	if state == nil {
		return fmt.Errorf("%w: acknowledgement request %d", ErrInvalidFrame, frame.ID)
	}
	state.mu.Lock()
	if !state.terminalSent || state.terminalAcked {
		state.mu.Unlock()
		return fmt.Errorf("%w: duplicate acknowledgement %d", ErrInvalidFrame, frame.ID)
	}
	state.terminalAcked = true
	close(state.terminalAck)
	state.mu.Unlock()
	select {
	case <-state.settled:
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	return nil
}

func (s *session) waitTerminalAck(ctx context.Context, state *requestState) error {
	timer := time.NewTimer(s.server.writeTimeout())
	defer timer.Stop()
	select {
	case <-state.terminalAck:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-timer.C:
		return errors.New("wire: terminal acknowledgement timeout")
	}
}

func (s *session) deliverRequestChunks(state *requestState) {
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

func (s *session) sendRejected(ctx context.Context, id uint64, reason string) error {
	return s.sendResponse(ctx, id, Response{Rejected: true, Reason: reason})
}

func (s *session) sendAdmittedRejectedCode(
	ctx context.Context,
	id uint64,
	state *requestState,
	code ResponseCode,
	reason string,
) error {
	return s.sendAdmittedResponse(ctx, id, state, Response{
		Rejected: true, Ack: true, Code: code, Reason: reason,
	})
}

func (s *session) sendResponse(ctx context.Context, id uint64, response Response) error {
	return s.sendResponseWritten(ctx, id, response, nil)
}

func (s *session) sendAdmittedResponse(
	ctx context.Context,
	id uint64,
	state *requestState,
	response Response,
) error {
	return s.sendResponseWritten(ctx, id, response, func() {
		state.mu.Lock()
		state.terminalSent = true
		state.mu.Unlock()
	})
}

func (s *session) sendResponseWritten(
	ctx context.Context,
	id uint64,
	response Response,
	beforeWrite func(),
) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("wire: marshal envelope: %w", err)
	}
	return s.enqueueAndWait(ctx, Frame{Kind: FrameResponse, Flags: FlagEnd, ID: id, Payload: payload}, beforeWrite)
}

func (s *session) enqueue(ctx context.Context, frame Frame) error {
	select {
	case s.outbound <- sessionOutbound{frame: frame}:
		select {
		case <-s.writerDone:
			return s.ctx.Err()
		default:
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.writerDone:
		return s.ctx.Err()
	}
}

func (s *session) enqueueAndWait(ctx context.Context, frame Frame, beforeWrite func()) error {
	done := make(chan error, 1)
	select {
	case s.outbound <- sessionOutbound{frame: frame, done: done, beforeWrite: beforeWrite}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.writerDone:
		return s.ctx.Err()
	}
	return <-done
}
