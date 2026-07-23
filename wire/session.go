package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/trust"
)

var errPeerGoAway = errors.New("wire: peer requested session close")

const (
	writerIdle uint32 = iota
	writerActive
	writerDraining
)

// AcceptedSession is a server-authenticated persistent client session.
type AcceptedSession struct{ s *session }

// Peer returns the OS identity captured once from the accepted socket.
func (s *AcceptedSession) Peer() Peer { return s.s.peer }

// WireBuild returns the client schema build supplied by the mandatory handshake.
func (s *AcceptedSession) WireBuild() string { return s.s.wireBuild }

// Protected reports whether pre-capacity trust classified this exact session
// for protected control and observation traffic.
func (s *AcceptedSession) Protected() bool { return s.s.protected }

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
	if err := s.s.eventCredits.acquire(ctx); err != nil {
		return err
	}
	return s.s.enqueue(ctx, Frame{Kind: FrameEvent, Flags: FlagEnd, Op: Op(event.Topic), Payload: event.Payload})
}

type session struct {
	server         *Server
	conn           net.Conn
	codec          *Codec
	ctx            context.Context
	cancel         context.CancelFunc
	peer           Peer
	role           trust.PeerRole
	protected      bool
	wireBuild      string
	generation     []byte
	admit          daemonAdmission
	admitProtected daemonAdmission
	accepted       *AcceptedSession
	outbound       chan sessionOutbound
	eventCredits   *creditWindow
	lifecycleLane  *latestWriteLane
	requestsDone   chan struct{}
	writerDone     chan struct{}
	disconnected   chan struct{}
	done           chan struct{}
	writerErr      error

	mu        sync.Mutex
	active    map[uint64]*requestState
	seen      map[uint64]struct{}
	watermark uint64

	requestWG      sync.WaitGroup
	writerWG       sync.WaitGroup
	closeOnce      sync.Once
	disconnectOnce sync.Once
	peerGoAway     atomic.Bool

	writerStateMu   sync.Mutex
	writerState     uint32
	activeWriteDone chan struct{}
}

type sessionOutbound struct {
	frame               Frame
	done                chan error
	beforeWrite         func()
	afterWrite          func(error)
	lifecycleLane       *latestWriteLane
	lifecycleGeneration uint64
}

type lifecycleWriteReceipt struct {
	lane       *latestWriteLane
	generation uint64
}

func (r lifecycleWriteReceipt) wait(ctx context.Context) error {
	return r.lane.wait(ctx, r.generation)
}

type requestState struct {
	cancel            context.CancelFunc
	chunks            chan Chunk
	inbound           *boundedStream[Chunk]
	responseCredits   *creditWindow
	deliveryDone      chan struct{}
	deliveryOnce      sync.Once
	terminalAck       chan struct{}
	terminalWrite     chan error
	settled           chan struct{}
	settledOnce       sync.Once
	terminalWriteOnce sync.Once

	mu            sync.Mutex
	inputSequence streamSequence
	inputEnded    bool
	transportErr  error
	terminalSent  bool
	terminalAcked bool
}

func (s *requestState) close() {
	s.cancel()
	s.markTerminalWrite(context.Canceled)
	s.mu.Lock()
	s.inputEnded = true
	s.mu.Unlock()
	s.inbound.close()
	s.responseCredits.close()
	s.deliveryOnce.Do(func() { close(s.deliveryDone) })
}

func (s *requestState) markTerminalWrite(err error) {
	terminalWrite := s.terminalWriteChannel()
	s.terminalWriteOnce.Do(func() {
		terminalWrite <- err
		close(terminalWrite)
	})
}

func (s *requestState) terminalWriteChannel() chan error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalWrite == nil {
		s.terminalWrite = make(chan error, 1)
	}
	return s.terminalWrite
}

func (s *requestState) error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transportErr
}

func (s *session) run(ctx context.Context, releaseCapacity func()) error {
	s.writerWG.Add(1)
	go s.writeLoop()
	err := s.readLoop(ctx)
	if errors.Is(err, errPeerGoAway) {
		s.peerGoAway.Store(true)
		activeWrite := s.beginWriterDrain()
		s.stop()
		interruptDone := s.interruptActiveWrite(activeWrite)
		s.closeRequestInputs()
		s.requestWG.Wait()
		close(s.requestsDone)
		s.writerWG.Wait()
		if interruptDone != nil {
			<-interruptDone
		}
		releaseCapacity()
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
	s.closeRequestInputs()
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
		s.eventCredits.close()
		s.lifecycleLane.fail(s.ctx.Err())
		s.mu.Lock()
		states := make([]*requestState, 0, len(s.active))
		for _, state := range s.active {
			states = append(states, state)
		}
		s.mu.Unlock()
		for _, state := range states {
			state.close()
		}
		s.disconnect()
	})
}

func (s *session) beginWrite() (chan struct{}, bool) {
	s.writerStateMu.Lock()
	defer s.writerStateMu.Unlock()
	if s.writerState != writerIdle {
		return nil, false
	}
	done := make(chan struct{})
	s.writerState = writerActive
	s.activeWriteDone = done
	return done, true
}

func (s *session) finishWrite(done chan struct{}) {
	s.writerStateMu.Lock()
	defer s.writerStateMu.Unlock()
	if s.activeWriteDone != done {
		panic("wire: writer completion identity mismatch")
	}
	s.activeWriteDone = nil
	if s.writerState == writerActive {
		s.writerState = writerIdle
	}
	close(done)
}

func (s *session) beginWriterDrain() <-chan struct{} {
	s.writerStateMu.Lock()
	defer s.writerStateMu.Unlock()
	s.writerState = writerDraining
	return s.activeWriteDone
}

func (s *session) interruptActiveWrite(active <-chan struct{}) <-chan struct{} {
	if active == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(s.server.writeTimeout())
		defer timer.Stop()
		select {
		case <-active:
		case <-timer.C:
			select {
			case <-active:
			default:
				_ = s.conn.Close()
			}
		}
	}()
	return done
}

func (s *session) writeLoop() {
	defer s.writerWG.Done()
	defer close(s.writerDone)
	var (
		terminalErr     error
		pendingOrdinary *sessionOutbound
	)
	for {
		if terminalErr != nil {
			if pendingOrdinary != nil {
				completeSessionOutbound(*pendingOrdinary, terminalErr)
				pendingOrdinary = nil
			}
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
		var outgoing sessionOutbound
		if lifecycle, ok := s.takeLifecycleOutbound(); ok {
			outgoing = lifecycle
		} else if pendingOrdinary != nil {
			outgoing = *pendingOrdinary
			pendingOrdinary = nil
		} else {
			select {
			case <-s.ctx.Done():
				terminalErr = s.ctx.Err()
				continue
			case <-s.lifecycleLane.notify:
				continue
			case ordinary := <-s.outbound:
				if lifecycle, ok := s.takeLifecycleOutbound(); ok {
					pendingOrdinary = &ordinary
					outgoing = lifecycle
				} else {
					outgoing = ordinary
				}
			}
		}
		if outgoing.beforeWrite != nil {
			outgoing.beforeWrite()
		}
		writeDone, ok := s.beginWrite()
		if !ok {
			err := s.ctx.Err()
			if err == nil {
				err = context.Canceled
			}
			completeSessionOutbound(outgoing, err)
			terminalErr = err
			continue
		}
		var finishLifecycle func(error)
		if outgoing.lifecycleLane != nil {
			generation, payload, finish, err := outgoing.lifecycleLane.beginWrite(outgoing.lifecycleGeneration)
			if err != nil {
				s.finishWrite(writeDone)
				completeSessionOutbound(outgoing, err)
				terminalErr = err
				continue
			}
			outgoing.lifecycleGeneration = generation
			outgoing.frame.Payload = payload
			finishLifecycle = finish
		}
		err := s.codec.WriteFrame(outgoing.frame)
		if finishLifecycle != nil {
			finishLifecycle(err)
		}
		s.finishWrite(writeDone)
		completeSessionOutbound(outgoing, err)
		if err != nil {
			s.writerErr = err
			s.close()
			terminalErr = err
		}
	}
}

func completeSessionOutbound(outgoing sessionOutbound, err error) {
	if outgoing.afterWrite != nil {
		outgoing.afterWrite(err)
	}
	if outgoing.done != nil {
		outgoing.done <- err
	}
}

func (s *session) takeLifecycleOutbound() (sessionOutbound, bool) {
	generation, payload, ok, err := s.lifecycleLane.tryTake()
	if err != nil || !ok {
		return sessionOutbound{}, false
	}
	return sessionOutbound{
		frame:         Frame{Kind: FrameLifecycle, Flags: FlagEnd, Payload: payload},
		lifecycleLane: s.lifecycleLane, lifecycleGeneration: generation,
	}, true
}

func (s *session) offerLifecycle(payload []byte, terminal bool) (lifecycleWriteReceipt, error) {
	if len(payload) == 0 {
		return lifecycleWriteReceipt{}, fmt.Errorf("%w: empty lifecycle payload", ErrInvalidFrame)
	}
	generation, err := s.lifecycleLane.offer(append([]byte(nil), payload...), terminal)
	if err != nil {
		return lifecycleWriteReceipt{}, err
	}
	return lifecycleWriteReceipt{lane: s.lifecycleLane, generation: generation}, nil
}

func (s *session) responseWritten(id uint64) (<-chan error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.active[id]
	if state == nil {
		return nil, fmt.Errorf("wire: request %d is not active", id)
	}
	return state.terminalWriteChannel(), nil
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
		case FrameAck:
			if err := s.receiveAck(frame); err != nil {
				return err
			}
		case FrameGoAway:
			return errPeerGoAway
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
	if entry.route == routeObservation && frame.Flags&FlagEnd == 0 {
		s.mu.Unlock()
		return s.sendRejected(ctx, frame.ID, ErrObservationUnary.Error())
	}
	if entry.route == routeLifecycle && (frame.Flags&FlagEnd == 0 || frame.Tenant != "") {
		s.mu.Unlock()
		return s.sendRejected(ctx, frame.ID, ErrInvalidFrame.Error())
	}
	requestCtx, cancel := s.server.requestContext(ctx, frame)
	state := &requestState{
		cancel:          cancel,
		chunks:          make(chan Chunk),
		inbound:         newBoundedStream[Chunk](s.server.streamQueue()),
		responseCredits: newCreditWindow(),
		deliveryDone:    make(chan struct{}),
		terminalAck:     make(chan struct{}),
		terminalWrite:   make(chan error, 1),
		settled:         make(chan struct{}),
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
	var finishAdmission func()
	defer func() {
		state.close()
		s.removeRequest(frame.ID)
		if finishAdmission != nil {
			finishAdmission()
		}
		state.settledOnce.Do(func() { close(state.settled) })
		s.requestWG.Done()
	}()

	switch entry.route {
	case routeStop:
		if s.wireBuild != s.server.WireBuild {
			if err := s.sendRejected(sessionCtx, frame.ID, ErrBuildMismatch.Error()); err != nil {
				s.closeOnRequestError()
			}
			return
		}
		var err error
		requestCtx, err = s.server.authorizeStopControl(requestCtx, s.peer, s.role, frame.Payload)
		if err != nil {
			code := ResponseCode("")
			if errors.Is(err, ErrPermissionDenied) {
				code = ResponseCodePermissionDenied
			}
			if err := s.sendRejectedCode(sessionCtx, frame.ID, code, err.Error()); err != nil {
				s.closeOnRequestError()
			}
			return
		}
	case routeBusiness, routeObservation, routeLifecycle:
		if s.wireBuild != s.server.WireBuild {
			if err := s.sendRejected(sessionCtx, frame.ID, ErrBuildMismatch.Error()); err != nil {
				s.closeOnRequestError()
			}
			return
		}
		if entry.route == routeLifecycle {
			if err := s.server.authorizeLifecycleControl(frame.Op, s.role); err != nil {
				if err := s.sendRejectedCode(sessionCtx, frame.ID, ResponseCodePermissionDenied, err.Error()); err != nil {
					s.closeOnRequestError()
				}
				return
			}
		}
		err := s.server.authorizePreReady(requestCtx, entry, Request{
			Op: frame.Op, Tenant: frame.Tenant, Peer: s.peer, WireBuild: s.wireBuild,
			Payload: append([]byte(nil), frame.Payload...),
		})
		if err != nil {
			code := ResponseCode("")
			switch {
			case errors.Is(err, ErrDraining):
				code = ResponseCodeRuntimeDraining
			case errors.Is(err, ErrNotReady):
				code = ResponseCodeRuntimeStarting
			}
			if err := s.sendRejectedCode(sessionCtx, frame.ID, code, err.Error()); err != nil {
				s.closeOnRequestError()
			}
			return
		}
	default:
		panic("wire: invalid route class")
	}
	admit := s.admit
	var releaseObservation func()
	if entry.route == routeStop || entry.route == routeLifecycle {
		admit = s.admitProtected
	}
	if entry.route == routeObservation {
		var ok bool
		releaseObservation, ok = s.server.acquireObservation()
		if !ok {
			if err := s.sendRejected(sessionCtx, frame.ID, ErrObservationBusy.Error()); err != nil {
				s.closeOnRequestError()
			}
			return
		}
		defer releaseObservation()
	}
	publication, done, err := admit()
	if err != nil {
		code := ResponseCode("")
		if errors.Is(err, daemon.ErrDraining) {
			code = ResponseCodeRuntimeDraining
			err = ErrDraining
		} else if errors.Is(err, daemon.ErrRuntimeNotReady) {
			code = ResponseCodeRuntimeStarting
			err = ErrNotReady
		}
		if err := s.sendRejectedCode(sessionCtx, frame.ID, code, err.Error()); err != nil {
			s.closeOnRequestError()
		}
		return
	}
	if done == nil {
		if err := s.sendError(sessionCtx, frame.ID, errors.New("wire: admission returned nil completion")); err != nil {
			s.closeOnRequestError()
		}
		return
	}
	finishAdmission = sync.OnceFunc(done)

	req := Request{
		ID:          frame.ID,
		Op:          frame.Op,
		Tenant:      frame.Tenant,
		Peer:        s.peer,
		WireBuild:   s.wireBuild,
		Payload:     append([]byte(nil), frame.Payload...),
		Chunks:      state.chunks,
		Session:     s.accepted,
		Publication: publication,
	}
	value, err := s.server.dispatch(requestCtx, entry, req)
	if requestErr := requestCtx.Err(); requestErr != nil {
		err = requestErr
	}
	transportErr := state.error()
	if transportErr != nil {
		err = transportErr
	}
	if entry.route == routeLifecycle && frame.Op == runtimeReadinessSubscribeOp && errors.Is(err, ErrDraining) {
		if err := s.sendAdmittedRejectedCode(
			sessionCtx, frame.ID, state, ResponseCodeRuntimeDraining, ErrDraining.Error(),
		); err != nil {
			s.closeOnRequestError()
			return
		}
		finishAdmission()
		if err := s.waitTerminalAck(sessionCtx, state); err != nil {
			s.closeOnRequestError()
		}
		return
	}
	if errors.Is(err, ErrQueueFull) {
		if err := s.sendAdmittedRejected(sessionCtx, frame.ID, state, err.Error()); err != nil {
			s.closeOnRequestError()
			return
		}
		finishAdmission()
		if err := s.waitTerminalAck(sessionCtx, state); err != nil {
			s.closeOnRequestError()
		}
		return
	}
	var responseWritten <-chan error
	var onResponseWritten func()
	if entry.route == routeLifecycle && frame.Op == runtimeReadinessSubscribeOp {
		responseWritten, err = s.responseWritten(frame.ID)
		if err != nil {
			s.closeOnRequestError()
			return
		}
		onResponseWritten = func() { s.server.startReadiness(s) }
	}
	if err := s.sendValue(
		requestCtx, sessionCtx, frame.ID, state, value, err,
		finishAdmission, responseWritten, onResponseWritten,
	); err != nil {
		s.closeOnRequestError()
	}
}

func (s *session) sendValue(
	requestCtx, responseCtx context.Context,
	id uint64,
	state *requestState,
	value any,
	handlerErr error,
	finishAdmission func(),
	responseWritten <-chan error,
	onResponseWritten func(),
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
	if responseWritten != nil {
		select {
		case err := <-responseWritten:
			if err != nil {
				return err
			}
		case <-responseCtx.Done():
			return responseCtx.Err()
		}
		onResponseWritten()
	}
	finishAdmission()
	return s.waitTerminalAck(responseCtx, state)
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
		err := s.eventCredits.grant(frame.Sequence)
		if errors.Is(err, errStreamClosed) && s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		return err
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

func (s *session) receiveAck(frame Frame) error {
	if frame.Flags != FlagEnd || frame.ID == 0 || frame.Sequence != 0 || frame.Op != "" ||
		frame.Tenant != "" || len(frame.Payload) != sessionGenerationBytes {
		return fmt.Errorf("%w: acknowledgement frame", ErrInvalidFrame)
	}
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
					s.closeOnRequestError()
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
	return s.sendRejectedCode(ctx, id, "", reason)
}

func (s *session) sendRejectedCode(ctx context.Context, id uint64, code ResponseCode, reason string) error {
	return s.sendResponse(ctx, id, Response{Rejected: true, Code: code, Reason: reason})
}

func (s *session) sendAdmittedRejected(
	ctx context.Context,
	id uint64,
	state *requestState,
	reason string,
) error {
	return s.sendAdmittedResponse(ctx, id, state, Response{Rejected: true, Ack: true, Reason: reason})
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
	return s.sendResponseWritten(ctx, id, response, nil, nil)
}

func (s *session) sendAdmittedResponse(
	ctx context.Context,
	id uint64,
	state *requestState,
	response Response,
) error {
	err := s.sendResponseWritten(ctx, id, response, func() {
		state.mu.Lock()
		state.terminalSent = true
		state.mu.Unlock()
	}, state.markTerminalWrite)
	state.markTerminalWrite(err)
	return err
}

func (s *session) sendResponseWritten(
	ctx context.Context,
	id uint64,
	response Response,
	beforeWrite func(),
	afterWrite func(error),
) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("wire: marshal envelope: %w", err)
	}
	return s.enqueueAndWait(
		ctx,
		Frame{Kind: FrameResponse, Flags: FlagEnd, ID: id, Payload: payload},
		beforeWrite,
		afterWrite,
	)
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

func (s *session) enqueueAndWait(
	ctx context.Context,
	frame Frame,
	beforeWrite func(),
	afterWrite func(error),
) error {
	return s.enqueueOnAndWait(ctx, s.outbound, frame, beforeWrite, afterWrite)
}

func (s *session) enqueueOnAndWait(
	ctx context.Context,
	queue chan<- sessionOutbound,
	frame Frame,
	beforeWrite func(),
	afterWrite func(error),
) error {
	done := make(chan error, 1)
	select {
	case queue <- sessionOutbound{frame: frame, done: done, beforeWrite: beforeWrite, afterWrite: afterWrite}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.writerDone:
		return s.ctx.Err()
	}
	return <-done
}
