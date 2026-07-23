package wire

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	// ErrUntrustedPeer means the accepted unix peer failed the same-uid floor.
	ErrUntrustedPeer = errors.New("wire: untrusted peer")
	// ErrHandshake means the first frame did not establish a v1 session.
	ErrHandshake = errors.New("wire: handshake failed")
	// ErrServerStarted means Serve was called more than once.
	ErrServerStarted = errors.New("wire: server already started")
	// ErrBuildMismatch means an ordinary request came from a different build.
	ErrBuildMismatch = errors.New("wire: client build does not match server build")
	// ErrProtectedSessionRequired means a protected request came from an
	// authenticated ordinary session rather than a protected peer.
	ErrProtectedSessionRequired = errors.New("wire: protected session required")
	// ErrObservationBusy means the bounded observation lane is occupied.
	ErrObservationBusy = errors.New("wire: observation lane is busy")
	// ErrObservationUnary means an observation attempted streamed input.
	ErrObservationUnary = errors.New("wire: observations require unary input")
	// ErrNotReady means the route cannot dispatch before runtime publication.
	ErrNotReady = errors.New("wire: runtime is starting")
)

const (
	defaultWorkers           = 8
	defaultBacklog           = 32
	defaultInboundQueue      = 64
	defaultOutboundQueue     = 128
	defaultStreamQueue       = 16
	defaultMaxSessions       = 64
	defaultPeerVerifyTimeout = 2 * time.Second
	defaultHandshakeTimeout  = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
)

var reservedOps = map[Op]struct{}{
	stopControlOp: {},
}

type class uint8

const (
	classControl class = iota
	classConcurrent
)

type routeClass uint8

const (
	routeBusiness routeClass = iota
	routeObservation
	routeStop
)

type entry struct {
	class    class
	route    routeClass
	h        Handler
	preReady bool
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

type sessionCapacity uint8

const (
	ordinarySessionCapacity sessionCapacity = iota + 1
	protectedSessionCapacity
)

// Server serves persistent, multiplexed v1 sessions on a listener owned by its caller.
// Register business, observation, and stop-control handlers before Serve.
type Server struct {
	// WireBuild is the stable schema identity sent during the mandatory handshake.
	WireBuild string
	// Trust augments the non-optional same-effective-uid trust floor. It must
	// return when ctx is canceled.
	Trust func(context.Context, Peer) error
	// Ladder supplies per-operation server deadlines.
	Ladder Ladder
	// Workers caps simultaneous concurrent handlers.
	Workers int
	// Backlog caps queued concurrent handlers beyond Workers.
	Backlog int
	// InboundQueue caps active request IDs per accepted session.
	InboundQueue int
	// OutboundQueue caps unsent response, stream, and event frames per session.
	OutboundQueue int
	// StreamQueue caps unread inbound chunks per request.
	StreamQueue int
	// MaxSessions caps accepted and handshaking connections.
	MaxSessions int
	// PeerVerificationTimeout bounds pre-capacity trust and classification.
	PeerVerificationTimeout time.Duration
	// MaxFrame caps each encoded frame.
	MaxFrame int
	// HandshakeTimeout bounds the mandatory first-frame exchange.
	HandshakeTimeout time.Duration
	// WriteTimeout bounds each frame write.
	WriteTimeout time.Duration
	// Log receives accept and session diagnostics.
	Log *slog.Logger

	mu                          sync.Mutex
	handlers                    map[Op]entry
	onActivity                  func()
	listener                    net.Listener
	started                     bool
	draining                    bool
	sessions                    map[*session]struct{}
	streamWindow                uint32
	protectedSessionClassifier  ProtectedSessionClassifier
	reservedProtectedSessions   int
	stopControlVerifier         StopControlVerifier
	stopTargetProcessGeneration string
	hasObservations             bool
	readiness                   ReadinessBarrier
	readinessCancel             context.CancelFunc
	readyPublished              bool
	bootstrapRoutes             map[Op]BootstrapAuthorizer
	publicationMu               sync.RWMutex

	queue                 chan job
	slots                 chan struct{}
	observationSlots      chan struct{}
	ordinarySessionSlots  chan struct{}
	protectedSessionSlots chan struct{}
	poolWG                sync.WaitGroup
	sessionWG             sync.WaitGroup
	closeOnce             sync.Once
}

// RegisterControl registers a control handler outside the worker pool.
func (s *Server) RegisterControl(op Op, h Handler) {
	s.register(op, classControl, routeBusiness, h)
}

// RegisterConcurrent registers a bounded worker-pool handler.
func (s *Server) RegisterConcurrent(op Op, h Handler) {
	s.register(op, classConcurrent, routeBusiness, h)
}

func (s *Server) register(op Op, class class, route routeClass, h Handler) {
	if op == "" || h == nil {
		panic("wire: operation and handler are required")
	}
	if _, reserved := reservedOps[op]; reserved && route != routeStop {
		panic(fmt.Sprintf("wire: op %q is a reserved control op", op))
	}
	if strings.HasPrefix(string(op), "daemon.") && route != routeStop {
		panic(fmt.Sprintf("wire: op %q uses daemonkit's private namespace", op))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		panic("wire: handlers cannot be registered after Serve")
	}
	if s.handlers == nil {
		s.handlers = make(map[Op]entry)
	}
	if _, exists := s.handlers[op]; exists {
		panic(fmt.Sprintf("wire: op %q already registered", op))
	}
	s.handlers[op] = entry{class: class, route: route, h: h}
}

// OnActivity installs a callback invoked immediately before each admitted handler.
func (s *Server) OnActivity(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		panic("wire: activity callback cannot change after Serve")
	}
	s.onActivity = f
}

// Serve accepts v1 sessions until ctx is cancelled. admit runs for every
// request that clears the pre-admission checks; its done runs when the
// request's execution settles, including cancellation and write-failure paths.
func (s *Server) Serve(
	ctx context.Context,
	listener net.Listener,
	ready func() error,
	admit, admitProtected func() (func(), error),
) error {
	if listener == nil {
		return errors.New("wire: listener is required")
	}
	if admit == nil {
		return errors.New("wire: admission callback is required")
	}
	if admitProtected == nil {
		return errors.New("wire: protected admission callback is required")
	}
	if ready == nil {
		return errors.New("wire: readiness callback is required")
	}
	workers, err := s.start(listener)
	if err != nil {
		return err
	}
	s.startWorkers(workers)
	if s.readiness == nil {
		if err := ready(); err != nil {
			_ = s.CloseIntake()
			s.stopWorkers()
			return fmt.Errorf("wire: publish readiness: %w", err)
		}
		s.publishReady()
	}
	acceptDone := make(chan error, 1)
	go func() { acceptDone <- s.accept(ctx, admit, admitProtected) }()
	if s.readiness != nil {
		readinessCtx, cancelReadiness := s.installReadinessContext(ctx)
		defer s.clearReadinessContext(cancelReadiness)
		readinessDone := make(chan error, 1)
		go func() { readinessDone <- s.runReadiness(readinessCtx, ready) }()
		select {
		case readinessErr := <-readinessDone:
			if readinessErr == nil {
				s.publishReady()
				break
			}
			_ = s.CloseIntake()
			acceptErr := <-acceptDone
			s.closeSessions()
			s.sessionWG.Wait()
			s.stopWorkers()
			return errors.Join(
				fmt.Errorf("wire: publish readiness: %w", readinessErr),
				wrapAcceptError(acceptErr),
			)
		case acceptErr := <-acceptDone:
			cancelReadiness()
			readinessErr := <-readinessDone
			s.closeSessions()
			s.sessionWG.Wait()
			s.stopWorkers()
			return errors.Join(wrapAcceptError(acceptErr), readinessErr)
		case <-ctx.Done():
			_ = s.CloseIntake()
			acceptErr := <-acceptDone
			readinessErr := <-readinessDone
			s.closeSessions()
			s.sessionWG.Wait()
			s.stopWorkers()
			return errors.Join(wrapAcceptError(acceptErr), readinessErr)
		}
	}
	var acceptErr error
	select {
	case <-ctx.Done():
		_ = s.CloseIntake()
		acceptErr = <-acceptDone
	case acceptErr = <-acceptDone:
		if acceptErr == nil || errors.Is(acceptErr, net.ErrClosed) {
			<-ctx.Done()
		} else {
			_ = s.CloseIntake()
		}
	}

	s.closeSessions()
	s.sessionWG.Wait()
	s.stopWorkers()
	return wrapAcceptError(acceptErr)
}

// ServeSession serves one daemonkit-issued existing duplex session through the
// same handshake, capacity, admission, backpressure, cancellation, and drain
// engine as listener sessions. It takes ownership of conn and joins the session
// synchronously. Spawned-parent identities are always ordinary, so protected
// observation and stop-control operations remain unavailable.
func (s *Server) ServeSession(
	ctx context.Context,
	conn net.Conn,
	identity SessionIdentity,
	ready func() error,
	admit, admitProtected func() (func(), error),
) error {
	if conn == nil {
		return errors.New("wire: existing session connection is required")
	}
	if admit == nil {
		return errors.New("wire: admission callback is required")
	}
	if admitProtected == nil {
		return errors.New("wire: protected admission callback is required")
	}
	if ready == nil {
		return errors.New("wire: readiness callback is required")
	}
	defer conn.Close()
	peer, allowProtected, err := identity.authenticatedPeer()
	if err != nil {
		return err
	}
	workers, err := s.start(nil)
	if err != nil {
		return err
	}
	s.startWorkers(workers)
	if err := ready(); err != nil {
		s.stopWorkers()
		return fmt.Errorf("wire: publish readiness: %w", err)
	}
	s.publishReady()
	verifyCtx, cancelVerify := context.WithTimeout(ctx, s.peerVerificationTimeout())
	protected := false
	if allowProtected {
		protected, err = s.verifyPeer(verifyCtx, peer)
	} else {
		err = s.verifyOrdinaryPeer(verifyCtx, peer)
	}
	cancelVerify()
	if err != nil {
		s.stopWorkers()
		return err
	}
	capacity, ok := s.acquireSessionCapacity(protected)
	if !ok {
		s.stopWorkers()
		return errors.New("wire: existing session capacity exhausted")
	}
	releaseCapacity := sync.OnceFunc(func() { s.releaseSessionCapacity(capacity) })
	s.sessionWG.Add(1)
	sessionErr := func() error {
		defer releaseCapacity()
		defer s.sessionWG.Done()
		return s.serveConn(ctx, conn, peer, protected, admit, admitProtected, releaseCapacity)
	}()
	s.closeSessions()
	s.sessionWG.Wait()
	s.stopWorkers()
	if isDisconnect(sessionErr) {
		return nil
	}
	return sessionErr
}

func (s *Server) start(listener net.Listener) (int, error) {
	if s.WireBuild == "" {
		return 0, errors.New("wire: WireBuild is required")
	}
	if err := s.validateSessionCapacity(); err != nil {
		return 0, err
	}
	streamWindow, err := uint32Length("stream queue", s.streamQueue())
	if err != nil {
		return 0, errors.New("wire: stream queue exceeds protocol window")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return 0, ErrServerStarted
	}
	if err := s.validateBootstrapRoutesLocked(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.started = true
	s.listener = listener
	s.sessions = make(map[*session]struct{})
	s.streamWindow = streamWindow
	if s.Log == nil {
		s.Log = slog.Default()
	}
	workers := s.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	backlog := s.Backlog
	if backlog < 0 {
		backlog = 0
	} else if backlog == 0 {
		backlog = defaultBacklog
	}
	s.queue = make(chan job, backlog)
	s.slots = make(chan struct{}, workers+backlog)
	s.ordinarySessionSlots = make(chan struct{}, s.maxSessions()-s.reservedProtectedSessions)
	s.protectedSessionSlots = make(chan struct{}, s.reservedProtectedSessions)
	if s.hasObservations {
		s.observationSlots = make(chan struct{}, 1)
	}
	s.mu.Unlock()
	return workers, nil
}

func (s *Server) validateBootstrapRoutesLocked() error {
	for op := range s.bootstrapRoutes {
		registered, exists := s.handlers[op]
		if !exists || registered.route != routeBusiness {
			return fmt.Errorf("wire: bootstrap route %q is not a registered business route", op)
		}
	}
	return nil
}

func (s *Server) startWorkers(workers int) {
	for range workers {
		s.poolWG.Add(1)
		go s.worker()
	}
}

func (s *Server) stopWorkers() {
	close(s.queue)
	s.poolWG.Wait()
}

func wrapAcceptError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return fmt.Errorf("wire: accept: %w", err)
}

// CloseIntake prevents new connections and new ordinary requests. Accepted
// sessions stay alive so admitted work and protected control can settle.
func (s *Server) CloseIntake() error {
	s.mu.Lock()
	s.draining = true
	listener := s.listener
	cancelReadiness := s.readinessCancel
	s.mu.Unlock()
	if cancelReadiness != nil {
		cancelReadiness()
	}
	if listener == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() { err = listener.Close() })
	return err
}

func (s *Server) accept(ctx context.Context, admit, admitProtected func() (func(), error)) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		unix, ok := conn.(*net.UnixConn)
		if !ok {
			_ = conn.Close()
			continue
		}
		peer, err := PeerFromConn(unix)
		if err != nil {
			_ = conn.Close()
			s.Log.Debug("wire: reject unidentified peer", "err", err)
			continue
		}
		verifyCtx, cancelVerify := context.WithTimeout(ctx, s.peerVerificationTimeout())
		protected, err := s.verifyPeer(verifyCtx, peer)
		cancelVerify()
		if err != nil {
			_ = conn.Close()
			s.Log.Debug("wire: reject untrusted peer", "err", err)
			continue
		}
		capacity, ok := s.acquireSessionCapacity(protected)
		if !ok {
			_ = conn.Close()
			continue
		}
		s.sessionWG.Add(1)
		go func(conn net.Conn, peer Peer, protected bool, capacity sessionCapacity) {
			releaseCapacity := sync.OnceFunc(func() { s.releaseSessionCapacity(capacity) })
			defer func() {
				releaseCapacity()
				s.sessionWG.Done()
			}()
			if err := s.serveConn(ctx, conn, peer, protected, admit, admitProtected, releaseCapacity); err != nil && !isDisconnect(err) {
				s.Log.Debug("wire: session ended", "err", err)
			}
		}(conn, peer, protected, capacity)
	}
}

func (s *Server) serveConn(
	ctx context.Context,
	conn net.Conn,
	peer Peer,
	protected bool,
	admit, admitProtected func() (func(), error),
	releaseCapacity func(),
) error {
	defer conn.Close()
	stopContext := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopContext()
	codec := NewCodec(conn)
	codec.MaxFrame = s.maxFrame()
	if err := codec.SetDeadline(earlierDeadline(ctx, s.handshakeTimeout())); err != nil {
		return err
	}
	identity, generation, err := s.serverHandshake(codec)
	if err != nil {
		return err
	}
	if err := codec.ClearDeadline(); err != nil {
		return err
	}
	codec.WriteTimeout = s.writeTimeout()

	sessCtx, cancel := context.WithCancel(ctx)
	sess := &session{
		server:         s,
		conn:           conn,
		codec:          codec,
		ctx:            sessCtx,
		cancel:         cancel,
		peer:           peer,
		protected:      protected,
		wireBuild:      identity.WireBuild,
		generation:     generation,
		admit:          admit,
		admitProtected: admitProtected,
		outbound:       make(chan sessionOutbound, s.outboundQueue()),
		eventCredits:   newCreditWindow(),
		requestsDone:   make(chan struct{}),
		writerDone:     make(chan struct{}),
		disconnected:   make(chan struct{}),
		done:           make(chan struct{}),
		active:         make(map[uint64]*requestState),
		seen:           make(map[uint64]struct{}),
	}
	sess.accepted = &AcceptedSession{s: sess}
	s.addSession(sess)
	err = sess.run(sessCtx, releaseCapacity)
	s.removeSession(sess)
	close(sess.done)
	return err
}

func (s *Server) serverHandshake(codec *Codec) (handshakeIdentity, []byte, error) {
	frame, err := codec.ReadFrame()
	if err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: %w", ErrHandshake, err)
	}
	if frame.Kind != FrameHello || frame.ID != 0 || frame.Sequence != 0 || frame.Flags != FlagEnd || frame.Op != "" || frame.Tenant != "" {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: invalid hello frame", ErrHandshake)
	}
	var identity handshakeIdentity
	if err := decodeStrict(frame.Payload, &identity); err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: identity: %w", ErrHandshake, err)
	}
	if identity.Protocol != ProtocolVersion {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: identity got %d", ErrProtocolVersion, identity.Protocol)
	}
	if identity.WireBuild == "" {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: empty wire build", ErrHandshake)
	}
	if len(identity.Session) != 0 {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: client supplied a session generation", ErrHandshake)
	}
	generation := make([]byte, sessionGenerationBytes)
	if _, err := rand.Read(generation); err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: generate session: %w", ErrHandshake, err)
	}
	payload, err := json.Marshal(handshakeIdentity{Protocol: ProtocolVersion, WireBuild: s.WireBuild, Session: generation})
	if err != nil {
		return handshakeIdentity{}, nil, err
	}
	if err := codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload}); err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: acknowledge: %w", ErrHandshake, err)
	}
	return identity, generation, nil
}

func (s *Server) worker() {
	defer s.poolWG.Done()
	for j := range s.queue {
		val, err := s.invoke(j.ctx, j.req, j.h)
		<-s.slots
		j.done <- result{val: val, err: err}
	}
}

func (s *Server) dispatch(ctx context.Context, e entry, req Request) (any, error) {
	switch e.class {
	case classControl:
		return s.invoke(ctx, req, e.h)
	case classConcurrent:
		select {
		case s.slots <- struct{}{}:
		default:
			return nil, ErrQueueFull
		}
		j := job{ctx: ctx, req: req, h: e.h, done: make(chan result, 1)}
		select {
		case s.queue <- j:
		case <-ctx.Done():
			<-s.slots
			return nil, ctx.Err()
		}
		select {
		case r := <-j.done:
			return r.val, r.err
		case <-ctx.Done():
			r := <-j.done
			return r.val, r.err
		}
	default:
		return nil, fmt.Errorf("wire: invalid dispatch class %d", e.class)
	}
}

func (s *Server) invoke(ctx context.Context, req Request, h Handler) (any, error) {
	if s.onActivity != nil {
		s.onActivity()
	}
	return h(ctx, req)
}

func (s *Server) requestContext(parent context.Context, frame Frame) (context.Context, context.CancelFunc) {
	deadline := time.Time{}
	if frame.DeadlineUnixMilli > 0 {
		deadline = time.UnixMilli(frame.DeadlineUnixMilli)
	}
	if frame.Op != stopControlOp {
		server, _, ok := s.Ladder.Deadlines(frame.Op)
		if ok {
			candidate := time.Now().Add(server)
			if deadline.IsZero() || candidate.Before(deadline) {
				deadline = candidate
			}
		}
	}
	if !deadline.IsZero() {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithCancel(parent)
}

func (s *Server) lookup(op Op) (entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.handlers[op]
	return e, ok
}

func (s *Server) isDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
}

func (s *Server) publishReady() {
	s.mu.Lock()
	s.readyPublished = true
	s.mu.Unlock()
}

func (s *Server) addSession(sess *session) {
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) removeSession(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}

func (s *Server) closeSessions() {
	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.close()
	}
}

func (s *Server) verifyPeer(ctx context.Context, peer Peer) (bool, error) {
	if err := s.verifyOrdinaryPeer(ctx, peer); err != nil {
		return false, err
	}
	if s.protectedSessionClassifier == nil {
		return false, nil
	}
	return s.protectedSessionClassifier.Classify(ctx, peer)
}

func (s *Server) verifyOrdinaryPeer(ctx context.Context, peer Peer) error {
	if peer.UID != os.Geteuid() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, peer.UID, os.Geteuid())
	}
	if s.Trust != nil {
		if err := s.Trust(ctx, peer); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) maxFrame() int {
	if s.MaxFrame > 0 {
		return s.MaxFrame
	}
	return DefaultMaxFrame
}

func (s *Server) handshakeTimeout() time.Duration {
	if s.HandshakeTimeout > 0 {
		return s.HandshakeTimeout
	}
	return defaultHandshakeTimeout
}

func (s *Server) writeTimeout() time.Duration {
	if s.WriteTimeout > 0 {
		return s.WriteTimeout
	}
	return defaultWriteTimeout
}

func (s *Server) peerVerificationTimeout() time.Duration {
	if s.PeerVerificationTimeout > 0 {
		return s.PeerVerificationTimeout
	}
	return defaultPeerVerifyTimeout
}

func (s *Server) inboundQueue() int {
	if s.InboundQueue > 0 {
		return s.InboundQueue
	}
	return defaultInboundQueue
}

func (s *Server) outboundQueue() int {
	if s.OutboundQueue > 0 {
		return s.OutboundQueue
	}
	return defaultOutboundQueue
}

func (s *Server) streamQueue() int {
	if s.StreamQueue > 0 {
		return s.StreamQueue
	}
	return defaultStreamQueue
}

func (s *Server) maxSessions() int {
	if s.MaxSessions > 0 {
		return s.MaxSessions
	}
	return defaultMaxSessions
}

func (s *Server) validateSessionCapacity() error {
	maximum := s.maxSessions()
	switch {
	case s.PeerVerificationTimeout < 0:
		return errors.New("wire: peer verification timeout must not be negative")
	case s.reservedProtectedSessions < 0:
		return errors.New("wire: reserved protected sessions must not be negative")
	case s.reservedProtectedSessions > maximum:
		return fmt.Errorf("wire: reserved protected sessions %d exceed maximum sessions %d", s.reservedProtectedSessions, maximum)
	case s.reservedProtectedSessions != 0 && s.protectedSessionClassifier == nil:
		return errors.New("wire: protected session classifier is required when capacity is reserved")
	case s.protectedSessionClassifier != nil:
		if err := s.protectedSessionClassifier.Validate(); err != nil {
			return fmt.Errorf("wire: protected session classifier: %w", err)
		}
		return nil
	default:
		return nil
	}
}

func (s *Server) acquireObservation() (func(), bool) {
	select {
	case s.observationSlots <- struct{}{}:
		return func() { <-s.observationSlots }, true
	default:
		return nil, false
	}
}

func (s *Server) acquireSessionCapacity(protected bool) (sessionCapacity, bool) {
	if protected {
		select {
		case s.protectedSessionSlots <- struct{}{}:
			return protectedSessionCapacity, true
		default:
		}
	}
	select {
	case s.ordinarySessionSlots <- struct{}{}:
		return ordinarySessionCapacity, true
	default:
		return 0, false
	}
}

func (s *Server) releaseSessionCapacity(capacity sessionCapacity) {
	switch capacity {
	case ordinarySessionCapacity:
		<-s.ordinarySessionSlots
	case protectedSessionCapacity:
		<-s.protectedSessionSlots
	default:
		panic("wire: invalid session capacity release")
	}
}

func earlierDeadline(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

func decodeStrict(payload []byte, dst any) error {
	dec := json.NewDecoder(bytesReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

func bytesReader(payload []byte) *sliceReader { return &sliceReader{payload: payload} }

type sliceReader struct{ payload []byte }

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.payload) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.payload)
	r.payload = r.payload[n:]
	return n, nil
}

func isDisconnect(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled)
}
