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
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
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
	// ErrPermissionDenied means an authenticated role lacks authority for a
	// private daemonkit control operation.
	ErrPermissionDenied = errors.New("wire: control permission denied")
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
	stopControlOp:               {},
	runtimeReadinessSubscribeOp: {},
	runtimeReceiptOp:            {},
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
	routeLifecycle
	routeStop
)

type daemonAdmission func() (daemon.Publication, func(), error)

type entry struct {
	class class
	route routeClass
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

type readinessSubscription struct {
	done      chan struct{}
	start     chan struct{}
	startOnce sync.Once
}

type sessionCapacity struct {
	role      trust.PeerRole
	protected bool
}

// Server serves persistent, multiplexed v1 sessions on a listener owned by its caller.
// Register business, observation, and stop-control handlers before Serve.
type Server struct {
	// WireBuild is the stable schema identity sent during the mandatory handshake.
	WireBuild string
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

	mu                           sync.Mutex
	handlers                     map[Op]entry
	listener                     net.Listener
	started                      bool
	sessions                     map[*session]struct{}
	streamWindow                 uint32
	trustPolicy                  trust.TrustPolicy
	trustWorkers                 *worker.RuntimeClaim
	trustExecutable              string
	stopControlStore             *proc.FileStore
	stopTargetProcessGeneration  string
	hasObservations              bool
	lifecycle                    *daemon.Lifecycle
	runtimeBuild                 string
	processGeneration            string
	readinessMu                  sync.Mutex
	readinessSubscribers         map[*session]*readinessSubscription
	readinessSubscriptionsClosed bool
	staticOrdinary               bool

	queue                chan job
	slots                chan struct{}
	observationSlots     chan struct{}
	ordinarySessionSlots chan struct{}
	protectedRoleSlots   map[trust.PeerRole]chan struct{}
	poolWG               sync.WaitGroup
	sessionWG            sync.WaitGroup
	closeOnce            sync.Once
}

// HandlerSpec defines one Ready-only product handler.
type HandlerSpec struct {
	Op         Op
	Handler    Handler
	Concurrent bool
}

// Register registers one Ready-only product handler.
func (s *Server) Register(spec HandlerSpec) {
	class := classControl
	if spec.Concurrent {
		class = classConcurrent
	}
	s.register(spec.Op, class, routeBusiness, spec.Handler)
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

// Serve accepts v1 sessions until ctx is cancelled. admit runs for every
// request that clears the pre-admission checks; its done runs when the
// request's execution settles, including cancellation and write-failure paths.
// Returning from Serve is process-terminal.
func (s *Server) serveRuntime(
	ctx context.Context,
	listener net.Listener,
	lifecycle *daemon.Lifecycle,
	trustWorkers *worker.RuntimeClaim,
	admit, admitProtected daemonAdmission,
	peerFence runtimeauth.PeerFence,
	serverExit runtimeauth.ServerExit,
	started chan<- error,
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
	if lifecycle == nil {
		return errors.New("wire: lifecycle controller is required")
	}
	if trustWorkers == nil {
		return errors.New("wire: trust worker pool is required")
	}
	if peerFence == nil {
		return errors.New("wire: child peer-fence authority is required")
	}
	if serverExit == nil {
		return errors.New("wire: server exit authority is required")
	}
	s.mu.Lock()
	if s.trustWorkers != nil && s.trustWorkers != trustWorkers {
		s.mu.Unlock()
		return errors.New("wire: trust worker pool differs from bound runtime")
	}
	s.trustWorkers = trustWorkers
	s.mu.Unlock()
	if s.lifecycle == nil {
		s.lifecycle = lifecycle
	} else if s.lifecycle != lifecycle {
		return errors.New("wire: lifecycle controller differs from bound runtime")
	}
	workers, err := s.start(listener)
	if err != nil {
		if started != nil {
			started <- err
		}
		return err
	}
	s.startWorkers(workers)
	acceptDone := make(chan error, 1)
	go func() { acceptDone <- s.accept(ctx, admit, admitProtected, peerFence) }()
	if started != nil {
		started <- nil
	}
	var acceptErr error
	select {
	case <-ctx.Done():
		_ = s.CloseIntake()
		acceptErr = <-acceptDone
	case acceptErr = <-acceptDone:
		_ = s.CloseIntake()
	}
	acceptErr = serverExit(wrapAcceptError(acceptErr))

	s.settleReadiness()
	s.closeSessions()
	s.sessionWG.Wait()
	s.stopWorkers()
	return acceptErr
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
	protectedRoles := s.protectedRoles()
	s.ordinarySessionSlots = make(chan struct{}, s.maxSessions()-len(protectedRoles))
	s.protectedRoleSlots = make(map[trust.PeerRole]chan struct{}, len(protectedRoles))
	for role := range protectedRoles {
		s.protectedRoleSlots[role] = make(chan struct{}, 1)
	}
	if s.hasObservations {
		s.observationSlots = make(chan struct{}, 1)
	}
	s.mu.Unlock()
	return workers, nil
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
	listener := s.listener
	s.mu.Unlock()
	s.readinessMu.Lock()
	s.readinessSubscriptionsClosed = true
	s.readinessMu.Unlock()
	var err error
	if listener != nil {
		s.closeOnce.Do(func() { err = listener.Close() })
	}
	return err
}

func (s *Server) accept(ctx context.Context, admit, admitProtected daemonAdmission, peerFence runtimeauth.PeerFence) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		unix, ok := conn.(*net.UnixConn)
		if !ok {
			s.rejectHandshake(ctx, conn, ResponseCodePeerUntrusted, ErrUntrustedPeer)
			continue
		}
		peer, err := PeerFromConn(unix)
		if err != nil {
			s.rejectHandshake(ctx, conn, ResponseCodePeerUntrusted, ErrUntrustedPeer)
			s.Log.Debug("wire: reject unidentified peer", "err", err)
			continue
		}
		verifyCtx, cancelVerify := context.WithTimeout(ctx, s.peerVerificationTimeout())
		role, protected, err := s.verifyPeer(verifyCtx, peer)
		if err != nil {
			cancelVerify()
			s.rejectHandshake(ctx, conn, ResponseCodePeerUntrusted, ErrUntrustedPeer)
			s.Log.Debug("wire: reject untrusted peer", "err", err)
			continue
		}
		fencePermit, err := peerFence(verifyCtx, peer)
		cancelVerify()
		if err != nil {
			s.rejectHandshake(ctx, conn, ResponseCodePeerUntrusted, ErrUntrustedPeer)
			s.Log.Debug("wire: reject child peer fence", "err", err)
			continue
		}
		capacity, ok := s.acquireSessionCapacity(role, protected)
		if !ok {
			if fencePermit != nil {
				fencePermit.Rollback()
			}
			s.rejectHandshake(ctx, conn, ResponseCodeSessionCapacity, ErrSessionCapacity)
			continue
		}
		s.sessionWG.Add(1)
		go func(conn net.Conn, peer Peer, role trust.PeerRole, protected bool, capacity sessionCapacity, fencePermit *runtimeauth.PeerFencePermit) {
			releaseCapacity := sync.OnceFunc(func() { s.releaseSessionCapacity(capacity) })
			defer func() {
				releaseCapacity()
				s.sessionWG.Done()
			}()
			if err := s.serveConn(ctx, conn, peer, role, protected, admit, admitProtected, releaseCapacity, fencePermit); err != nil && !isDisconnect(err) {
				s.Log.Debug("wire: session ended", "err", err)
			}
		}(conn, peer, role, protected, capacity, fencePermit)
	}
}

func (s *Server) serveConn(
	ctx context.Context,
	conn net.Conn,
	peer Peer,
	role trust.PeerRole,
	protected bool,
	admit, admitProtected daemonAdmission,
	releaseCapacity func(),
	fencePermit *runtimeauth.PeerFencePermit,
) error {
	if fencePermit != nil {
		defer fencePermit.Rollback()
	}
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
	var settleFence func()
	if fencePermit != nil {
		settleFence, err = fencePermit.Commit()
		if err != nil {
			return err
		}
		defer settleFence()
	}

	sessCtx, cancel := context.WithCancel(ctx)
	sess := &session{
		server:         s,
		conn:           conn,
		codec:          codec,
		ctx:            sessCtx,
		cancel:         cancel,
		peer:           peer,
		role:           role,
		protected:      protected,
		wireBuild:      identity.WireBuild,
		generation:     generation,
		admit:          admit,
		admitProtected: admitProtected,
		outbound:       make(chan sessionOutbound, s.outboundQueue()),
		eventCredits:   newCreditWindow(),
		lifecycleLane:  newLatestWriteLane(),
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
	if identity.WireBuild != s.WireBuild {
		if err := s.writeHandshakeAck(codec, handshakeAck{
			Protocol: ProtocolVersion, WireBuild: s.WireBuild, Rejected: true,
			Code: ResponseCodeBuildMismatch, Reason: ErrBuildMismatch.Error(),
		}); err != nil {
			return handshakeIdentity{}, nil, fmt.Errorf("%w: reject build: %w", ErrHandshake, err)
		}
		return handshakeIdentity{}, nil, ErrBuildMismatch
	}
	generation := make([]byte, sessionGenerationBytes)
	if _, err := rand.Read(generation); err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: generate session: %w", ErrHandshake, err)
	}
	if err := s.writeHandshakeAck(codec, handshakeAck{
		Protocol: ProtocolVersion, WireBuild: s.WireBuild, Session: generation,
	}); err != nil {
		return handshakeIdentity{}, nil, fmt.Errorf("%w: acknowledge: %w", ErrHandshake, err)
	}
	return identity, generation, nil
}

func (s *Server) rejectHandshake(ctx context.Context, conn net.Conn, code ResponseCode, cause error) {
	defer conn.Close()
	codec := NewCodec(conn)
	codec.MaxFrame = s.maxFrame()
	if err := codec.SetDeadline(earlierDeadline(ctx, s.handshakeTimeout())); err != nil {
		return
	}
	_ = s.writeHandshakeAck(codec, handshakeAck{
		Protocol: ProtocolVersion, WireBuild: s.WireBuild, Rejected: true,
		Code: code, Reason: cause.Error(),
	})
}

func (s *Server) writeHandshakeAck(codec *Codec, ack handshakeAck) error {
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload})
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

func (s *Server) addSession(sess *session) {
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) removeSession(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
	s.readinessMu.Lock()
	delete(s.readinessSubscribers, sess)
	s.readinessMu.Unlock()
}

//nolint:contextcheck // Subscription pumps are session-owned after the request returns.
func (s *Server) subscribeReadiness(sess *session) error {
	s.readinessMu.Lock()
	if s.readinessSubscriptionsClosed {
		s.readinessMu.Unlock()
		return ErrDraining
	}
	if s.readinessSubscribers == nil {
		s.readinessSubscribers = make(map[*session]*readinessSubscription)
	}
	if _, ok := s.readinessSubscribers[sess]; ok {
		s.readinessMu.Unlock()
		return errors.New("wire: readiness session is already subscribed")
	}
	subscription := &readinessSubscription{done: make(chan struct{}), start: make(chan struct{})}
	s.readinessSubscribers[sess] = subscription
	s.readinessMu.Unlock()
	go s.awaitReadinessPump(sess, subscription)
	return nil
}

func (s *Server) startReadiness(sess *session) {
	s.readinessMu.Lock()
	subscription := s.readinessSubscribers[sess]
	s.readinessMu.Unlock()
	if subscription != nil {
		subscription.startOnce.Do(func() { close(subscription.start) })
	}
}

func (s *Server) awaitReadinessPump(sess *session, subscription *readinessSubscription) {
	defer close(subscription.done)
	select {
	case <-sess.ctx.Done():
		return
	case <-subscription.start:
	}
	s.pumpReadiness(sess)
}

func (s *Server) pumpReadiness(sess *session) {
	sequence := uint64(0)
	for {
		progress := s.lifecycle.Snapshot()
		if progress.Sequence > sequence {
			sequence = progress.Sequence
			payload, err := json.Marshal(runtimeReadinessEvent{
				Protocol: ProtocolVersion, WireBuild: s.WireBuild,
				RuntimeIdentity: RuntimeIdentity{
					RuntimeBuild: s.runtimeBuild, ProcessGeneration: s.processGeneration,
				},
				Progress: progress,
			})
			if err != nil {
				sess.close()
				return
			}
			terminal := progress.State == RuntimeFailed || progress.State == RuntimeDraining
			receipt, err := sess.offerLifecycle(payload, terminal)
			if err != nil {
				sess.close()
				return
			}
			if terminal {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(sess.ctx), s.writeTimeout())
				err = receipt.wait(ctx)
				cancel()
				if err != nil {
					sess.close()
				}
				return
			}
		}
		if _, err := s.lifecycle.WaitChange(sess.ctx, sequence); err != nil {
			return
		}
	}
}

func (s *Server) waitReadinessTerminal(ctx context.Context) error {
	s.readinessMu.Lock()
	done := make([]<-chan struct{}, 0, len(s.readinessSubscribers))
	for _, subscription := range s.readinessSubscribers {
		done = append(done, subscription.done)
	}
	s.readinessMu.Unlock()
	for _, settled := range done {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-settled:
		}
	}
	return nil
}

//nolint:contextcheck // Terminal publication settlement uses its own bounded write context.
func (s *Server) settleReadiness() {
	if s.lifecycle == nil {
		return
	}
	state := s.lifecycle.Snapshot().State
	if state != RuntimeFailed && state != RuntimeDraining {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.writeTimeout())
	defer cancel()
	_ = s.waitReadinessTerminal(ctx)
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

func (s *Server) verifyPeer(ctx context.Context, peer Peer) (trust.PeerRole, bool, error) {
	if err := s.verifyOrdinaryPeer(ctx, peer); err != nil {
		return "", false, err
	}
	var matched trust.PeerRole
	for _, role := range s.trustPolicy.RoleNames() {
		requirement, ok := s.trustPolicy.Requirement(role)
		if !ok {
			return "", false, errors.New("wire: compiled trust role is absent")
		}
		verifier := trust.ProcessVerifier{
			Runner: s.trustWorkers, Executable: s.trustExecutable,
			Policy: trust.Policy{Requirement: &requirement},
		}
		if err := verifier.Check(ctx, peer); err == nil {
			if matched != "" {
				return "", false, trust.ErrAmbiguousRole
			}
			matched = role
		} else if !errors.Is(err, trust.ErrUntrustedPeer) {
			return "", false, err
		}
	}
	protected := s.trustPolicy.AllowsStop(matched) || s.trustPolicy.AllowsReceipt(matched) ||
		s.trustPolicy.AllowsReadiness(matched) || s.trustPolicy.AllowsHandoff(matched)
	return matched, protected, nil
}

func (s *Server) verifyOrdinaryPeer(ctx context.Context, peer Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if peer.UID != s.trustPolicy.ExpectedUID() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, peer.UID, s.trustPolicy.ExpectedUID())
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
	protected := len(s.protectedRoles())
	switch {
	case s.PeerVerificationTimeout < 0:
		return errors.New("wire: peer verification timeout must not be negative")
	case protected > maximum:
		return fmt.Errorf("wire: protected role capacity %d exceeds maximum sessions %d", protected, maximum)
	default:
		return nil
	}
}

func (s *Server) protectedRoles() map[trust.PeerRole]struct{} {
	roles := make(map[trust.PeerRole]struct{})
	for _, role := range s.trustPolicy.RoleNames() {
		if s.trustPolicy.AllowsStop(role) || s.trustPolicy.AllowsReceipt(role) ||
			s.trustPolicy.AllowsReadiness(role) || s.trustPolicy.AllowsHandoff(role) {
			roles[role] = struct{}{}
		}
	}
	return roles
}

func (s *Server) acquireObservation() (func(), bool) {
	select {
	case s.observationSlots <- struct{}{}:
		return func() { <-s.observationSlots }, true
	default:
		return nil, false
	}
}

func (s *Server) acquireSessionCapacity(role trust.PeerRole, protected bool) (sessionCapacity, bool) {
	if protected {
		bucket := s.protectedRoleSlots[role]
		if bucket == nil {
			return sessionCapacity{}, false
		}
		select {
		case bucket <- struct{}{}:
			return sessionCapacity{role: role, protected: true}, true
		default:
			return sessionCapacity{}, false
		}
	}
	select {
	case s.ordinarySessionSlots <- struct{}{}:
		return sessionCapacity{}, true
	default:
		return sessionCapacity{}, false
	}
}

func (s *Server) releaseSessionCapacity(capacity sessionCapacity) {
	if capacity.protected {
		bucket := s.protectedRoleSlots[capacity.role]
		if bucket == nil {
			panic("wire: invalid protected session capacity role")
		}
		<-bucket
		return
	}
	<-s.ordinarySessionSlots
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
