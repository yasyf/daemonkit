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
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
)

const (
	defaultConcurrency          = 8
	defaultHandshakeTimeout     = 10 * time.Second
	defaultHandshakeReadTimeout = 2 * time.Second
	defaultWriteTimeout         = 10 * time.Second
)

// Trust holds the per-lane requirements. The same-EUID floor is unconditional
// and not expressible here; a nil requirement is explicit UID-only trust.
type Trust struct {
	Control  *trust.Requirement
	Business *trust.Requirement
}

// Config configures one Server. Every queue and cap derives from Concurrency.
type Config struct {
	// Schemas is required; index 0 is this build's own digest.
	Schemas Schemas
	// Trust holds the per-lane requirements over the unconditional EUID floor.
	Trust Trust
	// Concurrency bounds business sessions and in-flight Handles; 8 when zero.
	Concurrency int
	// MaxFrame caps each encoded frame; DefaultMaxFrame when zero.
	MaxFrame int
	// Handshake bounds the whole admission of one connection; 10s when zero.
	Handshake time.Duration
	// HandshakeRead bounds the pre-verification hello read alone, so a
	// partial-hello slow-loris releases its pending slot fast; 2s when zero.
	HandshakeRead time.Duration
	// Write bounds each frame write; 10s when zero.
	Write time.Duration
	// Log receives accept and session diagnostics.
	Log *slog.Logger
}

// Server serves persistent multiplexed sessions for one Runtime. Its exported
// surface beyond Serve is the drain triad and AdoptHandoff.
type Server struct {
	rt  Runtime
	cfg Config
	log *slog.Logger

	concurrency   int
	pendingSlots  chan struct{}
	controlSlot   chan struct{}
	businessSlots chan struct{}
	handleSem     chan struct{}

	mu           sync.Mutex
	serveCtx     context.Context
	listener     net.Listener
	started      bool
	intakeClosed bool
	sessions     map[*session]struct{}

	pendingWG sync.WaitGroup
	sessionWG sync.WaitGroup
	closeOnce sync.Once
}

// NewServer validates cfg and returns an unstarted server for rt.
func NewServer(rt Runtime, cfg Config) (*Server, error) {
	if rt == nil {
		return nil, errors.New("wire: runtime is required")
	}
	if len(cfg.Schemas) == 0 || cfg.Schemas.Own() == "" {
		return nil, errors.New("wire: Schemas is required")
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		rt:            rt,
		cfg:           cfg,
		log:           log,
		concurrency:   concurrency,
		pendingSlots:  make(chan struct{}, 2*concurrency+2),
		controlSlot:   make(chan struct{}, 1),
		businessSlots: make(chan struct{}, concurrency),
		handleSem:     make(chan struct{}, concurrency),
		sessions:      make(map[*session]struct{}),
	}, nil
}

// Serve accepts sessions on ln until ctx ends. After CloseIntake it keeps
// accepted sessions alive until ctx ends so admitted work can settle.
// Returning from Serve is process-terminal.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if ln == nil {
		return errors.New("wire: listener is required")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return ErrServerStarted
	}
	s.started = true
	s.listener = ln
	s.serveCtx = ctx
	s.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = s.CloseIntake() })
	defer stop()
	err := wrapAcceptError(s.accept(ctx))
	if err == nil && ctx.Err() == nil {
		s.mu.Lock()
		closed := s.intakeClosed
		s.mu.Unlock()
		if closed {
			<-ctx.Done()
		}
	}
	_ = s.CloseIntake()
	s.closeSessions()
	s.pendingWG.Wait()
	s.sessionWG.Wait()
	return err
}

func (s *Server) accept(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		unixConn, ok := conn.(*net.UnixConn)
		if !ok {
			_ = conn.Close()
			continue
		}
		select {
		case s.pendingSlots <- struct{}{}:
		default:
			_ = unixConn.Close()
			continue
		}
		s.pendingWG.Add(1)
		go func() {
			defer func() { <-s.pendingSlots; s.pendingWG.Done() }()
			if err := s.startConnection(ctx, unixConn); err != nil {
				s.log.Debug("wire: reject connection", "err", err)
			}
		}()
	}
}

// AdoptHandoff admits a connected descriptor received over the broker-handoff
// verb, taking ownership of conn. The adopted peer is unverified like any
// other: it draws a pending slot and walks the same gate stack as an accepted
// connection.
func (s *Server) AdoptHandoff(conn *net.UnixConn) error {
	s.mu.Lock()
	ctx := s.serveCtx
	closed := s.intakeClosed || ctx == nil
	s.mu.Unlock()
	if closed || ctx.Err() != nil {
		_ = conn.Close()
		return ErrDraining
	}
	select {
	case s.pendingSlots <- struct{}{}:
	default:
		_ = conn.Close()
		return ErrSessionCapacity
	}
	s.pendingWG.Add(1)
	defer func() { <-s.pendingSlots; s.pendingWG.Done() }()
	return s.startConnection(ctx, conn)
}

// startConnection walks the admission gate stack in order: hello read under
// the short pre-verification deadline, drain preamble, trust, schema, lane
// capacity — capacity strictly after verification, so nothing an unverified
// peer holds is lane state — then the phase-carrying ack.
func (s *Server) startConnection(ctx context.Context, conn *net.UnixConn) error {
	start := time.Now()
	codec := NewCodec(conn)
	codec.MaxFrame = s.maxFrame()
	if err := codec.SetDeadline(earlierDeadline(ctx, start.Add(s.handshakeReadTimeout()))); err != nil {
		_ = conn.Close()
		return err
	}
	hello, err := readClientHello(codec)
	if err != nil {
		s.rejectHandshake(conn, codec, ResponseCodePeerUntrusted, err)
		return err
	}
	if hello.Lane == LaneBusiness && len(hello.Nonce) != 0 {
		err := fmt.Errorf("%w: nonce outside a spawned session", ErrHandshake)
		s.rejectHandshake(conn, codec, ResponseCodePeerUntrusted, err)
		return err
	}
	if err := codec.SetDeadline(earlierDeadline(ctx, start.Add(s.handshakeTimeout()))); err != nil {
		_ = conn.Close()
		return err
	}
	if s.draining() {
		_, _ = conn.Write(drainPreamble[:])
		_ = conn.Close()
		return ErrDraining
	}
	peer, err := trust.PeerCredentials(conn)
	if err != nil {
		s.rejectHandshake(conn, codec, ResponseCodePeerUntrusted, ErrUntrustedPeer)
		return fmt.Errorf("wire: identify peer: %w", err)
	}
	requirement := s.cfg.Trust.Business
	if hello.Lane == LaneControl {
		requirement = s.cfg.Trust.Control
	}
	if err := trust.Verify(peer, requirement); err != nil {
		s.rejectHandshake(conn, codec, ResponseCodePeerUntrusted, ErrUntrustedPeer)
		err = fmt.Errorf("wire: verify peer: %w", err)
		// The peer sees only PeerUntrusted; an infrastructure failure is not a
		// policy denial and must be loud on the daemon side. A peer that exited
		// before verification completed is an expected per-connection outcome,
		// not infrastructure — it stays on the quiet per-connection debug path.
		if !errors.Is(err, trust.ErrUntrustedPeer) && !errors.Is(err, trust.ErrPeerGone) {
			s.log.Error("wire: peer verification infrastructure failure", "err", err)
		}
		return err
	}
	if hello.Lane == LaneBusiness && !s.cfg.Schemas.Accepts(hello.Schema) {
		s.rejectHandshake(conn, codec, ResponseCodeBuildMismatch, ErrBuildMismatch)
		return ErrBuildMismatch
	}
	s.mu.Lock()
	if s.intakeClosed {
		s.mu.Unlock()
		_, _ = conn.Write(drainPreamble[:])
		_ = conn.Close()
		return ErrDraining
	}
	release, ok := s.acquireLaneSlot(hello.Lane)
	if ok {
		s.sessionWG.Add(1)
	}
	s.mu.Unlock()
	if !ok {
		s.rejectHandshake(conn, codec, ResponseCodeSessionCapacity, ErrSessionCapacity)
		return ErrSessionCapacity
	}
	generation, err := s.completeHandshake(codec)
	if err != nil {
		release()
		s.sessionWG.Done()
		_ = conn.Close()
		return err
	}
	if err := codec.ClearDeadline(); err != nil {
		release()
		s.sessionWG.Done()
		_ = conn.Close()
		return err
	}
	codec.WriteTimeout = s.writeTimeout()
	go s.serveSession(ctx, conn, codec, hello, peer, generation, release)
	return nil
}

func (s *Server) serveSession(
	ctx context.Context,
	conn net.Conn,
	codec *Codec,
	hello helloIdentity,
	peer trust.Peer,
	generation []byte,
	release func(),
) {
	releaseOnce := sync.OnceFunc(release)
	defer func() {
		releaseOnce()
		s.sessionWG.Done()
	}()
	err := s.runSession(ctx, conn, codec, hello.Lane, hello.Schema, peer, generation)
	if err != nil && !isDisconnect(err) {
		s.log.Debug("wire: session ended", "err", err)
	}
}

func (s *Server) runSession(
	ctx context.Context,
	conn net.Conn,
	codec *Codec,
	lane Lane,
	schema string,
	peer trust.Peer,
	generation []byte,
) error {
	stopContext := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopContext()
	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess := &session{
		server:       s,
		conn:         conn,
		codec:        codec,
		ctx:          sessCtx,
		cancel:       cancel,
		peer:         peer,
		lane:         lane,
		schema:       schema,
		generation:   generation,
		outbound:     make(chan sessionOutbound, 4*s.concurrency),
		requestsDone: make(chan struct{}),
		writerDone:   make(chan struct{}),
		disconnected: make(chan struct{}),
		done:         make(chan struct{}),
		active:       make(map[uint64]*requestState),
		seen:         make(map[uint64]struct{}),
	}
	sess.accepted = &AcceptedSession{s: sess}
	s.addSession(sess)
	go s.pumpPhase(sess)
	err := sess.run(sessCtx)
	s.removeSession(sess)
	close(sess.done)
	return err
}

// pumpPhase pushes a lifecycle snapshot to one session on establishment and
// on every phase change, riding Runtime.WaitPhase until the session ends.
func (s *Server) pumpPhase(sess *session) {
	snapshot := s.rt.Phase()
	for {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			sess.close()
			return
		}
		if err := sess.enqueue(sess.ctx, Frame{Kind: FrameLifecycle, Flags: FlagEnd, Payload: payload}); err != nil {
			return
		}
		snapshot, err = s.rt.WaitPhase(sess.ctx, snapshot.Sequence)
		if err != nil {
			return
		}
	}
}

// CloseIntake prevents new connections and typed-rejects new business
// requests. Accepted sessions stay alive so admitted work and control can
// settle.
func (s *Server) CloseIntake() error {
	s.mu.Lock()
	listener := s.listener
	s.intakeClosed = true
	s.mu.Unlock()
	var err error
	if listener != nil {
		s.closeOnce.Do(func() { err = listener.Close() })
	}
	return err
}

// CancelRequests cancels every in-flight Handle context.
func (s *Server) CancelRequests() {
	for _, sess := range s.snapshotSessions() {
		sess.cancelRequests()
	}
}

// Settle waits until every terminal response already written has been
// acknowledged by its peer, or ctx ends.
func (s *Server) Settle(ctx context.Context) error {
	for _, sess := range s.snapshotSessions() {
		if err := sess.settleTerminalRequests(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) draining() bool {
	s.mu.Lock()
	closed := s.intakeClosed
	s.mu.Unlock()
	return closed || s.rt.Phase().Phase == PhaseDraining
}

func (s *Server) gatePhase() error {
	s.mu.Lock()
	closed := s.intakeClosed
	s.mu.Unlock()
	if closed {
		return ErrDraining
	}
	switch s.rt.Phase().Phase {
	case PhaseReady:
		return nil
	case PhaseStarting:
		return ErrNotReady
	default:
		return ErrDraining
	}
}

func (s *Server) executeDrain(ctx context.Context) (any, error) {
	s.rt.Drain()
	snapshot := s.rt.Phase()
	for snapshot.Phase != PhaseDraining && snapshot.Phase != PhaseFailed {
		var err error
		snapshot, err = s.rt.WaitPhase(ctx, snapshot.Sequence)
		if err != nil {
			return nil, err
		}
	}
	return json.RawMessage("{}"), nil
}

func (s *Server) acquireLaneSlot(lane Lane) (func(), bool) {
	slots := s.businessSlots
	if lane == LaneControl {
		slots = s.controlSlot
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return nil, false
	}
}

func (s *Server) completeHandshake(codec *Codec) ([]byte, error) {
	generation := make([]byte, sessionGenerationBytes)
	if _, err := rand.Read(generation); err != nil {
		return nil, fmt.Errorf("%w: generate session: %w", ErrHandshake, err)
	}
	if err := s.writeAck(codec, helloAck{
		Protocol: ProtocolVersion, Schema: s.cfg.Schemas.Own(),
		Session: generation, Phase: s.rt.Phase().Phase,
	}); err != nil {
		return nil, fmt.Errorf("%w: acknowledge: %w", ErrHandshake, err)
	}
	return generation, nil
}

func (s *Server) rejectHandshake(conn net.Conn, codec *Codec, code ResponseCode, cause error) {
	defer conn.Close()
	_ = s.writeAck(codec, helloAck{
		Protocol: ProtocolVersion, Schema: s.cfg.Schemas.Own(), Phase: s.rt.Phase().Phase,
		Rejected: true, Code: code, Reason: cause.Error(),
	})
}

func (s *Server) writeAck(codec *Codec, ack helloAck) error {
	payload, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload})
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

func (s *Server) snapshotSessions() []*session {
	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	return sessions
}

func (s *Server) closeSessions() {
	for _, sess := range s.snapshotSessions() {
		sess.close()
	}
}

func (s *Server) maxFrame() int {
	if s.cfg.MaxFrame > 0 {
		return s.cfg.MaxFrame
	}
	return DefaultMaxFrame
}

func (s *Server) handshakeTimeout() time.Duration {
	return durationOr(s.cfg.Handshake, defaultHandshakeTimeout)
}

func (s *Server) handshakeReadTimeout() time.Duration {
	return durationOr(s.cfg.HandshakeRead, defaultHandshakeReadTimeout)
}

func (s *Server) writeTimeout() time.Duration {
	return durationOr(s.cfg.Write, defaultWriteTimeout)
}

func (s *Server) inboundQueue() int { return 2 * s.concurrency }

func wrapAcceptError(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return fmt.Errorf("wire: accept: %w", err)
}

func earlierDeadline(ctx context.Context, deadline time.Time) time.Time {
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func isDisconnect(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled)
}
