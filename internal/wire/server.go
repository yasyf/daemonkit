package wire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

const (
	defaultConcurrency          = 8
	defaultHandshakeTimeout     = 10 * time.Second
	defaultHandshakeReadTimeout = 2 * time.Second
	defaultWriteTimeout         = 10 * time.Second
)

// Config configures one Server. Every queue and cap derives from Concurrency.
type Config struct {
	// Schemas is required; index 0 is this build's own digest.
	Schemas Schemas
	// Concurrency bounds sessions and in-flight Handles; 8 when zero.
	Concurrency int
	// MaxFrame caps each encoded frame; DefaultMaxFrame when zero, and at
	// most maxFrameCeiling so no legal length prefix reads as the drain
	// preamble.
	MaxFrame int
	// Handshake bounds the whole admission of one connection; 10s when zero.
	Handshake time.Duration
	// HandshakeRead bounds the hello read alone, so a partial-hello slow-loris
	// releases its pending slot fast; 2s when zero.
	HandshakeRead time.Duration
	// Write bounds each frame write; 10s when zero.
	Write time.Duration
	// Idle reclaims a session's slot after this long with no frame and
	// no request in flight; never when zero. A quiet peer whose socket the
	// kernel never tears down holds its slot until the daemon restarts.
	Idle time.Duration
	// Serving is the process identity the daemon.health verb reports.
	Serving Serving
	// Log receives accept and session diagnostics.
	Log *slog.Logger
}

// Server serves persistent multiplexed sessions for one Runtime. Its exported
// surface beyond Serve is the drain triad and AdoptHandoff.
type Server struct {
	rt  Runtime
	cfg Config
	log *slog.Logger

	concurrency  int
	pendingSlots chan struct{}
	sessionSlots chan struct{}
	handleSem    chan struct{}

	admitted    atomic.Int64
	lastSession atomic.Uint64

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
	if cfg.MaxFrame > maxFrameCeiling {
		return nil, fmt.Errorf("wire: MaxFrame %d admits a frame whose length prefix opens with the drain preamble; the cap stays at or below %#x", cfg.MaxFrame, maxFrameCeiling)
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
		rt:           rt,
		cfg:          cfg,
		log:          log,
		concurrency:  concurrency,
		pendingSlots: make(chan struct{}, 2*concurrency+2),
		sessionSlots: make(chan struct{}, concurrency),
		handleSem:    make(chan struct{}, concurrency),
		sessions:     make(map[*session]struct{}),
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
// verb, taking ownership of conn. It draws a pending slot and walks the same
// admission as an accepted connection.
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

// AdoptMinted admits a parent-minted socketpair end as one session whose peer
// is the spawned child: a socketpair's kernel peer names its creator — this
// daemon — so the socket read that identifies an accepted peer would lie
// here. The hello must echo the per-mint nonce.
func (s *Server) AdoptMinted(conn *net.UnixConn, peer proc.Peer, nonce []byte) error {
	if len(nonce) != spawnedNonceBytes {
		_ = conn.Close()
		return fmt.Errorf("wire: minted nonce must be %d bytes", spawnedNonceBytes)
	}
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
	return s.startMinted(ctx, conn, peer, nonce)
}

// startMinted is AdoptMinted's gate walk. It refuses the drain preamble
// outright — a spawned peer holds no drain authority — and takes the pinned
// peer where startConnection reads the socket.
func (s *Server) startMinted(ctx context.Context, conn *net.UnixConn, peer proc.Peer, nonce []byte) error {
	start := time.Now()
	codec := NewCodec(conn)
	codec.MaxFrame = s.maxFrame()
	if err := codec.SetDeadline(earlierDeadline(ctx, start.Add(s.handshakeReadTimeout()))); err != nil {
		_ = conn.Close()
		return err
	}
	drain, err := codec.PeekPreamble()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if drain {
		_ = conn.Close()
		return fmt.Errorf("%w: drain preamble on a minted session", ErrHandshake)
	}
	hello, err := readClientHello(codec)
	if err != nil {
		s.rejectHandshake(conn, codec, ResponseCodeHandshake, err)
		return err
	}
	if subtle.ConstantTimeCompare(hello.Nonce, nonce) != 1 {
		err := fmt.Errorf("%w: minted session nonce", ErrHandshake)
		s.rejectHandshake(conn, codec, ResponseCodeHandshake, err)
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
	return s.admitSession(ctx, conn, codec, hello, peer)
}

// startConnection walks admission in order: the inbound drain preamble peek,
// hello read under the short deadline, the outbound drain preamble, session
// capacity, then the phase-carrying ack. The schema gates product dispatch,
// never admission, so a successor of another era still reads and drains this
// daemon.
func (s *Server) startConnection(ctx context.Context, conn *net.UnixConn) error {
	start := time.Now()
	codec := NewCodec(conn)
	codec.MaxFrame = s.maxFrame()
	codec.admitsLegacy = true
	if err := codec.SetDeadline(earlierDeadline(ctx, start.Add(s.handshakeReadTimeout()))); err != nil {
		_ = conn.Close()
		return err
	}
	drain, err := codec.PeekPreamble()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if drain {
		return s.admitPreambleDrain(ctx, conn, start)
	}
	hello, err := readClientHello(codec)
	if err != nil {
		s.rejectHandshake(conn, codec, ResponseCodeHandshake, err)
		return err
	}
	if len(hello.Nonce) != 0 {
		err := fmt.Errorf("%w: nonce outside a spawned session", ErrHandshake)
		s.rejectHandshake(conn, codec, ResponseCodeHandshake, err)
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
	peer, err := proc.PeerCredentials(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("wire: identify peer: %w", err)
	}
	return s.admitSession(ctx, conn, codec, hello, peer)
}

// admitSession is the tail both admission paths share: session capacity,
// then the phase-carrying ack.
func (s *Server) admitSession(
	ctx context.Context,
	conn *net.UnixConn,
	codec *Codec,
	hello helloIdentity,
	peer proc.Peer,
) error {
	s.mu.Lock()
	if s.intakeClosed {
		s.mu.Unlock()
		_, _ = conn.Write(drainPreamble[:])
		_ = conn.Close()
		return ErrDraining
	}
	release, ok := s.acquireSlot()
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

// admitPreambleDrain is the repair channel's protocol-blind admission: the
// frozen two-byte preamble sits below the protocol gate — no frame, no
// version, no schema — so a successor speaking any protocol can drain this
// daemon. The connection is answered with nothing: the requester observes the
// drain out of the process table, and the close is its cue.
func (s *Server) admitPreambleDrain(ctx context.Context, conn *net.UnixConn, start time.Time) error {
	defer func() { _ = conn.Close() }()
	peer, err := proc.PeerCredentials(conn)
	if err != nil {
		return fmt.Errorf("wire: identify draining peer: %w", err)
	}
	s.log.Info("wire: drain admitted on the frozen preamble", "peer_pid", peer.PID)
	drainCtx, cancel := context.WithDeadline(ctx, start.Add(s.handshakeTimeout()))
	defer cancel()
	if _, err := s.executeDrain(drainCtx); err != nil {
		return fmt.Errorf("wire: preamble drain: %w", err)
	}
	return nil
}

func (s *Server) serveSession(
	ctx context.Context,
	conn net.Conn,
	codec *Codec,
	hello helloIdentity,
	peer proc.Peer,
	generation []byte,
	release func(),
) {
	releaseOnce := sync.OnceFunc(release)
	defer func() {
		releaseOnce()
		s.sessionWG.Done()
	}()
	err := s.runSession(ctx, conn, codec, hello.Schema, peer, generation, releaseOnce)
	if err != nil && !isDisconnect(err) {
		s.log.Debug("wire: session ended", "err", err)
	}
}

func (s *Server) runSession(
	ctx context.Context,
	conn net.Conn,
	codec *Codec,
	schema string,
	peer proc.Peer,
	generation []byte,
	release func(),
) error {
	stopContext := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopContext()
	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess := &session{
		server:       s,
		id:           s.lastSession.Add(1),
		conn:         conn,
		codec:        codec,
		ctx:          sessCtx,
		cancel:       cancel,
		peer:         peer,
		schema:       schema,
		current:      s.cfg.Schemas.Accepts(schema),
		generation:   generation,
		release:      release,
		outbound:     make(chan sessionOutbound, 4*s.concurrency),
		requestsDone: make(chan struct{}),
		writerDone:   make(chan struct{}),
		disconnected: make(chan struct{}),
		done:         make(chan struct{}),
		active:       make(map[uint64]*requestState),
		seen:         make(map[uint64]struct{}),
		idle:         s.cfg.Idle,
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

// CloseIntake prevents new connections and typed-rejects new product
// requests. Accepted sessions stay alive so admitted work and the drain can
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

func (s *Server) acquireSlot() (func(), bool) {
	select {
	case s.sessionSlots <- struct{}{}:
		return func() { <-s.sessionSlots }, true
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
		Protocol: codec.protocol(), Schema: s.cfg.Schemas.Own(),
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
