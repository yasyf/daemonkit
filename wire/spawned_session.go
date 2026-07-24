package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/internal/spawnedsession"
	"github.com/yasyf/daemonkit/proc"
)

// SessionLimits is one exact, bounded spawned-session resource policy.
type SessionLimits struct {
	Workers                 int
	Backlog                 int
	MaxFrame                int
	InboundQueue            int
	OutboundQueue           int
	StreamQueue             int
	EventQueue              int
	HandshakeTimeout        time.Duration
	WriteTimeout            time.Duration
	CancelSettlementTimeout time.Duration
}

// SpawnedSessionConfig configures one static ordinary child session.
type SpawnedSessionConfig struct {
	Identity  proc.SpawnedSessionIdentity
	WireBuild string
	Ladder    Ladder
	Limits    SessionLimits
	Handlers  []HandlerSpec
}

// SpawnedClientConfig configures one static ordinary parent session.
type SpawnedClientConfig struct {
	Endpoint  proc.SpawnedSessionEndpoint
	WireBuild string
	Ladder    Ladder
	Limits    SessionLimits
}

// SpawnedClient is one sealed parent session with bounded calls and events.
type SpawnedClient struct{ client *Client }

// RunSpawnedSession consumes the inherited child identity and joins its one session.
func RunSpawnedSession(ctx context.Context, config SpawnedSessionConfig) error {
	server, err := compileSpawnedServer(config)
	if err != nil {
		return err
	}
	opened, err := config.Identity.OpenForWire(spawnedsession.WireAuthority())
	if err != nil {
		return fmt.Errorf("wire: open spawned child session: %w", err)
	}
	peer := Peer{
		PID: opened.Peer.PID, UID: opened.Peer.UID,
		StartTime: opened.Peer.StartTime, Boot: opened.Peer.Boot,
		Comm: opened.Peer.Comm, Executable: opened.Peer.Executable,
	}
	workers, err := server.startStatic()
	if err != nil {
		_ = opened.Conn.Close()
		return err
	}
	server.startWorkers(workers)
	defer server.stopWorkers()
	return server.serveConn(
		ctx, opened.Conn, peer, "", false,
		staticAdmission, staticAdmission, func() {}, nil,
	)
}

// NewSpawnedClient consumes the receipt-bound endpoint and completes the v1 handshake.
func NewSpawnedClient(ctx context.Context, config SpawnedClientConfig) (*SpawnedClient, error) {
	if err := validateSpawnedClientConfig(config); err != nil {
		return nil, err
	}
	opened, err := config.Endpoint.OpenForWire(ctx, spawnedsession.WireAuthority())
	if err != nil {
		return nil, fmt.Errorf("wire: open spawned parent session: %w", err)
	}
	var once sync.Once
	dial := func(context.Context) (net.Conn, error) {
		var conn net.Conn
		once.Do(func() {
			conn = opened.Conn
			opened.Conn = nil
		})
		if conn == nil {
			return nil, errors.New("wire: spawned parent endpoint already consumed")
		}
		return conn, nil
	}
	client, err := newClient(ctx, ClientConfig{
		Dial: dial, WireBuild: config.WireBuild, Ladder: config.Ladder,
		MaxFrame:                config.Limits.MaxFrame,
		OutboundQueue:           config.Limits.OutboundQueue,
		StreamQueue:             config.Limits.StreamQueue,
		EventQueue:              config.Limits.EventQueue,
		HandshakeTimeout:        config.Limits.HandshakeTimeout,
		WriteTimeout:            config.Limits.WriteTimeout,
		CancelSettlementTimeout: config.Limits.CancelSettlementTimeout,
	})
	if err != nil {
		if opened.Conn != nil {
			_ = opened.Conn.Close()
		}
		return nil, err
	}
	return &SpawnedClient{client: client}, nil
}

// Call sends one unary request and waits for its terminal response.
func (c *SpawnedClient) Call(ctx context.Context, op Op, tenant string, payload []byte) (Result, error) {
	if c == nil || c.client == nil {
		return Result{Outcome: PreSendFailure}, errors.New("wire: spawned client is required")
	}
	return c.client.Call(ctx, op, tenant, payload)
}

// OpenStream starts one request with bounded bidirectional streaming.
func (c *SpawnedClient) OpenStream(
	ctx context.Context,
	op Op,
	tenant string,
	payload []byte,
	endInput bool,
) (*ClientCall, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("wire: spawned client is required")
	}
	return c.client.Open(ctx, op, tenant, payload, endInput)
}

// Events returns the bounded server-pushed event stream.
func (c *SpawnedClient) Events() <-chan Event {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Events()
}

// WireBuild returns this session's exact static schema identity.
func (c *SpawnedClient) WireBuild() string {
	if c == nil || c.client == nil {
		return ""
	}
	return c.client.WireBuild()
}

// Close sends GoAway and joins all client session loops.
func (c *SpawnedClient) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

// Abort tears down the session and joins all client session loops.
func (c *SpawnedClient) Abort(cause error) error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Abort(cause)
}

func compileSpawnedServer(config SpawnedSessionConfig) (*Server, error) {
	if config.WireBuild == "" {
		return nil, errors.New("wire: spawned WireBuild is required")
	}
	if err := validateSpawnedLimits(config.Limits); err != nil {
		return nil, err
	}
	handlers, err := copySpawnedHandlers(config.Handlers, config.Ladder)
	if err != nil {
		return nil, err
	}
	return &Server{
		WireBuild: config.WireBuild, Ladder: config.Ladder,
		Workers: config.Limits.Workers, Backlog: config.Limits.Backlog,
		MaxFrame:         config.Limits.MaxFrame,
		InboundQueue:     config.Limits.InboundQueue,
		OutboundQueue:    config.Limits.OutboundQueue,
		StreamQueue:      config.Limits.StreamQueue,
		HandshakeTimeout: config.Limits.HandshakeTimeout,
		WriteTimeout:     config.Limits.WriteTimeout,
		handlers:         handlers, staticOrdinary: true,
	}, nil
}

func validateSpawnedClientConfig(config SpawnedClientConfig) error {
	if config.WireBuild == "" {
		return errors.New("wire: spawned WireBuild is required")
	}
	return validateSpawnedLimits(config.Limits)
}

func validateSpawnedLimits(limits SessionLimits) error {
	switch {
	case limits.Workers <= 0:
		return errors.New("wire: spawned workers must be positive")
	case limits.Backlog < 0:
		return errors.New("wire: spawned backlog must not be negative")
	case limits.MaxFrame <= 0:
		return errors.New("wire: spawned max frame must be positive")
	case limits.InboundQueue <= 0:
		return errors.New("wire: spawned inbound queue must be positive")
	case limits.OutboundQueue <= 0:
		return errors.New("wire: spawned outbound queue must be positive")
	case limits.StreamQueue <= 0:
		return errors.New("wire: spawned stream queue must be positive")
	case limits.EventQueue <= 0:
		return errors.New("wire: spawned event queue must be positive")
	case limits.HandshakeTimeout <= 0:
		return errors.New("wire: spawned handshake timeout must be positive")
	case limits.WriteTimeout <= 0:
		return errors.New("wire: spawned write timeout must be positive")
	case limits.CancelSettlementTimeout <= 0:
		return errors.New("wire: spawned cancellation settlement timeout must be positive")
	}
	if _, err := uint32Length("spawned stream queue", limits.StreamQueue); err != nil {
		return err
	}
	if _, err := uint32Length("spawned event queue", limits.EventQueue); err != nil {
		return err
	}
	if limits.Workers > int(^uint(0)>>1)-limits.Backlog {
		return errors.New("wire: spawned worker capacity overflows")
	}
	return nil
}

func copySpawnedHandlers(specs []HandlerSpec, ladder Ladder) (map[Op]entry, error) {
	if len(specs) == 0 {
		return nil, errors.New("wire: spawned handlers are required")
	}
	handlers := make(map[Op]entry, len(specs))
	for _, spec := range append([]HandlerSpec(nil), specs...) {
		if spec.Op == "" || spec.Handler == nil {
			return nil, errors.New("wire: spawned operation and handler are required")
		}
		if _, reserved := reservedOps[spec.Op]; reserved || strings.HasPrefix(string(spec.Op), "daemon.") {
			return nil, fmt.Errorf("wire: spawned op %q uses daemonkit's private namespace", spec.Op)
		}
		if _, exists := handlers[spec.Op]; exists {
			return nil, fmt.Errorf("wire: spawned op %q is duplicated", spec.Op)
		}
		if _, _, ok := ladder.Deadlines(spec.Op); !ok {
			return nil, fmt.Errorf("wire: spawned op %q has no exact deadline pair", spec.Op)
		}
		kind := classControl
		if spec.Concurrent {
			kind = classConcurrent
		}
		handlers[spec.Op] = entry{class: kind, route: routeBusiness, h: spec.Handler}
	}
	return handlers, nil
}

func (s *Server) startStatic() (int, error) {
	streamWindow, err := uint32Length("stream queue", s.StreamQueue)
	if err != nil {
		return 0, errors.New("wire: spawned stream queue exceeds protocol window")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return 0, ErrServerStarted
	}
	s.started = true
	s.sessions = make(map[*session]struct{})
	s.streamWindow = streamWindow
	s.queue = make(chan job, s.Backlog)
	s.slots = make(chan struct{}, s.Workers+s.Backlog)
	return s.Workers, nil
}

func staticAdmission() (daemon.Publication, func(), error) {
	return daemon.Publication{}, func() {}, nil
}
