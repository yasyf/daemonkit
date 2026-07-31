package wire

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
)

// SpawnedNonceEnv carries the parent-minted attach nonce to a spawned child,
// hex-encoded, alongside proc's Cmd.Handoff descriptor.
const SpawnedNonceEnv = "DAEMONKIT_SPAWNED_NONCE"

const spawnedNonceBytes = 32

// SessionLimits is one exact, bounded spawned-session resource policy. Every
// queue derives from Concurrency.
type SessionLimits struct {
	Concurrency             int
	MaxFrame                int
	HandshakeTimeout        time.Duration
	WriteTimeout            time.Duration
	CancelSettlementTimeout time.Duration
}

// SpawnedSessionConfig configures one static business child session.
type SpawnedSessionConfig struct {
	// Nonce is the 32-byte single-use attach secret the parent minted,
	// conveyed via SpawnedNonceEnv.
	Nonce    []byte
	Schema   string
	Limits   SessionLimits
	Handlers []HandlerSpec
}

// SpawnedClientConfig configures one static business parent session.
type SpawnedClientConfig struct {
	// Conn is the parent end of proc's Cmd.Handoff socketpair, from Child.Handoff.
	Conn   *net.UnixConn
	Nonce  []byte
	Schema string
	Limits SessionLimits
}

// SpawnedClient is one sealed parent session with bounded calls and events.
type SpawnedClient struct{ client *Client }

// RunSpawnedSession claims the inherited handoff descriptor via
// proc.ClaimHandoff and serves exactly one business session on it. A wrong,
// absent, or repeated nonce closes the connection and returns the error: the
// child exits, it does not negotiate.
func RunSpawnedSession(ctx context.Context, config SpawnedSessionConfig) error {
	if len(config.Nonce) != spawnedNonceBytes {
		return fmt.Errorf("wire: spawned nonce must be %d bytes", spawnedNonceBytes)
	}
	if config.Schema == "" {
		return errors.New("wire: spawned Schema is required")
	}
	rt, err := newSpawnedRuntime(config.Handlers)
	if err != nil {
		return err
	}
	server, err := NewServer(rt, Config{
		Schemas:     Schemas{config.Schema},
		Concurrency: config.Limits.Concurrency,
		MaxFrame:    config.Limits.MaxFrame,
		Handshake:   config.Limits.HandshakeTimeout,
		Write:       config.Limits.WriteTimeout,
	})
	if err != nil {
		return err
	}
	file, err := proc.ClaimHandoff()
	if err != nil {
		return fmt.Errorf("wire: claim spawned handoff: %w", err)
	}
	fileConn, err := net.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		return errors.Join(fmt.Errorf("wire: adopt spawned handoff: %w", err), closeErr)
	}
	if closeErr != nil {
		_ = fileConn.Close()
		return closeErr
	}
	conn, ok := fileConn.(*net.UnixConn)
	if !ok {
		_ = fileConn.Close()
		return fmt.Errorf("wire: spawned handoff is %T, want unix stream", fileConn)
	}
	peer, err := trust.PeerCredentials(conn)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("wire: identify spawned peer: %w", err)
	}
	if err := trust.Verify(peer, nil); err != nil {
		_ = conn.Close()
		return fmt.Errorf("wire: verify spawned peer: %w", err)
	}
	codec := NewCodec(conn)
	codec.MaxFrame = server.maxFrame()
	if err := codec.SetDeadline(earlierDeadline(ctx, time.Now().Add(server.handshakeTimeout()))); err != nil {
		_ = conn.Close()
		return err
	}
	hello, err := readClientHello(codec)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if hello.Lane != LaneBusiness {
		_ = conn.Close()
		return fmt.Errorf("%w: spawned session requires the business lane", ErrHandshake)
	}
	if hello.Schema != config.Schema {
		_ = conn.Close()
		return fmt.Errorf("%w: %w", ErrHandshake, ErrBuildMismatch)
	}
	if subtle.ConstantTimeCompare(hello.Nonce, config.Nonce) != 1 {
		_ = conn.Close()
		return fmt.Errorf("%w: spawned nonce mismatch", ErrHandshake)
	}
	generation, err := server.completeHandshake(codec)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := codec.ClearDeadline(); err != nil {
		_ = conn.Close()
		return err
	}
	codec.WriteTimeout = server.writeTimeout()
	return server.runSession(ctx, conn, codec, LaneBusiness, hello.Schema, peer, generation)
}

// NewSpawnedClient attaches the parent end of the handoff socketpair, carrying
// the minted nonce, and completes the protocol-2 handshake.
func NewSpawnedClient(ctx context.Context, config SpawnedClientConfig) (*SpawnedClient, error) {
	if config.Conn == nil {
		return nil, errors.New("wire: spawned Conn is required")
	}
	if len(config.Nonce) != spawnedNonceBytes {
		return nil, fmt.Errorf("wire: spawned nonce must be %d bytes", spawnedNonceBytes)
	}
	if config.Schema == "" {
		return nil, errors.New("wire: spawned Schema is required")
	}
	peer, err := trust.PeerCredentials(config.Conn)
	if err != nil {
		return nil, fmt.Errorf("wire: identify spawned peer: %w", err)
	}
	if err := trust.Verify(peer, nil); err != nil {
		return nil, fmt.Errorf("wire: verify spawned peer: %w", err)
	}
	var once sync.Once
	dial := func(context.Context) (net.Conn, error) {
		var conn net.Conn
		once.Do(func() { conn = config.Conn })
		if conn == nil {
			return nil, errors.New("wire: spawned parent endpoint already consumed")
		}
		return conn, nil
	}
	client, err := NewClient(ctx, ClientConfig{
		Dial:                    dial,
		Lane:                    LaneBusiness,
		Schema:                  config.Schema,
		Nonce:                   config.Nonce,
		Concurrency:             config.Limits.Concurrency,
		MaxFrame:                config.Limits.MaxFrame,
		HandshakeTimeout:        config.Limits.HandshakeTimeout,
		WriteTimeout:            config.Limits.WriteTimeout,
		CancelSettlementTimeout: config.Limits.CancelSettlementTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &SpawnedClient{client: client}, nil
}

// Call sends one unary request and waits for its terminal response.
func (c *SpawnedClient) Call(ctx context.Context, op Op, tenant string, payload []byte) (Result, error) {
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
	return c.client.Open(ctx, op, tenant, payload, endInput)
}

// Events returns the bounded server-pushed event stream.
func (c *SpawnedClient) Events() <-chan Event { return c.client.Events() }

// WireBuild returns this session's exact static schema identity.
func (c *SpawnedClient) WireBuild() string { return c.client.WireBuild() }

// Close sends GoAway and joins all client session loops.
func (c *SpawnedClient) Close() error { return c.client.Close() }

// Abort tears down the session and joins all client session loops.
func (c *SpawnedClient) Abort(cause error) error { return c.client.Abort(cause) }

type spawnedRuntime struct {
	handlers map[Op]HandlerSpec
	serial   sync.Mutex
}

func newSpawnedRuntime(specs []HandlerSpec) (*spawnedRuntime, error) {
	if len(specs) == 0 {
		return nil, errors.New("wire: spawned handlers are required")
	}
	handlers := make(map[Op]HandlerSpec, len(specs))
	for _, spec := range specs {
		if spec.Op == "" || spec.Handler == nil {
			return nil, errors.New("wire: spawned operation and handler are required")
		}
		if strings.HasPrefix(string(spec.Op), reservedOpPrefix) {
			return nil, fmt.Errorf("wire: spawned op %q uses daemonkit's private namespace", spec.Op)
		}
		if _, exists := handlers[spec.Op]; exists {
			return nil, fmt.Errorf("wire: spawned op %q is duplicated", spec.Op)
		}
		handlers[spec.Op] = spec
	}
	return &spawnedRuntime{handlers: handlers}, nil
}

func (r *spawnedRuntime) Handle(ctx context.Context, req Request) (any, error) {
	spec, ok := r.handlers[req.Op]
	if !ok {
		return nil, fmt.Errorf("wire: unknown op %q", req.Op)
	}
	if !spec.Concurrent {
		r.serial.Lock()
		defer r.serial.Unlock()
	}
	return spec.Handler(ctx, req)
}

func (r *spawnedRuntime) Phase() PhaseSnapshot {
	return PhaseSnapshot{Sequence: 1, Phase: PhaseReady}
}

func (r *spawnedRuntime) WaitPhase(ctx context.Context, after uint64) (PhaseSnapshot, error) {
	if after == 0 {
		return r.Phase(), nil
	}
	<-ctx.Done()
	return PhaseSnapshot{}, ctx.Err()
}

func (*spawnedRuntime) Drain() {}
