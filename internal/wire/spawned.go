package wire

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
)

// SpawnedNonceEnv carries the parent-minted attach nonce to a spawned child,
// hex-encoded, alongside proc's Cmd.Handoff descriptor. The nonce is not a
// secret — any same-UID process reads a peer's environment via KERN_PROCARGS2
// — it is fd-mixup defence: proof the attaching peer inherited fd 3 from this
// exec rather than some other descriptor plumbing.
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
	// Conn is the claimed child end of proc's Cmd.Handoff socketpair.
	// RunSpawnedSession owns it from the call on and closes it on every return.
	Conn net.Conn
	// Nonce is the 32-byte single-use attach nonce the parent minted, conveyed
	// via SpawnedNonceEnv. Fd-mixup defence, not a secret.
	Nonce   []byte
	Schema  string
	Limits  SessionLimits
	Runtime Runtime
}

// SpawnedClientConfig configures one static business parent session.
type SpawnedClientConfig struct {
	// Conn is the parent end of proc's Cmd.Handoff socketpair, from Child.Handoff.
	Conn   net.Conn
	Nonce  []byte
	Schema string
	Limits SessionLimits
}

// RunSpawnedSession serves exactly one business session on the claimed handoff
// descriptor. A wrong, absent, or repeated nonce closes the connection and
// returns the error: the child exits, it does not negotiate.
func RunSpawnedSession(ctx context.Context, config SpawnedSessionConfig) error {
	if config.Conn == nil {
		return errors.New("wire: spawned Conn is required")
	}
	conn, ok := config.Conn.(*net.UnixConn)
	if !ok {
		_ = config.Conn.Close()
		return fmt.Errorf("wire: spawned handoff is %T, want unix stream", config.Conn)
	}
	if len(config.Nonce) != spawnedNonceBytes {
		_ = conn.Close()
		return fmt.Errorf("wire: spawned nonce must be %d bytes", spawnedNonceBytes)
	}
	if config.Schema == "" {
		_ = conn.Close()
		return errors.New("wire: spawned Schema is required")
	}
	if config.Runtime == nil {
		_ = conn.Close()
		return errors.New("wire: spawned Runtime is required")
	}
	server, err := NewServer(config.Runtime, Config{
		Schemas:     Schemas{config.Schema},
		Concurrency: config.Limits.Concurrency,
		MaxFrame:    config.Limits.MaxFrame,
		Handshake:   config.Limits.HandshakeTimeout,
		Write:       config.Limits.WriteTimeout,
	})
	if err != nil {
		_ = conn.Close()
		return err
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

// authorizeSpawnedConn is the spawned lane's named Authorize waiver. The lane's
// property is directional confinement, not peer identity: the conn is the
// parent's own end of a kernel socketpair, a channel no other process has a
// path to dial, so there is no accepting peer to judge — the child's nonce
// check is the fd-mixup defence on the other end.
func authorizeSpawnedConn(net.Conn) error { return nil }

// NewSpawnedClient attaches the parent end of the handoff socketpair, carrying
// the minted nonce, and completes the protocol-2 handshake. It verifies no
// peer: credentials read from a self-created socketpair name the creator —
// the parent itself — so a check here would only ever judge this process.
// Confinement is directional, by construction of the pair.
func NewSpawnedClient(ctx context.Context, config SpawnedClientConfig) (*Client, error) {
	if config.Conn == nil {
		return nil, errors.New("wire: spawned Conn is required")
	}
	if len(config.Nonce) != spawnedNonceBytes {
		return nil, fmt.Errorf("wire: spawned nonce must be %d bytes", spawnedNonceBytes)
	}
	if config.Schema == "" {
		return nil, errors.New("wire: spawned Schema is required")
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
	return NewClient(ctx, ClientConfig{
		Dial:                    dial,
		Authorize:               authorizeSpawnedConn,
		Lane:                    LaneBusiness,
		Schema:                  config.Schema,
		Nonce:                   config.Nonce,
		Concurrency:             config.Limits.Concurrency,
		MaxFrame:                config.Limits.MaxFrame,
		HandshakeTimeout:        config.Limits.HandshakeTimeout,
		WriteTimeout:            config.Limits.WriteTimeout,
		CancelSettlementTimeout: config.Limits.CancelSettlementTimeout,
	})
}
