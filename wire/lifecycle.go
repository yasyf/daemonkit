package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/wire/lifeproto"
)

// Lifecycle is the daemon runtime surface exposed through reserved v1 operations.
type Lifecycle interface {
	Health(context.Context) (daemon.Health, error)
	Shutdown(context.Context) error
	Handoff(context.Context) error
}

// RegisterLifecycle installs the reserved lifecycle handlers: health, shutdown,
// and handoff. They skip build-mismatch and drain rejection — a shutdown
// request is acknowledged mid-drain — but require protected admission and an
// authorized same-or-newer daemon build.
func (s *Server) RegisterLifecycle(lifecycle Lifecycle) {
	if lifecycle == nil {
		panic("wire: lifecycle is required")
	}
	s.register(Op(lifeproto.OpHealth), classControl, routeLifecycle, func(ctx context.Context, req Request) (any, error) {
		var message lifeproto.HealthRequest
		if err := decodeLifecycle(req, lifeproto.OpHealth, &message); err != nil {
			return nil, err
		}
		health, err := lifecycle.Health(ctx)
		if err != nil {
			return nil, err
		}
		return lifeproto.NewHealthResponse(
			health.Build,
			health.Protocol,
			health.PID,
			string(health.State),
			health.Draining,
			health.Busy,
		), nil
	})
	s.register(Op(lifeproto.OpShutdown), classControl, routeLifecycle, func(ctx context.Context, req Request) (any, error) {
		var message lifeproto.ShutdownRequest
		if err := decodeLifecycle(req, lifeproto.OpShutdown, &message); err != nil {
			return nil, err
		}
		if err := lifecycle.Shutdown(ctx); err != nil {
			return nil, err
		}
		return lifeproto.NewShutdownResponse(true), nil
	})
	s.register(Op(lifeproto.OpHandoff), classControl, routeLifecycle, func(ctx context.Context, req Request) (any, error) {
		var message lifeproto.HandoffRequest
		if err := decodeLifecycle(req, lifeproto.OpHandoff, &message); err != nil {
			return nil, err
		}
		if err := lifecycle.Handoff(ctx); err != nil {
			return nil, err
		}
		return lifeproto.NewHandoffResponse(true), nil
	})
}

func decodeLifecycle(req Request, op string, dst any) error {
	envelope, err := lifeproto.DecodeEnvelope(req.Payload)
	if err != nil {
		return err
	}
	if envelope.Op != op || string(req.Op) != op {
		return fmt.Errorf("wire: lifecycle op mismatch: route=%q payload=%q", req.Op, envelope.Op)
	}
	if err := decodeStrict(req.Payload, dst); err != nil {
		return fmt.Errorf("wire: decode lifecycle %s: %w", op, err)
	}
	return nil
}

// LifecyclePeer is a persistent v1 session client implementing daemon.Peer.
type LifecyclePeer struct {
	Config ClientConfig

	mu     sync.Mutex
	client *Client
}

var _ daemon.Peer = (*LifecyclePeer)(nil)

// Health returns the peer's exact build/protocol snapshot.
func (p *LifecyclePeer) Health(ctx context.Context) (daemon.Health, error) {
	var response lifeproto.HealthResponse
	err := p.call(ctx, Op(lifeproto.OpHealth), lifeproto.NewHealthRequest(), &response)
	if err != nil && !errors.Is(err, daemon.ErrNoPeer) && p.disconnected() {
		err = p.call(ctx, Op(lifeproto.OpHealth), lifeproto.NewHealthRequest(), &response)
	}
	if err != nil {
		return daemon.Health{}, err
	}
	return daemon.Health{
		Build:    response.Build,
		Protocol: response.Protocol,
		PID:      response.PID,
		State:    daemon.State(response.State),
		Draining: response.Draining,
		Busy:     response.Busy,
	}, nil
}

func (p *LifecyclePeer) disconnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client == nil
}

// Shutdown asks the peer to begin orderly shutdown.
func (p *LifecyclePeer) Shutdown(ctx context.Context) error {
	var response lifeproto.ShutdownResponse
	if err := p.callPreSend(ctx, Op(lifeproto.OpShutdown), lifeproto.NewShutdownRequest(), &response); err != nil {
		return err
	}
	if !response.OK {
		return errors.New("wire: lifecycle shutdown not acknowledged")
	}
	return nil
}

// Handoff asks the peer to release its listener for a successor.
func (p *LifecyclePeer) Handoff(ctx context.Context) error {
	var response lifeproto.HandoffResponse
	if err := p.callPreSend(ctx, Op(lifeproto.OpHandoff), lifeproto.NewHandoffRequest(), &response); err != nil {
		return err
	}
	if !response.OK {
		return errors.New("wire: lifecycle handoff not acknowledged")
	}
	return nil
}

// Close closes the persistent lifecycle session when one has been established.
func (p *LifecyclePeer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		return nil
	}
	err := p.client.Close()
	p.client = nil
	return err
}

func (p *LifecyclePeer) call(ctx context.Context, op Op, message any, dst any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	payload, err := lifeproto.Encode(message)
	if err != nil {
		return err
	}
	client, err := p.session(ctx)
	if err != nil {
		return err
	}
	result, err := client.Call(ctx, op, "", payload)
	if err != nil {
		p.reset(ctx, client)
		return err
	}
	if result.Response.Err != "" {
		return errors.New(result.Response.Err)
	}
	if result.Outcome != Delivered {
		return fmt.Errorf("wire: lifecycle %s outcome %s: %s", op, result.Outcome, result.Response.Reason)
	}
	if err := decodeStrict(result.Response.Payload, dst); err != nil {
		return fmt.Errorf("wire: decode lifecycle %s response: %w", op, err)
	}
	envelope, err := lifeproto.DecodeEnvelope(result.Response.Payload)
	if err != nil {
		return err
	}
	if envelope.Op != string(op) {
		return fmt.Errorf("wire: lifecycle response op mismatch: got %q, want %q", envelope.Op, op)
	}
	return nil
}

func (p *LifecyclePeer) callPreSend(ctx context.Context, op Op, message any, dst any) error {
	err := p.call(ctx, op, message, dst)
	if err == nil || ctx.Err() != nil || !provesPreSendFailure(err) {
		return err
	}
	return p.call(ctx, op, message, dst)
}

func provesPreSendFailure(err error) bool {
	var openErr *OpenError
	return errors.As(err, &openErr) && openErr.Outcome == PreSendFailure
}

func (p *LifecyclePeer) session(ctx context.Context) (*Client, error) {
	if p.client != nil {
		return p.client, nil
	}
	if p.Config.LifecycleBuild == "" {
		return nil, errors.New("wire: lifecycle peer release build is required")
	}
	client, err := NewClient(ctx, p.Config)
	if err != nil {
		if provesNoListener(err) {
			return nil, fmt.Errorf("wire: lifecycle dial: %w", daemon.ErrNoPeer)
		}
		return nil, err
	}
	p.client = client
	return client, nil
}

func (p *LifecyclePeer) reset(ctx context.Context, client *Client) {
	if p.client != client {
		return
	}
	_ = client.close(ctx)
	p.client = nil
}

func provesNoListener(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}

// UnixDialer returns a context-aware unix socket dialer for ClientConfig.
func UnixDialer(path string) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", path)
	}
}
