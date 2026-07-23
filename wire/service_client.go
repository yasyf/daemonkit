package wire

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const serviceRetryDelay = 25 * time.Millisecond

// ErrServiceClientClosed means a service call arrived after Close.
var ErrServiceClientClosed = errors.New("wire: service client is closed")

type serviceSession struct {
	client  *Client
	active  int
	retired bool
}

// ServiceClient owns persistent unary sessions across expected service startup
// and takeover.
type ServiceClient struct {
	config ClientConfig
	wait   func(context.Context, time.Duration) error

	mu        sync.Mutex
	current   *serviceSession
	sessions  map[*serviceSession]struct{}
	dialing   chan struct{}
	closed    bool
	closeOnce sync.Once
	done      chan struct{}
	closeErr  error
}

// NewServiceClient returns a lazy persistent client for one exact wire build.
func NewServiceClient(config ClientConfig) (*ServiceClient, error) {
	if _, err := validateClientConfig(config); err != nil {
		return nil, err
	}
	return &ServiceClient{
		config: config, wait: waitServiceRetry, sessions: make(map[*serviceSession]struct{}), done: make(chan struct{}),
	}, nil
}

// WireBuild returns the exact schema identity required from every generation.
func (c *ServiceClient) WireBuild() string { return c.config.WireBuild }

// Done closes only when the service client itself is closed.
func (c *ServiceClient) Done() <-chan struct{} { return c.done }

// Call waits through proven non-dispatch startup and takeover transitions.
func (c *ServiceClient) Call(
	ctx context.Context,
	op Op,
	tenant string,
	payload []byte,
) (Result, error) {
	if err := validateOperation(op); err != nil {
		return Result{Outcome: PreSendFailure}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return Result{Outcome: PreSendFailure}, err
		}
		session, err := c.acquire(ctx)
		if err != nil {
			if !provesNoListener(err) {
				return Result{Outcome: PreSendFailure}, err
			}
			if err := c.wait(ctx, serviceRetryDelay); err != nil {
				return Result{Outcome: PreSendFailure}, fmt.Errorf("wire: await service endpoint: %w", err)
			}
			continue
		}

		result, callErr := session.client.Call(ctx, op, tenant, payload)
		retry, reconnect := retryServiceCall(result, callErr)
		if reconnect {
			c.retire(session)
		}
		c.release(ctx, session)
		if !retry {
			return result, callErr
		}
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("wire: await service readiness: %w", err)
		}
		if err := c.wait(ctx, serviceRetryDelay); err != nil {
			return result, fmt.Errorf("wire: await service readiness: %w", err)
		}
	}
}

// Close permanently closes current and retiring service sessions.
func (c *ServiceClient) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		sessions := make([]*serviceSession, 0, len(c.sessions))
		for session := range c.sessions {
			session.retired = true
			sessions = append(sessions, session)
		}
		c.current = nil
		clear(c.sessions)
		c.mu.Unlock()

		errs := make([]error, 0, len(sessions))
		for _, session := range sessions {
			if err := session.client.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		c.closeErr = errors.Join(errs...)
		close(c.done)
	})
	return c.closeErr
}

func (c *ServiceClient) acquire(ctx context.Context) (*serviceSession, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrServiceClientClosed
		}
		if c.current != nil && !c.current.retired {
			session := c.current
			session.active++
			c.mu.Unlock()
			return session, nil
		}
		if c.dialing != nil {
			dialing := c.dialing
			c.mu.Unlock()
			select {
			case <-dialing:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		dialing := make(chan struct{})
		c.dialing = dialing
		c.mu.Unlock()

		client, err := NewClient(ctx, c.config)
		c.mu.Lock()
		c.dialing = nil
		closed := c.closed
		var session *serviceSession
		if err == nil && !closed {
			session = &serviceSession{client: client, active: 1}
			c.current = session
			c.sessions[session] = struct{}{}
		}
		close(dialing)
		c.mu.Unlock()

		if err != nil {
			return nil, err
		}
		if closed {
			return nil, errors.Join(ErrServiceClientClosed, client.close(ctx))
		}
		return session, nil
	}
}

func (c *ServiceClient) retire(session *serviceSession) {
	c.mu.Lock()
	session.retired = true
	if c.current == session {
		c.current = nil
	}
	c.mu.Unlock()
}

func (c *ServiceClient) release(ctx context.Context, session *serviceSession) {
	closeSession := false
	c.mu.Lock()
	session.active--
	if session.active < 0 {
		c.mu.Unlock()
		panic("wire: negative service session references")
	}
	if session.retired && session.active == 0 {
		if _, tracked := c.sessions[session]; tracked {
			delete(c.sessions, session)
			closeSession = true
		}
	}
	c.mu.Unlock()
	if closeSession {
		_ = session.client.close(ctx)
	}
}

func retryServiceCall(result Result, err error) (retry, reconnect bool) {
	if result.Outcome == Rejected {
		rejection := result.Rejection()
		return errors.Is(rejection, ErrNotReady) || errors.Is(rejection, ErrDraining),
			errors.Is(rejection, ErrDraining)
	}
	if result.Outcome != PreSendFailure || errors.Is(err, ErrProtocolVersion) || errors.Is(err, ErrInvalidFrame) {
		return false, false
	}
	return true, true
}

func waitServiceRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
