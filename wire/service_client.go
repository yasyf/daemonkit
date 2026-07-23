package wire

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrServiceClientClosed means Close permanently rejected the logical client.
var ErrServiceClientClosed = errors.New("wire: service client is closed")

// ReplayPolicy states the proof required before ServiceClient may repeat one
// logical call on a replacement runtime session.
type ReplayPolicy uint8

const (
	// ReplayProvenNonDispatch permits replay only when wire proves the request
	// was not dispatched.
	ReplayProvenNonDispatch ReplayPolicy = iota
	// ReplayIdempotent also permits replay after delivery becomes unknown. The
	// caller must supply a product-level stable operation identity.
	ReplayIdempotent
)

// ServiceCall is one context-bounded logical unary request.
type ServiceCall struct {
	Op               Op
	Tenant           string
	Payload          []byte
	Replay           ReplayPolicy
	ExpectedIdentity *RuntimeIdentity
}

type serviceConnect struct {
	done     chan struct{}
	changed  chan struct{}
	identity RuntimeIdentity
	progress ReadinessProgress
	err      error
}

type serviceSession struct {
	client   *Client
	identity RuntimeIdentity
	progress ReadinessProgress
	active   int
	retired  bool
	removed  bool
	cause    error
}

// ServiceClient maintains one exact-build logical service across runtime
// publication, drain, listener replacement, and stale-session transitions.
type ServiceClient struct {
	config RuntimeClientConfig
	wait   readinessWaiter
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	closed     bool
	terminal   error
	done       chan struct{}
	current    *serviceSession
	connecting *serviceConnect
	sessions   map[*serviceSession]struct{}
}

// NewServiceClient returns a lazy reconnecting client for one exact wire
// build. Each Call owns its complete readiness and operation deadline.
func NewServiceClient(config RuntimeClientConfig) (*ServiceClient, error) {
	return newServiceClient(config, waitReadinessRetry)
}

func newServiceClient(
	config RuntimeClientConfig,
	wait readinessWaiter,
) (*ServiceClient, error) {
	if err := validateRuntimeClientConfig(config); err != nil {
		return nil, err
	}
	serviceContext, cancel := context.WithCancel(context.Background())
	client := &ServiceClient{
		config:   config,
		wait:     wait,
		ctx:      serviceContext,
		cancel:   cancel,
		done:     make(chan struct{}),
		sessions: make(map[*serviceSession]struct{}),
	}
	return client, nil
}

// WireBuild returns the exact schema identity required from every generation.
func (c *ServiceClient) WireBuild() string { return c.config.Client.WireBuild }

// Call executes one logical request across expected runtime handoffs. Every
// call context must carry its complete deadline.
func (c *ServiceClient) Call(ctx context.Context, call ServiceCall) (Result, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Result{Outcome: PreSendFailure}, errors.New("wire: service call requires a caller deadline")
	}
	if err := ctx.Err(); err != nil {
		return Result{Outcome: PreSendFailure}, err
	}
	if call.Op == "" {
		return Result{Outcome: PreSendFailure}, errors.New("wire: service call operation is required")
	}
	if err := validateOperation(call.Op); err != nil {
		return Result{Outcome: PreSendFailure}, err
	}
	if call.Replay != ReplayProvenNonDispatch && call.Replay != ReplayIdempotent {
		return Result{Outcome: PreSendFailure}, fmt.Errorf("wire: invalid replay policy %d", call.Replay)
	}
	if call.ExpectedIdentity != nil {
		if err := validateRuntimeIdentity(*call.ExpectedIdentity); err != nil {
			return Result{Outcome: PreSendFailure}, err
		}
		expected := *call.ExpectedIdentity
		call.ExpectedIdentity = &expected
	}
	call.Payload = append([]byte(nil), call.Payload...)
	progress := newRuntimeProgressTracker(c.config, call.ExpectedIdentity, realReadinessClock{})
	for {
		session, err := c.acquire(ctx, progress)
		if err != nil {
			return Result{Outcome: PreSendFailure}, err
		}
		result, callErr := session.client.Call(ctx, call.Op, call.Tenant, call.Payload)
		if callerErr := ctx.Err(); callerErr != nil && errors.Is(callErr, callerErr) {
			c.release(session)
			return result, callErr
		}
		transition := isServiceCallTransition(result, callErr)
		replay := serviceCallReplayable(result, callErr, call.Replay)
		if terminal := serviceCallTerminal(ctx, result, callErr); terminal != nil {
			c.terminate(terminal)
			c.release(session)
			return result, callErr
		}
		if transition {
			cause := callErr
			if cause == nil {
				cause = result.Rejection()
			}
			c.retire(session, cause)
		}
		c.release(session)
		if replay {
			continue
		}
		return result, callErr
	}
}

// Done closes when Close is called or an exact terminal service identity
// failure is observed. Raw session replacement never closes it.
func (c *ServiceClient) Done() <-chan struct{} { return c.done }

// Close aborts every owned session and rejects future calls.
func (c *ServiceClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cancel()
	closeServiceDone(c.done)
	clients := c.detachSessionsLocked()
	c.mu.Unlock()
	var result error
	for _, client := range clients {
		result = errors.Join(result, client.Abort(ErrServiceClientClosed))
	}
	return result
}

func (c *ServiceClient) acquire(
	ctx context.Context,
	progress *runtimeProgressTracker,
) (*serviceSession, error) {
	for {
		c.mu.Lock()
		if c.terminal != nil {
			terminal := c.terminal
			c.mu.Unlock()
			return nil, terminal
		}
		if c.closed {
			c.mu.Unlock()
			return nil, ErrServiceClientClosed
		}
		if c.current != nil {
			if err := c.current.client.sessionErr(); err == nil {
				if err := progress.adopt(c.current.identity, c.current.progress); err != nil {
					c.mu.Unlock()
					return nil, err
				}
				c.current.active++
				session := c.current
				c.mu.Unlock()
				return session, nil
			}
			cleanup := c.retireLocked(c.current, errRuntimePeerStale)
			c.mu.Unlock()
			if cleanup != nil {
				_ = cleanup.Abort(errRuntimePeerStale)
			}
			continue
		}
		if c.connecting != nil {
			connecting := c.connecting
			c.mu.Unlock()
			if err := c.waitConnect(ctx, progress, connecting); err != nil {
				return nil, err
			}
			continue
		}
		connecting := &serviceConnect{done: make(chan struct{}), changed: make(chan struct{})}
		c.connecting = connecting
		c.mu.Unlock()
		go c.connect(connecting)
	}
}

func (c *ServiceClient) connect(connecting *serviceConnect) {
	progress := &runtimeProgressTracker{
		clock: realReadinessClock{}, progress: ReadinessProgress{Detail: []byte{}},
	}
	progress.notify = func(identity RuntimeIdentity, snapshot ReadinessProgress) {
		c.mu.Lock()
		if c.connecting == connecting {
			connecting.identity = identity
			connecting.progress = cloneReadinessProgress(snapshot)
			close(connecting.changed)
			connecting.changed = make(chan struct{})
		}
		c.mu.Unlock()
	}
	client, err := waitRuntimeReadyTracked(
		c.ctx, c.config, nil, c.wait, progress, true,
	)
	if errors.Is(err, context.Canceled) {
		err = ErrServiceClientClosed
	}

	c.mu.Lock()
	closed := c.closed
	terminal := c.terminal
	var terminalClients []*Client
	if err != nil && !closed && terminal == nil && isTerminalReadinessFailure(err) {
		c.terminal = err
		terminal = err
		c.cancel()
		closeServiceDone(c.done)
		terminalClients = c.detachSessionsLocked()
	}
	var session *serviceSession
	if err == nil && !closed && terminal == nil {
		identity, snapshot := progress.current()
		session = &serviceSession{client: client, identity: identity, progress: snapshot}
		c.current = session
		c.sessions[session] = struct{}{}
	}
	c.mu.Unlock()

	if session != nil {
		started := make(chan struct{})
		go c.monitorSession(session, progress, started)
		<-started
	}

	c.mu.Lock()
	if c.connecting == connecting {
		c.connecting = nil
		connecting.err = err
		if terminal != nil {
			connecting.err = terminal
		} else if closed {
			connecting.err = ErrServiceClientClosed
		}
		close(connecting.done)
	}
	c.mu.Unlock()
	for _, terminalClient := range terminalClients {
		_ = terminalClient.Abort(err)
	}
	if client != nil && (closed || terminal != nil) {
		_ = client.Abort(connecting.err)
	}
}

func (c *ServiceClient) waitConnect(
	ctx context.Context,
	progress *runtimeProgressTracker,
	connecting *serviceConnect,
) error {
	for {
		c.mu.Lock()
		identity := connecting.identity
		snapshot := cloneReadinessProgress(connecting.progress)
		changed := connecting.changed
		done := connecting.done
		c.mu.Unlock()
		if identity.RuntimeBuild != "" {
			if err := progress.adopt(identity, snapshot); err != nil {
				return err
			}
		}
		_, err := withinProgress(ctx, progress, func(waitCtx context.Context) (struct{}, error) {
			select {
			case <-waitCtx.Done():
				return struct{}{}, waitCtx.Err()
			case <-changed:
				return struct{}{}, nil
			case <-done:
				return struct{}{}, nil
			}
		})
		if err != nil {
			return err
		}
		select {
		case <-done:
			c.mu.Lock()
			err := connecting.err
			c.mu.Unlock()
			return err
		default:
		}
	}
}

func (c *ServiceClient) monitorSession(
	session *serviceSession,
	progress *runtimeProgressTracker,
	started chan<- struct{},
) {
	for {
		event, ok, err := tryRuntimeReadiness(session.client, c.config.Client.WireBuild)
		if !ok {
			close(started)
			break
		}
		if !c.observeSession(session, progress, event, err) {
			close(started)
			return
		}
	}
	for {
		event, err := nextRuntimeReadiness(c.ctx, session.client, c.config.Client.WireBuild)
		if !c.observeSession(session, progress, event, err) {
			return
		}
	}
}

func (c *ServiceClient) observeSession(
	session *serviceSession,
	progress *runtimeProgressTracker,
	event runtimeReadinessEvent,
	eventErr error,
) bool {
	if eventErr != nil {
		if event.Progress.State == RuntimeFailed || event.Progress.State == RuntimeDraining {
			_, progressErr := progress.observe(event, false)
			if progressErr != nil {
				eventErr = errors.Join(progressErr, eventErr)
			}
		}
		if isTerminalReadinessFailure(eventErr) {
			c.terminate(eventErr)
		} else {
			c.retire(session, eventErr)
		}
		return false
	}
	_, err := progress.observe(event, false)
	identity, snapshot := progress.current()
	c.mu.Lock()
	if !session.removed {
		session.identity = identity
		session.progress = snapshot
	}
	c.mu.Unlock()
	if err == nil {
		return true
	}
	if isTerminalReadinessFailure(err) {
		c.terminate(err)
	} else {
		c.retire(session, err)
	}
	return false
}

func (c *ServiceClient) terminate(cause error) {
	c.mu.Lock()
	if c.closed || c.terminal != nil {
		c.mu.Unlock()
		return
	}
	c.terminal = cause
	c.cancel()
	closeServiceDone(c.done)
	clients := c.detachSessionsLocked()
	c.mu.Unlock()
	for _, client := range clients {
		_ = client.Abort(cause)
	}
}

func (c *ServiceClient) detachSessionsLocked() []*Client {
	clients := make([]*Client, 0, len(c.sessions))
	for session := range c.sessions {
		session.removed = true
		clients = append(clients, session.client)
	}
	clear(c.sessions)
	c.current = nil
	return clients
}

func closeServiceDone(done chan struct{}) {
	select {
	case <-done:
	default:
		close(done)
	}
}

func (c *ServiceClient) retire(session *serviceSession, cause error) {
	c.mu.Lock()
	cleanup := c.retireLocked(session, cause)
	c.mu.Unlock()
	if cleanup != nil {
		_ = cleanup.Abort(cause)
	}
}

func (c *ServiceClient) retireLocked(session *serviceSession, cause error) *Client {
	if session.removed {
		return nil
	}
	if c.current == session {
		c.current = nil
	}
	session.retired = true
	if session.cause == nil {
		session.cause = cause
	}
	if session.active != 0 {
		return nil
	}
	session.removed = true
	delete(c.sessions, session)
	return session.client
}

func (c *ServiceClient) release(session *serviceSession) {
	c.mu.Lock()
	if session.active <= 0 {
		c.mu.Unlock()
		panic("wire: release inactive service session")
	}
	session.active--
	var cleanup *Client
	var cause error
	if session.retired && session.active == 0 && !session.removed {
		session.removed = true
		delete(c.sessions, session)
		cleanup = session.client
		cause = session.cause
	}
	c.mu.Unlock()
	if cleanup != nil {
		_ = cleanup.Abort(cause)
	}
}

func serviceCallReplayable(result Result, err error, policy ReplayPolicy) bool {
	if !isServiceCallTransition(result, err) {
		return false
	}
	if err == nil {
		return result.Outcome.Replayable()
	}
	switch result.Outcome {
	case PreSendFailure:
		return true
	case PostSendFailure, DeliveryUnknown:
		return policy == ReplayIdempotent
	default:
		return false
	}
}

func isServiceCallTransition(result Result, err error) bool {
	if err != nil {
		if isLocalServiceCallError(result, err) || isServicePeerTerminal(err) {
			return false
		}
		return true
	}
	rejection := result.Rejection()
	return errors.Is(rejection, ErrNotReady) || errors.Is(rejection, ErrDraining)
}

func serviceCallTerminal(ctx context.Context, result Result, err error) error {
	if err != nil {
		if callerErr := ctx.Err(); callerErr != nil && errors.Is(err, callerErr) {
			return nil
		}
		if isLocalServiceCallError(result, err) {
			return nil
		}
		if isServicePeerTerminal(err) {
			return err
		}
		return nil
	}
	rejection := result.Rejection()
	if errors.Is(rejection, ErrBuildMismatch) {
		return rejection
	}
	return nil
}

func isLocalServiceCallError(result Result, err error) bool {
	var openErr *OpenError
	return result.Outcome == PreSendFailure && errors.As(err, &openErr) &&
		openErr.Outcome == PreSendFailure &&
		(errors.Is(err, ErrFrameTooLarge) || errors.Is(err, ErrInvalidFrame))
}

func isServicePeerTerminal(err error) bool {
	return errors.Is(err, ErrInvalidFrame) || errors.Is(err, ErrProtocolVersion) ||
		errors.Is(err, ErrBuildMismatch) || errors.Is(err, ErrHandshake) ||
		errors.Is(err, ErrUntrustedPeer)
}

func isTerminalReadinessFailure(err error) bool {
	return errors.Is(err, ErrRuntimeFailed) || errors.Is(err, ErrReadinessProgress) || errors.Is(err, ErrInvalidFrame) ||
		errors.Is(err, ErrProtocolVersion) || errors.Is(err, ErrBuildMismatch) ||
		errors.Is(err, ErrHandshake) || errors.Is(err, ErrUntrustedPeer)
}
