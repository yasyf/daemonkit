package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

const stopControlPrepareOp = Op("daemon.control.stop.prepare")

type stopControlPrepareResponse struct {
	Version          uint16            `json:"version"`
	StopSession      []byte            `json:"stop_session"`
	PreparationNonce []byte            `json:"preparation_nonce"`
	Target           stopControlTarget `json:"target"`
	RuntimeIdentity  RuntimeIdentity   `json:"runtime_identity"`
	RuntimeProtocol  int               `json:"runtime_protocol"`
}

type stopControlPrepareRequest struct {
	Version     uint16         `json:"version"`
	ControlRole trust.PeerRole `json:"control_role"`
}

type preparedStopClient interface {
	Call(context.Context, Op, string, []byte) (Result, error)
	Close() error
	PeerWireIdentity() WireIdentity
}

// PreparedStop pins one authenticated runtime and its kernel process identity.
type PreparedStop struct {
	client           preparedStopClient
	target           RuntimeIdentity
	process          proc.Identity
	runtimeProtocol  int
	stopSession      proc.StopSessionID
	preparationNonce proc.StopPreparationNonce

	mu       sync.Mutex
	inflight bool
	sealed   bool
	closed   bool
}

// PrepareStop authenticates and pins one runtime without consuming stop authority.
//
//nolint:contextcheck // Failed preparation closes the private client under its own settlement bound.
func PrepareStop(
	ctx context.Context,
	config RuntimeClientConfig,
	controlRole trust.PeerRole,
	authority runtimeauth.StopControlAuthority,
) (*PreparedStop, error) {
	if !authority.Valid() {
		return nil, errors.New("wire: stop preparation authority is required")
	}
	if config.Client.Dial == nil {
		return nil, errors.New("wire: stop preparation dialer is required")
	}
	if controlRole == "" {
		return nil, errors.New("wire: stop preparation control role is required")
	}
	originalDial := config.Client.Dial
	clientConfig := config.Client
	var kernelIdentity proc.Identity
	var dialed bool
	clientConfig.Dial = func(ctx context.Context) (net.Conn, error) {
		if dialed {
			return nil, errors.New("wire: stop preparation dialed more than once")
		}
		dialed = true
		conn, err := originalDial(ctx)
		if err != nil {
			return nil, err
		}
		unix, ok := conn.(*net.UnixConn)
		if !ok {
			_ = conn.Close()
			return nil, errors.New("wire: stop preparation requires a Unix connection")
		}
		identity, err := peer.FromConn(unix)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("wire: capture stop target peer: %w", err)
		}
		kernelIdentity = identity.ProcessIdentity()
		return conn, nil
	}
	client, err := NewClient(ctx, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("wire: prepare stop connect: %w", err)
	}
	closeClient := true
	defer func() {
		if closeClient {
			_ = client.Close()
		}
	}()
	payload, err := json.Marshal(stopControlPrepareRequest{Version: 1, ControlRole: controlRole})
	if err != nil {
		return nil, fmt.Errorf("wire: encode stop preparation: %w", err)
	}
	result, err := client.Call(ctx, stopControlPrepareOp, "", payload)
	if err != nil {
		return nil, fmt.Errorf("wire: prepare stop request: %w", err)
	}
	if result.Outcome != Delivered || result.Response.Rejected {
		return nil, fmt.Errorf("wire: stop preparation rejected: %w", result.Rejection())
	}
	if result.Response.Err != "" {
		return nil, fmt.Errorf("wire: stop preparation failed: %s", result.Response.Err)
	}
	var response stopControlPrepareResponse
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return nil, fmt.Errorf("wire: decode stop preparation: %w", err)
	}
	process, err := response.Target.identity()
	if err != nil {
		return nil, fmt.Errorf("wire: stop preparation target: %w", err)
	}
	if response.Version != 1 || response.RuntimeProtocol <= 0 ||
		len(response.StopSession) != len(proc.StopSessionID{}) ||
		len(response.PreparationNonce) != len(proc.StopPreparationNonce{}) ||
		response.RuntimeIdentity.RuntimeBuild == "" || response.RuntimeIdentity.ProcessGeneration == (proc.OwnerGeneration{}) ||
		response.RuntimeIdentity.ProcessGeneration != response.Target.ProcessGeneration {
		return nil, errors.New("wire: stop preparation returned an incomplete runtime identity")
	}
	if string(response.StopSession) != string(client.PeerWireIdentity().Session) {
		return nil, errors.New("wire: prepared stop session differs from connected session")
	}
	if process.PID == os.Getpid() {
		return nil, errors.New("wire: stop preparation refuses the caller process")
	}
	if process != kernelIdentity {
		return nil, errors.New("wire: prepared stop target differs from connected kernel peer")
	}
	var stopSession proc.StopSessionID
	copy(stopSession[:], response.StopSession)
	var preparationNonce proc.StopPreparationNonce
	copy(preparationNonce[:], response.PreparationNonce)
	closeClient = false
	return &PreparedStop{
		client: client, target: response.RuntimeIdentity, process: process,
		runtimeProtocol: response.RuntimeProtocol, stopSession: stopSession,
		preparationNonce: preparationNonce,
	}, nil
}

// Target returns the exact runtime identity pinned before dispatch.
func (s *PreparedStop) Target() RuntimeIdentity {
	if s == nil {
		return RuntimeIdentity{}
	}
	return s.target
}

// Process returns the exact kernel process pinned before dispatch.
func (s *PreparedStop) Process() proc.Identity {
	if s == nil {
		return proc.Identity{}
	}
	return s.process
}

// RuntimeProtocol returns the pinned runtime protocol.
func (s *PreparedStop) RuntimeProtocol() int {
	if s == nil {
		return 0
	}
	return s.runtimeProtocol
}

// StopSession returns the exact authenticated session bound by preparation.
func (s *PreparedStop) StopSession() proc.StopSessionID {
	if s == nil {
		return proc.StopSessionID{}
	}
	return s.stopSession
}

// PreparationNonce returns the random session-local preparation identity.
func (s *PreparedStop) PreparationNonce() proc.StopPreparationNonce {
	if s == nil {
		return proc.StopPreparationNonce{}
	}
	return s.preparationNonce
}

// Dispatch consumes one stop authority on the pinned session.
func (s *PreparedStop) Dispatch(ctx context.Context, operationID string) (StopResult, Outcome, error) {
	if s == nil || s.client == nil {
		return StopResult{}, PreSendFailure, errors.New("wire: prepared stop is absent")
	}
	if operationID == "" {
		return StopResult{}, PreSendFailure, errors.New("wire: stop operation ID is required")
	}
	if err := ctx.Err(); err != nil {
		return StopResult{}, PreSendFailure, err
	}
	payload, err := json.Marshal(stopControlRequest{
		Version: 1, OperationID: operationID,
		StopSession: s.stopSession[:], PreparationNonce: s.preparationNonce[:],
		Target:          newStopControlTarget(s.process, s.target.ProcessGeneration),
		RuntimeIdentity: s.target, RuntimeProtocol: s.runtimeProtocol,
	})
	if err != nil {
		return StopResult{}, PreSendFailure, fmt.Errorf("wire: encode stop control request: %w", err)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return StopResult{}, PreSendFailure, errors.New("wire: prepared stop is closed")
	}
	if s.sealed {
		s.mu.Unlock()
		return StopResult{}, PreSendFailure, errors.New("wire: prepared stop was already dispatched")
	}
	if s.inflight {
		s.mu.Unlock()
		return StopResult{}, PreSendFailure, errors.New("wire: prepared stop dispatch is in flight")
	}
	s.inflight = true
	s.mu.Unlock()
	result, err := s.client.Call(ctx, stopControlOp, "", payload)
	s.mu.Lock()
	s.inflight = false
	if result.Outcome != PreSendFailure {
		s.sealed = true
	}
	s.mu.Unlock()
	if err != nil {
		return StopResult{}, result.Outcome, fmt.Errorf("wire: stop control request: %w", err)
	}
	if result.Outcome != Delivered || result.Response.Rejected {
		return StopResult{}, result.Outcome, fmt.Errorf("wire: stop control rejected: %w", result.Rejection())
	}
	if result.Response.Err != "" {
		return StopResult{}, result.Outcome, fmt.Errorf("wire: stop control failed: %s", result.Response.Err)
	}
	var response stopControlResponse
	if err := decodeStrict(result.Response.Payload, &response); err != nil {
		return StopResult{}, result.Outcome, fmt.Errorf("wire: decode stop control response: %w", err)
	}
	process, err := response.Target.identity()
	if err != nil {
		return StopResult{}, result.Outcome, fmt.Errorf("wire: stop control target: %w", err)
	}
	stop := StopResult{
		Process: process, ProcessGeneration: response.Target.ProcessGeneration,
		RuntimeBuild: response.RuntimeBuild, RuntimeProtocol: response.RuntimeProtocol, Stopped: response.Stopped,
	}
	if response.Version != 1 || process != s.process ||
		stop.RuntimeBuild != s.target.RuntimeBuild || stop.ProcessGeneration != s.target.ProcessGeneration ||
		stop.RuntimeProtocol != s.runtimeProtocol {
		return StopResult{}, result.Outcome, errors.New("wire: stop response differs from prepared target")
	}
	return stop, result.Outcome, nil
}

// Close idempotently closes the pinned session.
func (s *PreparedStop) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	return s.client.Close()
}
