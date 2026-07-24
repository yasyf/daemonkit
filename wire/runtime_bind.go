package wire

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

type sessionStopPreparation struct {
	role             trust.PeerRole
	stopSession      proc.StopSessionID
	preparationNonce proc.StopPreparationNonce
	target           proc.Identity
	runtime          RuntimeIdentity
	protocol         int
}

type runtimeControl interface {
	Health(context.Context) (daemon.Health, error)
	Lifecycle() daemon.LifecycleView
	Shutdown(context.Context) error
}

func (s *Server) bindRuntime(
	build string,
	control runtimeControl,
	stopStore *proc.FileStore,
	observations []ObservationRoute,
) error {
	if control == nil {
		return errors.New("wire: runtime control is required")
	}
	if build == "" {
		return errors.New("wire: runtime build is required")
	}
	if stopStore == nil {
		return errors.New("wire: stop control store is required")
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("wire: trust verifier executable: %w", err)
	}
	observation, err := observationHandlers(observations, s.maxFrame())
	if err != nil {
		return err
	}
	health, err := control.Health(context.Background())
	if err != nil {
		return fmt.Errorf("wire: runtime health identity: %w", err)
	}
	if health.ProcessGeneration == "" {
		return errors.New("wire: runtime process generation is required")
	}
	if health.RuntimeBuild != build {
		return fmt.Errorf(
			"wire: runtime health build %q does not match configured build %q", health.RuntimeBuild, build,
		)
	}
	lifecycleView := control.Lifecycle()
	lifecycle, ok := lifecycleView.(*daemon.Lifecycle)
	if !ok || lifecycle == nil {
		return errors.New("wire: runtime lifecycle is required")
	}
	prepareStopHandler := func(ctx context.Context, req Request) (any, error) {
		var message stopControlPrepareRequest
		if err := decodeStrict(req.Payload, &message); err != nil {
			return nil, fmt.Errorf("wire: decode stop preparation: %w", err)
		}
		if message.Version != 1 || message.ControlRole == "" {
			return nil, errors.New("wire: invalid stop preparation request")
		}
		health, err := control.Health(ctx)
		if err != nil {
			return nil, err
		}
		target, err := currentStopControlIdentity()
		if err != nil {
			return nil, fmt.Errorf("wire: capture prepared stop target: %w", err)
		}
		if target.PID != health.PID || health.RuntimeBuild != build ||
			health.ProcessGeneration != s.stopTargetProcessGeneration || health.RuntimeProtocol <= 0 {
			return nil, errors.New("wire: runtime identity changed during stop preparation")
		}
		if req.Session == nil || req.Session.s == nil {
			return nil, errors.New("wire: stop preparation session is required")
		}
		var stopSession proc.StopSessionID
		if len(req.Session.s.generation) != len(stopSession) {
			return nil, errors.New("wire: stop preparation session identity is invalid")
		}
		copy(stopSession[:], req.Session.s.generation)
		var nonce proc.StopPreparationNonce
		for nonce == (proc.StopPreparationNonce{}) {
			if _, err := rand.Read(nonce[:]); err != nil {
				return nil, fmt.Errorf("wire: generate stop preparation nonce: %w", err)
			}
		}
		runtimeIdentity := RuntimeIdentity{
			RuntimeBuild: health.RuntimeBuild, ProcessGeneration: health.ProcessGeneration,
		}
		preparation := sessionStopPreparation{
			role: message.ControlRole, stopSession: stopSession, preparationNonce: nonce,
			target: target, runtime: runtimeIdentity, protocol: health.RuntimeProtocol,
		}
		if err := req.Session.s.installStopPreparation(preparation); err != nil {
			return nil, err
		}
		return stopControlPrepareResponse{
			Version: 1, StopSession: stopSession[:], PreparationNonce: nonce[:],
			Target:          newStopControlTarget(target, health.ProcessGeneration),
			RuntimeIdentity: runtimeIdentity,
			RuntimeProtocol: health.RuntimeProtocol,
		}, nil
	}
	stopHandler := func(ctx context.Context, req Request) (any, error) {
		var message stopControlRequest
		if err := decodeStrict(req.Payload, &message); err != nil {
			return nil, fmt.Errorf("wire: decode stop control: %w", err)
		}
		health, err := control.Health(ctx)
		if err != nil {
			return nil, err
		}
		expectedTarget, err := message.Target.identity()
		if err != nil {
			return nil, fmt.Errorf("wire: decode expected stop target: %w", err)
		}
		expectedRuntime := RuntimeIdentity{
			RuntimeBuild: health.RuntimeBuild, ProcessGeneration: health.ProcessGeneration,
		}
		if message.RuntimeIdentity != expectedRuntime || message.RuntimeProtocol != health.RuntimeProtocol {
			return nil, errors.New("wire: prepared runtime identity changed before shutdown")
		}
		target, err := currentStopControlIdentity()
		if err != nil {
			return nil, fmt.Errorf("wire: capture stop target: %w", err)
		}
		if target.PID != health.PID || target != expectedTarget {
			return nil, errors.New("wire: prepared stop target changed before shutdown")
		}
		generation := health.ProcessGeneration
		if generation == "" || generation != s.stopTargetProcessGeneration {
			return nil, errors.New("wire: stop target generation changed before shutdown")
		}
		if req.Session == nil || req.Session.s == nil {
			return nil, errors.New("wire: stop control session is required")
		}
		var authority proc.Record
		err = req.Session.s.consumeStopPreparation(message, func(preparation sessionStopPreparation) error {
			var consumed bool
			authority, consumed, err = s.stopControlStore.ConsumeStopControl(
				ctx, req.Peer.ProcessIdentity(), string(req.Session.s.role), message.OperationID,
				preparation.stopSession, preparation.preparationNonce,
				message.RuntimeProtocol,
				s.stopTargetProcessGeneration, time.Now(),
			)
			if err != nil {
				return fmt.Errorf("wire: consume stop control record: %w", err)
			}
			if !consumed {
				return errors.New("wire: no exact unexpired stop control authority")
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if authority.TargetProcessGeneration != generation || authority.RuntimeProtocol != health.RuntimeProtocol {
			return nil, errors.New("wire: stop control verifier returned another runtime identity")
		}
		response := stopControlResponse{
			Version: 1, Target: newStopControlTarget(target, generation),
			RuntimeBuild: health.RuntimeBuild, RuntimeProtocol: health.RuntimeProtocol,
		}
		if err := control.Shutdown(ctx); err != nil {
			return nil, err
		}
		response.Stopped = true
		return response, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("wire: runtime cannot be bound after Serve")
	}
	if s.stopControlStore != nil {
		return errors.New("wire: runtime is already bound")
	}
	if s.handlers == nil {
		s.handlers = make(map[Op]entry)
	}
	if _, exists := s.handlers[stopControlOp]; exists {
		return fmt.Errorf("wire: stop control op %q is already registered", stopControlOp)
	}
	if _, exists := s.handlers[stopControlPrepareOp]; exists {
		return fmt.Errorf("wire: stop preparation op %q is already registered", stopControlPrepareOp)
	}
	if _, exists := s.handlers[runtimeReadinessSubscribeOp]; exists {
		return fmt.Errorf("wire: runtime readiness op %q is already registered", runtimeReadinessSubscribeOp)
	}
	if _, exists := s.handlers[runtimeReceiptOp]; exists {
		return fmt.Errorf("wire: runtime receipt op %q is already registered", runtimeReceiptOp)
	}
	for op := range observation {
		if _, exists := s.handlers[op]; exists {
			return fmt.Errorf("wire: observation op %q is already registered", op)
		}
	}
	s.handlers[stopControlPrepareOp] = entry{class: classControl, route: routeStopPrepare, h: prepareStopHandler}
	s.handlers[stopControlOp] = entry{class: classControl, route: routeStop, h: stopHandler}
	s.handlers[runtimeReadinessSubscribeOp] = entry{
		class: classControl, route: routeLifecycle,
		h: func(_ context.Context, req Request) (any, error) {
			var message runtimeReadinessSubscribeRequest
			if err := decodeStrict(req.Payload, &message); err != nil {
				return nil, fmt.Errorf("wire: decode runtime readiness: %w", err)
			}
			if message.Protocol != ProtocolVersion {
				return nil, fmt.Errorf("%w: readiness protocol=%d", ErrProtocolVersion, message.Protocol)
			}
			if req.Session == nil {
				return nil, errors.New("wire: readiness session is required")
			}
			if err := s.subscribeReadiness(req.Session.s); err != nil {
				return nil, err
			}
			return runtimeReadinessSubscribeResponse{Protocol: ProtocolVersion}, nil
		},
	}
	runtimeIdentity := RuntimeIdentity{
		RuntimeBuild: build, ProcessGeneration: health.ProcessGeneration,
	}
	s.handlers[runtimeReceiptOp] = entry{
		class: classControl, route: routeLifecycle,
		h: func(_ context.Context, req Request) (any, error) {
			var message runtimeReceiptRequest
			if err := decodeStrict(req.Payload, &message); err != nil {
				return nil, fmt.Errorf("wire: decode runtime receipt: %w", err)
			}
			if message.Protocol != ProtocolVersion {
				return nil, fmt.Errorf("%w: runtime receipt protocol=%d", ErrProtocolVersion, message.Protocol)
			}
			return runtimeReceiptResponse{
				Protocol: ProtocolVersion, RuntimeIdentity: runtimeIdentity,
			}, nil
		},
	}
	for op, handler := range observation {
		s.handlers[op] = entry{
			class: classControl, route: routeObservation, h: handler,
		}
	}
	s.trustExecutable = executable
	s.stopControlStore = stopStore
	s.stopTargetProcessGeneration = health.ProcessGeneration
	s.runtimeBuild = build
	s.processGeneration = health.ProcessGeneration
	s.hasObservations = true
	s.lifecycle = lifecycle
	return nil
}

func (s *Server) authorizeStopPreparation(role trust.PeerRole, payload []byte) error {
	var message stopControlPrepareRequest
	if err := decodeStrict(payload, &message); err != nil {
		return fmt.Errorf("wire: decode stop preparation authorization: %w", err)
	}
	if message.Version != 1 || message.ControlRole == "" {
		return errors.New("wire: invalid stop preparation request")
	}
	if role != message.ControlRole || !s.trustPolicy.AllowsStop(role) {
		return ErrPermissionDenied
	}
	return nil
}

func (s *Server) authorizeStopControl(
	ctx context.Context,
	session *session,
	peer Peer,
	role trust.PeerRole,
	payload []byte,
) (context.Context, error) {
	var message stopControlRequest
	if err := decodeStrict(payload, &message); err != nil {
		return ctx, fmt.Errorf("wire: decode stop control authorization: %w", err)
	}
	if message.Version != 1 || message.OperationID == "" || strings.TrimSpace(message.OperationID) != message.OperationID ||
		len(message.OperationID) > 256 || message.RuntimeIdentity.RuntimeBuild == "" ||
		message.RuntimeIdentity.ProcessGeneration == "" || message.RuntimeProtocol <= 0 {
		return ctx, errors.New("wire: invalid stop control request")
	}
	if _, err := message.Target.identity(); err != nil {
		return ctx, fmt.Errorf("wire: invalid stop control target: %w", err)
	}
	if !s.trustPolicy.AllowsStop(role) {
		return ctx, ErrPermissionDenied
	}
	if session == nil || session.peer.ProcessIdentity() != peer.ProcessIdentity() || session.role != role {
		return ctx, ErrPermissionDenied
	}
	if err := session.validateStopPreparation(message); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (s *session) installStopPreparation(preparation sessionStopPreparation) error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if s.stopPreparation != nil {
		return errors.New("wire: stop preparation is already active on this session")
	}
	s.stopPreparation = &preparation
	return nil
}

func (s *session) validateStopPreparation(message stopControlRequest) error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	_, err := s.matchStopPreparation(message)
	return err
}

func (s *session) consumeStopPreparation(
	message stopControlRequest,
	consume func(sessionStopPreparation) error,
) error {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	preparation, err := s.matchStopPreparation(message)
	if err != nil {
		return err
	}
	if err := consume(preparation); err != nil {
		return err
	}
	s.stopPreparation = nil
	return nil
}

func (s *session) matchStopPreparation(message stopControlRequest) (sessionStopPreparation, error) {
	if err := s.ctx.Err(); err != nil {
		return sessionStopPreparation{}, err
	}
	if s.stopPreparation == nil {
		return sessionStopPreparation{}, errors.New("wire: no active stop preparation on this session")
	}
	preparation := *s.stopPreparation
	target, err := message.Target.identity()
	if err != nil {
		return sessionStopPreparation{}, fmt.Errorf("wire: invalid stop control target: %w", err)
	}
	if len(message.StopSession) != len(preparation.stopSession) ||
		len(message.PreparationNonce) != len(preparation.preparationNonce) ||
		string(message.StopSession) != string(preparation.stopSession[:]) ||
		string(message.PreparationNonce) != string(preparation.preparationNonce[:]) ||
		preparation.role != s.role || target != preparation.target ||
		message.RuntimeIdentity != preparation.runtime || message.RuntimeProtocol != preparation.protocol {
		return sessionStopPreparation{}, errors.New("wire: stop control differs from session preparation")
	}
	return preparation, nil
}

func (s *Server) authorizeLifecycleControl(op Op, role trust.PeerRole) error {
	allowed := false
	switch op {
	case runtimeReceiptOp:
		allowed = s.trustPolicy.AllowsReceipt(role)
	case runtimeReadinessSubscribeOp:
		allowed = s.trustPolicy.AllowsReadiness(role)
	}
	if !allowed {
		return ErrPermissionDenied
	}
	return nil
}
