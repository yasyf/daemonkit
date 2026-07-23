package wire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/version"
)

type stopAuthorityContextKey struct{}

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
	stopHandler := func(ctx context.Context, req Request) (any, error) {
		var message stopControlRequest
		if err := decodeStrict(req.Payload, &message); err != nil {
			return nil, fmt.Errorf("wire: decode stop control: %w", err)
		}
		health, err := control.Health(ctx)
		if err != nil {
			return nil, err
		}
		authority, ok := ctx.Value(stopAuthorityContextKey{}).(proc.Record)
		if !ok {
			return nil, errors.New("wire: stop control authority is absent")
		}
		if authority.TargetProcessGeneration != s.stopTargetProcessGeneration {
			return nil, errors.New("wire: stop control targets another runtime generation")
		}
		if authority.RuntimeProtocol != health.RuntimeProtocol {
			return nil, fmt.Errorf("wire: stop control runtime protocol got %d, want %d", authority.RuntimeProtocol, health.RuntimeProtocol)
		}
		target, err := currentStopControlIdentity()
		if err != nil {
			return nil, fmt.Errorf("wire: capture stop target: %w", err)
		}
		if target.PID != health.PID {
			return nil, errors.New("wire: stop target differs from runtime health")
		}
		generation := health.ProcessGeneration
		if generation == "" || generation != authority.TargetProcessGeneration || generation != s.stopTargetProcessGeneration {
			return nil, errors.New("wire: stop target generation changed before shutdown")
		}
		response := stopControlResponse{
			Version: 1, Target: newStopControlTarget(target, generation),
			RuntimeBuild: health.RuntimeBuild, RuntimeProtocol: health.RuntimeProtocol,
		}
		if authority.Intent == string(StopIntentUpgrade) && !version.Newer(authority.RuntimeBuild, health.RuntimeBuild) {
			return response, nil
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

func (s *Server) authorizeStopControl(ctx context.Context, peer Peer, role trust.PeerRole, payload []byte) (context.Context, error) {
	var message stopControlRequest
	if err := decodeStrict(payload, &message); err != nil {
		return ctx, fmt.Errorf("wire: decode stop control authorization: %w", err)
	}
	if message.Version != 1 {
		return ctx, errors.New("wire: invalid stop control request")
	}
	if !s.trustPolicy.AllowsStop(role) {
		return ctx, ErrPermissionDenied
	}
	authority, consumed, err := s.stopControlStore.ConsumeStopControl(
		ctx, peer.ProcessIdentity(), string(role), s.stopTargetProcessGeneration, time.Now(),
	)
	if err != nil {
		return ctx, fmt.Errorf("wire: consume stop control record: %w", err)
	}
	if !consumed {
		return ctx, errors.New("wire: no exact unexpired stop control authority")
	}
	if authority.TargetProcessGeneration != s.stopTargetProcessGeneration {
		return ctx, errors.New("wire: stop control verifier returned another runtime generation")
	}
	return context.WithValue(ctx, stopAuthorityContextKey{}, authority), nil
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
