package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/version"
)

type stopAuthorityContextKey struct{}

type runtimeControl interface {
	Health(context.Context) (daemon.Health, error)
	Shutdown(context.Context) error
}

func (s *Server) bindRuntime(
	build string,
	classifier ProtectedSessionClassifier,
	reserved int,
	control runtimeControl,
	stopVerifier StopControlVerifier,
	observations []ObservationRoute,
	readiness ReadinessBarrier,
	bootstrapRoutes []BootstrapRoute,
) error {
	if control == nil {
		return errors.New("wire: runtime control is required")
	}
	if build == "" {
		return errors.New("wire: runtime build is required")
	}
	if classifier == nil {
		return errors.New("wire: protected session classifier is required")
	}
	if err := classifier.Validate(); err != nil {
		return fmt.Errorf("wire: protected session classifier: %w", err)
	}
	if stopVerifier == nil {
		return errors.New("wire: stop control verifier is required")
	}
	if err := stopVerifier.Validate(); err != nil {
		return fmt.Errorf("wire: stop control verifier: %w", err)
	}
	if reserved <= 0 || reserved > s.maxSessions() {
		return fmt.Errorf("wire: reserved protected sessions %d outside [1,%d]", reserved, s.maxSessions())
	}
	observation, err := observationHandlers(observations, s.maxFrame())
	if err != nil {
		return err
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
	if s.stopControlVerifier != nil || s.protectedSessionClassifier != nil || s.reservedProtectedSessions != 0 {
		return errors.New("wire: runtime is already bound")
	}
	if s.handlers == nil {
		s.handlers = make(map[Op]entry)
	}
	bootstrap := make(map[Op]BootstrapAuthorizer, len(bootstrapRoutes))
	for _, route := range bootstrapRoutes {
		if readiness == nil {
			return errors.New("wire: bootstrap routes require a readiness barrier")
		}
		if route.Authorize == nil {
			return fmt.Errorf("wire: bootstrap route %q requires an authorizer", route.Op)
		}
		if _, duplicate := bootstrap[route.Op]; duplicate {
			return fmt.Errorf("wire: bootstrap route %q is duplicated", route.Op)
		}
		bootstrap[route.Op] = route.Authorize
	}
	if _, exists := s.handlers[stopControlOp]; exists {
		return fmt.Errorf("wire: stop control op %q is already registered", stopControlOp)
	}
	for op := range observation {
		if _, exists := s.handlers[op]; exists {
			return fmt.Errorf("wire: observation op %q is already registered", op)
		}
	}
	s.handlers[stopControlOp] = entry{class: classControl, route: routeStop, h: stopHandler}
	for op, handler := range observation {
		s.handlers[op] = entry{
			class: classControl, route: routeObservation, h: handler,
			preReady: observationAvailableBeforeReady(observations, op),
		}
	}
	s.protectedSessionClassifier = classifier
	s.reservedProtectedSessions = reserved
	s.stopControlVerifier = stopVerifier
	health, err := control.Health(context.Background())
	if err != nil {
		return fmt.Errorf("wire: runtime health identity: %w", err)
	}
	if health.ProcessGeneration == "" {
		return errors.New("wire: runtime process generation is required")
	}
	s.stopTargetProcessGeneration = health.ProcessGeneration
	s.hasObservations = len(observation) != 0
	s.readiness = readiness
	s.bootstrapRoutes = bootstrap
	return nil
}

func (s *Server) authorizeStopControl(ctx context.Context, peer Peer, payload []byte) (context.Context, error) {
	var message stopControlRequest
	if err := decodeStrict(payload, &message); err != nil {
		return ctx, fmt.Errorf("wire: decode stop control authorization: %w", err)
	}
	if message.Version != 1 {
		return ctx, errors.New("wire: invalid stop control request")
	}
	authority, err := s.stopControlVerifier.VerifyStopControl(ctx, peer, s.stopTargetProcessGeneration)
	if err != nil {
		return ctx, err
	}
	if authority.TargetProcessGeneration != s.stopTargetProcessGeneration {
		return ctx, errors.New("wire: stop control verifier returned another runtime generation")
	}
	return context.WithValue(ctx, stopAuthorityContextKey{}, authority), nil
}
