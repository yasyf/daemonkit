package wire

import (
	"context"
	"errors"
)

// ReadinessBarrier owns product bootstrap between live wire acceptance and
// publication of daemon readiness.
type ReadinessBarrier interface {
	BeforeReady(context.Context) error
	AfterReady(error)
	Published() bool
}

// BootstrapRequest is the immutable authenticated view available to a
// pre-ready route authorizer.
type BootstrapRequest struct {
	Op        Op
	Tenant    string
	Peer      Peer
	WireBuild string
	Payload   []byte
}

// BootstrapAuthorizer authenticates one classifier-protected pre-ready call.
type BootstrapAuthorizer func(context.Context, BootstrapRequest) error

// BootstrapRoute permits one registered business op before readiness only
// after its product authorizer accepts the authenticated peer.
type BootstrapRoute struct {
	Op        Op
	Authorize BootstrapAuthorizer
}

func (s *Server) runReadiness(ctx context.Context, ready func() error) error {
	err := s.readiness.BeforeReady(ctx)
	if err != nil {
		s.readiness.AfterReady(err)
		return err
	}
	if err = ctx.Err(); err != nil {
		s.readiness.AfterReady(err)
		return err
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	s.readiness.AfterReady(nil)
	if !s.readiness.Published() {
		return errors.New("wire: readiness barrier did not publish")
	}
	if err = ready(); err != nil {
		s.readiness.AfterReady(err)
		if s.readiness.Published() {
			return errors.Join(err, errors.New("wire: failed readiness remained published"))
		}
		return err
	}
	return nil
}

func (s *Server) installReadinessContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	s.mu.Lock()
	if s.readinessCancel != nil {
		s.mu.Unlock()
		cancel()
		panic("wire: readiness context installed twice")
	}
	s.readinessCancel = cancel
	s.mu.Unlock()
	return ctx, cancel
}

func (s *Server) clearReadinessContext(cancel context.CancelFunc) {
	cancel()
	s.mu.Lock()
	s.readinessCancel = nil
	s.mu.Unlock()
}

func (s *Server) authorizePreReady(
	ctx context.Context,
	entry entry,
	request BootstrapRequest,
) (func(), error) {
	s.publicationMu.RLock()
	s.mu.Lock()
	readiness := s.readiness
	ready := s.readyPublished
	authorize := s.bootstrapRoutes[request.Op]
	s.mu.Unlock()
	if readiness != nil {
		ready = readiness.Published()
	}
	if ready {
		s.publicationMu.RUnlock()
		return func() {}, nil
	}
	if entry.preReady {
		return s.publicationMu.RUnlock, nil
	}
	if entry.route != routeBusiness || authorize == nil {
		s.publicationMu.RUnlock()
		return nil, ErrNotReady
	}
	if err := authorize(ctx, request); err != nil {
		s.publicationMu.RUnlock()
		return nil, err
	}
	return s.publicationMu.RUnlock, nil
}
