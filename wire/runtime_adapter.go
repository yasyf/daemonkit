package wire

import (
	"context"
	"errors"
	"net"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/internal/runtimeauth"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

type runtimeServerAdapter struct {
	server      *Server
	trustPolicy trust.TrustPolicy
}

func (a runtimeServerAdapter) ServeRuntime(
	ctx context.Context,
	listener net.Listener,
	lifecycleValue any,
	trustWorkers *worker.RuntimeClaim,
	admit, admitProtected runtimeauth.Admission,
	peerFence runtimeauth.PeerFence,
	serverExit runtimeauth.ServerExit,
	started chan<- error,
) error {
	lifecycle, ok := lifecycleValue.(*daemon.Lifecycle)
	if !ok || lifecycle == nil {
		return errors.New("wire: invalid internal lifecycle controller")
	}
	a.server.mu.Lock()
	if a.server.started {
		a.server.mu.Unlock()
		return ErrServerStarted
	}
	a.server.trustPolicy = a.trustPolicy
	a.server.mu.Unlock()
	adapt := func(capability runtimeauth.Admission) daemonAdmission {
		return func() (daemon.Publication, func(), error) {
			value, done, err := capability()
			if err != nil {
				return daemon.Publication{}, done, err
			}
			publication, ok := value.(daemon.Publication)
			if !ok {
				if done != nil {
					done()
				}
				return daemon.Publication{}, nil, errors.New("wire: invalid internal publication")
			}
			return publication, done, nil
		}
	}
	return a.server.serveRuntime(ctx, listener, lifecycle, trustWorkers, adapt(admit), adapt(admitProtected), peerFence, serverExit, started)
}

func (a runtimeServerAdapter) CloseRuntimeIntake() error { return a.server.CloseIntake() }

func (a runtimeServerAdapter) CancelRuntimeRequests() { a.server.cancelRequests() }

func (a runtimeServerAdapter) SettleRuntimeSessions(ctx context.Context) error {
	return a.server.settleSessions(ctx)
}
