// Package runtimeauth connects daemonkit's private runtime constructor to its
// public wire composer without exposing lifecycle construction outside the
// module.
package runtimeauth

import (
	"context"
	"errors"
	"net"
	"sync"

	peeridentity "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

// Admission is the internal runtime-to-wire admission capability.
type Admission func() (any, func(), error)

// ServerExit linearizes transport termination into the private controller
// before the transport settles lifecycle subscribers.
type ServerExit func(error) error

// PeerFencePermit commits one verified child only after the wire handshake is established.
type PeerFencePermit struct {
	Commit   func() (func(), error)
	Rollback func()
}

// PeerFence provisionally verifies one exact armed child identity and its
// explicitly requested session role.
type PeerFence func(context.Context, peeridentity.Identity, trust.PeerRole) (*PeerFencePermit, error)

type stopControlToken struct{}

// StopControlAuthority seals runtime stop preparation inside daemonkit.
type StopControlAuthority struct{ token *stopControlToken }

// NewStopControlAuthority constructs one module-private stop authority.
func NewStopControlAuthority() StopControlAuthority {
	return StopControlAuthority{token: &stopControlToken{}}
}

// Valid reports whether the authority was constructed by daemonkit.
func (a StopControlAuthority) Valid() bool { return a.token != nil }

// SessionServer is the internal authenticated transport boundary.
type SessionServer interface {
	ServeRuntime(context.Context, net.Listener, any, *worker.RuntimeClaim, Admission, Admission, PeerFence, ServerExit, chan<- error) error
	CloseRuntimeIntake() error
	CancelRuntimeRequests()
	SettleRuntimeSessions(context.Context) error
}

// Composition carries an internal transport capability into daemon assembly.
type Composition struct {
	RuntimeConfig any
	TrustPolicy   any
	Server        SessionServer
}

type builder func(config any) (any, error)

var (
	mu      sync.Mutex
	compose builder
)

// Register installs the daemon package's private constructor once.
func Register(candidate func(config any) (any, error)) {
	mu.Lock()
	defer mu.Unlock()
	if candidate == nil || compose != nil {
		panic("runtimeauth: invalid constructor registration")
	}
	compose = candidate
}

// Build invokes the module-private constructor installed by daemon.
func Build(config any) (any, error) {
	mu.Lock()
	candidate := compose
	mu.Unlock()
	if candidate == nil {
		return nil, errors.New("runtimeauth: constructor is not registered")
	}
	return candidate(config)
}
