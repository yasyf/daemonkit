// Package daemon is the consumer-agnostic lifecycle shell for a detached daemon:
// the successor-initiated takeover ladder, the target-version ensure gate, the
// incumbent-initiated skew watch, an idle-exit timer, and a foreign-key-preserving
// state file. It talks to a running peer only through the exact-protocol Peer
// interface, which each consumer adapts from the generated lifecycle client.
package daemon

import "context"

// State is a peer's coarse health verdict. A temporary secure-hardware outage is
// StateDegraded, never a crash loop.
type State string

const (
	// StateHealthy means the peer is serving normally.
	StateHealthy State = "healthy"
	// StateDegraded means the peer is serving with reduced capability, e.g. a
	// temporary secure-hardware unavailability.
	StateDegraded State = "degraded"
	// StateFailed means the peer cannot serve.
	StateFailed State = "failed"
)

// Health is a peer's lifecycle snapshot. Build orders takeover and Protocol is
// the exact compatibility contract.
type Health struct {
	// Build is the peer's build identity, classified by the version package.
	Build string
	// Protocol is the peer's exact lifecycle protocol version.
	Protocol int
	// PID is the peer's process id, the revalidation anchor for any signal.
	PID int
	// State is the coarse health verdict.
	State State
	// Draining reports whether the peer is shedding work ahead of exit.
	Draining bool
	// Busy reports whether the peer is mid-operation; a busy ResourceOwner is
	// never killed for being older.
	Busy bool
}

// Peer is a running daemon as its successor or a client sees it, adapted by each
// consumer from its own frozen wire client. Every method blocks on I/O and takes
// ctx first.
type Peer interface {
	// Health returns the peer's current snapshot.
	Health(ctx context.Context) (Health, error)
	// Shutdown asks the peer to exit.
	Shutdown(ctx context.Context) error
	// Handoff asks the peer to release its socket for a successor.
	Handoff(ctx context.Context) error
}
