// Package daemon is the consumer-agnostic lifecycle shell for a detached daemon:
// the successor-initiated takeover ladder, the target-version ensure gate, the
// incumbent-initiated skew watch, an idle-exit timer, and a foreign-key-preserving
// state file. It talks to a running peer only through the Peer interface, which
// each consumer adapts from its own frozen wire client, and it infers capability
// from Health.Features bits alone — never from a version compare or a trial call.
package daemon

import "context"

// FeatureHandoff is the Health.Features bit that advertises socket handoff; its
// presence is the only signal that gates the takeover handoff path.
const FeatureHandoff = "handoff"

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

// Health is a peer's lifecycle snapshot. Features is the sole source of
// capability truth.
type Health struct {
	// Version is the peer's build version string, classified by the version package.
	Version string
	// PID is the peer's process id, the revalidation anchor for any signal.
	PID int
	// State is the coarse health verdict.
	State State
	// Draining reports whether the peer is shedding work ahead of exit.
	Draining bool
	// Busy reports whether the peer is mid-operation; a busy ResourceOwner is
	// never killed for being older.
	Busy bool
	// Features are the advertised capability bits.
	Features []string
}

// HasFeature reports whether name is among the advertised Features.
func (h Health) HasFeature(name string) bool {
	for _, f := range h.Features {
		if f == name {
			return true
		}
	}
	return false
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
