// Package daemon is the consumer-agnostic process runtime for a detached
// daemon: exclusive listener ownership, readiness, ordered shutdown, skew
// observation, idle exit, and embedded-process coordination.
package daemon

import "github.com/yasyf/daemonkit/proc"

// State is a runtime's coarse health verdict. A temporary secure-hardware outage is
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

// Health is one runtime generation's process and service snapshot.
type Health struct {
	// RuntimeBuild is the product runtime build identity.
	RuntimeBuild string
	// RuntimeProtocol is the product runtime's exact protocol version.
	RuntimeProtocol int
	// ProcessGeneration is daemonkit's canonical identity for this process execution.
	ProcessGeneration proc.OwnerGeneration
	// PID is the peer's process id, the revalidation anchor for any signal.
	PID int
	// State is the coarse health verdict.
	State State
	// Detail is copied product health context.
	Detail []byte
	// Draining reports whether the peer is shedding work ahead of exit.
	Draining bool
	// Busy reports product work outside the runtime's admission and worker lanes.
	Busy bool
	// Ready reports whether all runtime serving prerequisites were published.
	Ready bool
}
