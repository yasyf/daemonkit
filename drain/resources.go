// Package drain is the drain-on-upgrade engine: a draining daemon hands its
// resources to a strictly-newer successor with every row in exactly one journal,
// and never force-clears without ForcePolicyConfirmedDead death proof.
package drain

import "context"

// Key names one consumer resource in the drain journals.
type Key string

// IdleVerdict is a tri-state idle attestation. The zero value is
// IdleUndetermined: only IdleConfirmed lets a sweep proceed.
type IdleVerdict int

const (
	// IdleUndetermined means idleness could not be proven; the sweep aborts.
	IdleUndetermined IdleVerdict = iota
	// IdleConfirmed means the resource is provably idle and may be yielded.
	IdleConfirmed
	// IdleBusy means the resource is in use; the sweep aborts and restores.
	IdleBusy
)

// Fence is a held exclusion over one resource, from busy-check through teardown
// and handoff. fusekit's *lease.Fence satisfies it.
type Fence interface {
	// Held reports whether the fence is still held; false mid-sweep aborts.
	Held() bool
	// Release drops the fence after the journal row has advanced.
	Release() error
}

// Resources is the consumer seam the sweep drives. Every method blocks on I/O,
// takes ctx first, and must honor cancellation.
type Resources interface {
	// Keys enumerates every live resource; an error proves nothing (never
	// treated as zero candidates).
	Keys(ctx context.Context) ([]Key, error)
	// AttestIdle reports whether key is provably idle, checked under the fence.
	AttestIdle(ctx context.Context, key Key) (IdleVerdict, error)
	// Seize takes key's fence exclusively; failure aborts that key's sweep.
	Seize(ctx context.Context, key Key) (Fence, error)
	// Yield tears down the local hold and hands key to the successor, keeping
	// fence held throughout; any error aborts and restores.
	Yield(ctx context.Context, key Key, fence Fence) error
	// Restore reinstates a partially-swept key after an aborted attempt.
	Restore(ctx context.Context, key Key, fence Fence) error
}
