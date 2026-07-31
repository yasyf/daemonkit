// Package converge observes what is actually on the system. Every fact in a
// World is re-derived from a real boundary at the moment it is read — the
// socket, the durable owner record, launchd's own view of the LaunchAgent — so
// a repair decision is never taken from an intent someone claimed and stored.
package converge

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
)

// Sources are the boundaries Observe re-derives a World from.
type Sources struct {
	// Serving asks whatever is on the socket for the health it publishes, and
	// names the process instance the attach pinned to answer it.
	Serving func(context.Context) (wire.HealthReport, proc.Identity, error)
	// Recorded shared-reads the durable owner record.
	Recorded func(string) (proc.Owner, bool, error)
	// RecordPath is the record file Recorded reads.
	RecordPath string
	// Agent is the desired LaunchAgent whose applied state is observed.
	Agent launchd.Agent
	// Launchctl is how launchd itself is asked about that agent.
	Launchctl launchd.Runner
}

// World is one observation of everything a repair ladder decides from.
type World struct {
	// Health is what the incumbent served; meaningful only when Attach is nil.
	Health wire.HealthReport
	// Pinned is the process instance the attach that read Health pinned, zero
	// when nothing served. It is the strongest thing an observation names: the
	// process that answered on the socket and cleared the serving requirement,
	// where the owner record beside it is same-UID writable.
	Pinned proc.Identity
	// Attach is why nothing served, kept verbatim: absence, drain, and distrust
	// are the caller's vocabulary to classify, never this package's to name.
	Attach error
	// Owner is the recorded incumbent, valid only when Recorded is true.
	Owner proc.Owner
	// Recorded reports whether a well-formed owner record names an incumbent.
	Recorded bool
	// Applied reports whether launchd is already running exactly the desired
	// agent: the byte-exact plist where launchd reads it and launchd itself
	// reporting the job bootstrapped. It is a fact about launchd's
	// configuration, never about the process — a loaded job whose program
	// exited still reads true — but an agent launchd has never heard of does
	// not, however exact its plist.
	Applied bool
}

// Serving reports whether a daemon answered on the socket.
func (w World) Serving() bool { return w.Attach == nil }

// Observed is the process instance this observation named: the peer the attach
// pinned when one served, the recorded incumbent otherwise, and the zero
// identity when neither named one. It is what a later proof correlates an
// unnameable process against, so an observation that named no instance answers
// with a pin no live process can match rather than with nothing at all.
//
// The attach's pin comes first because it is the stronger evidence and the only
// one a live peer guarantees: it answered on the socket and cleared the serving
// requirement, while the record beside it is same-UID writable and may name
// nobody at all — which is exactly what an upgrade that unlinked the daemon's
// bytes and its record leaves behind.
func (w World) Observed() proc.Identity {
	if w.Pinned != (proc.Identity{}) {
		return w.Pinned
	}
	if !w.Recorded {
		return proc.Identity{}
	}
	return w.Owner.Identity()
}

// Observe re-derives a World from s. A boundary that answers with a refusal is
// recorded as that refusal; only a boundary that could not be consulted at all
// fails the observation.
func Observe(ctx context.Context, s Sources) (World, error) {
	if s.Serving == nil || s.Recorded == nil || s.Launchctl == nil {
		return World{}, errors.New("converge: Serving, Recorded, and Launchctl observers are required")
	}
	world := World{}
	world.Health, world.Pinned, world.Attach = s.Serving(ctx)
	owner, recorded, err := s.Recorded(s.RecordPath)
	if err != nil {
		return World{}, fmt.Errorf("converge: observe owner record: %w", err)
	}
	world.Owner, world.Recorded = owner, recorded
	applied, err := launchd.Verify(ctx, s.Launchctl, s.Agent)
	if err != nil {
		return World{}, fmt.Errorf("converge: observe applied agent %q: %w", s.Agent.Label, err)
	}
	world.Applied = applied
	return world, nil
}
