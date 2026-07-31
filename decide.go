package daemonkit

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/wire"
)

// Action is what Ensure did to make the wanted build serve.
type Action uint8

const (
	actionInvalid Action = iota
	// ActionNothing means the wanted build was already serving and ready, and
	// its LaunchAgent was already exactly applied.
	ActionNothing
	// ActionStarted means nothing was serving and the daemon was started.
	ActionStarted
	// ActionUpgraded means a different build was evicted and the wanted one
	// took its place.
	ActionUpgraded
	// ActionRestarted means the wanted build was serving unserviceably — or its
	// LaunchAgent had drifted — and was replaced by a fresh instance.
	ActionRestarted
	// actionObserve is the transitional verdict: the incumbent is starting or
	// draining and no decision is knowable yet. Ensure re-observes; it is never
	// published on an Ensured.
	actionObserve
)

// String names the action for a diagnostic.
func (a Action) String() string {
	switch a {
	case ActionNothing:
		return "nothing"
	case ActionStarted:
		return "started"
	case ActionUpgraded:
		return "upgraded"
	case ActionRestarted:
		return "restarted"
	case actionObserve:
		return "observe"
	default:
		return fmt.Sprintf("Action(%d)", uint8(a))
	}
}

// decide is the repair ladder's whole judgment, and it is pure: one observed
// Health and the build that must be serving in, one Action out. No I/O, no
// clock, no state — every branch is reached by a table test.
//
// The order is load-bearing. An incomplete identity is refused before anything
// is compared, because a daemon that cannot name itself cannot be reasoned
// about. A build that is not the wanted one is then replaced whatever phase it
// is in, so an obsolete daemon is never waited on for a readiness that would
// not settle the upgrade. Only past that does the phase decide: a failed
// runtime is replaced, a ready one is left alone, and a starting or draining
// one is transitional — nothing to conclude yet.
func decide(observed Health, want string) (Action, error) {
	if !observed.complete() {
		return actionInvalid, fmt.Errorf(
			"daemonkit: incumbent identity is incomplete: phase=%d protocol=%d build=%q generation=%d",
			observed.Phase, observed.Protocol, observed.Build, observed.Generation,
		)
	}
	if observed.Build != want {
		return ActionUpgraded, nil
	}
	if observed.Phase == PhaseFailed {
		return ActionRestarted, nil
	}
	if observed.Phase == PhaseReady {
		return ActionNothing, nil
	}
	return actionObserve, nil
}

// complete reports whether the observation names an incumbent at all: a phase
// this build understands, the exact transport it speaks, a build stamp, and
// the instance name every proof is pinned to.
func (h Health) complete() bool {
	switch h.Phase {
	case PhaseStarting, PhaseReady, PhaseDraining, PhaseFailed:
	default:
		return false
	}
	return h.Protocol == wire.ProtocolVersion && h.Build != "" && h.Generation != 0
}
