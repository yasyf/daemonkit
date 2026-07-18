package drain

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
)

// Liveness is a tri-state death verdict. The zero value is Undetermined: a probe
// timeout, enumeration failure, or unreadable identity is NEVER Dead.
type Liveness int

const (
	// Undetermined means liveness could not be proven either way; every
	// force/reap/adopt gate treats it as "do nothing".
	Undetermined Liveness = iota
	// Alive means the recorded identity matches a live process.
	Alive
	// Dead means a full successful scan proved the process instance gone.
	Dead
)

// String renders a Liveness for logs and test failures.
func (l Liveness) String() string {
	switch l {
	case Undetermined:
		return "undetermined"
	case Alive:
		return "alive"
	case Dead:
		return "dead"
	default:
		return fmt.Sprintf("Liveness(%d)", int(l))
	}
}

// ForcePolicy declares whether a resource spec ever permits force-clearing.
// There is no default that permits force: the zero value is ForcePolicyDefer.
type ForcePolicy int

const (
	// ForcePolicyDefer never forces, regardless of any death verdict.
	ForcePolicyDefer ForcePolicy = iota
	// ForcePolicyConfirmedDead forces only on a proven-dead carcass.
	ForcePolicyConfirmedDead
)

// AllowForce is the single force gate: only ForcePolicyConfirmedDead paired with
// a proven Dead verdict permits force; Undetermined never does.
func AllowForce(p ForcePolicy, l Liveness) bool {
	return p == ForcePolicyConfirmedDead && l == Dead
}

// Assess revalidates a recorded identity against the live process table: gone or
// reused PID is Dead, a matching instance is Alive, a probe failure Undetermined.
func Assess(id proc.Identity) Liveness { return assess(sysProber{}, id) }

func assess(p prober, id proc.Identity) Liveness {
	if id.Boot != "" {
		boot, err := p.boot()
		if err != nil {
			return Undetermined
		}
		// A different boot session proves death outright: no process survives
		// reboot, and linux start stamps are only unique within one boot.
		if boot != id.Boot {
			return Dead
		}
	}
	cur, err := p.probe(id.PID)
	switch {
	case errors.Is(err, proc.ErrNoProcess):
		return Dead
	case err != nil:
		return Undetermined
	case cur.StartTime != id.StartTime:
		return Dead
	default:
		return Alive
	}
}

// TakeoverProof adapts the force gate to daemon.TakeoverConfig.ConfirmedDead:
// ForcePolicyDefer never confirms; ForcePolicyConfirmedDead confirms only when
// the reported PID probes definitively gone. A probe failure is an error.
func TakeoverProof(policy ForcePolicy) func(context.Context, daemon.Health) (bool, error) {
	return takeoverProof(policy, sysProber{})
}

func takeoverProof(policy ForcePolicy, p prober) func(context.Context, daemon.Health) (bool, error) {
	return func(_ context.Context, h daemon.Health) (bool, error) {
		if policy != ForcePolicyConfirmedDead {
			return false, nil
		}
		_, err := p.probe(h.PID)
		if errors.Is(err, proc.ErrNoProcess) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("drain: probe incumbent %d: %w", h.PID, err)
		}
		return false, nil
	}
}
