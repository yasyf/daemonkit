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

// Observe re-derives a World from s. A boundary that answers with a refusal is
// recorded as that refusal; only a boundary that could not be consulted at all
// fails the observation.
func Observe(ctx context.Context, s Sources) (World, error) {
	if s.Serving == nil || s.Recorded == nil || s.Launchctl == nil {
		return World{}, errors.New("converge: Serving, Recorded, and Launchctl observers are required")
	}
	world, err := ObserveRuntime(ctx, s)
	if err != nil {
		return World{}, err
	}
	applied, err := launchd.Verify(ctx, s.Launchctl, s.Agent)
	if err != nil {
		return World{}, fmt.Errorf("converge: observe applied agent %q: %w", s.Agent.Label, err)
	}
	world.Applied = applied
	return world, nil
}
