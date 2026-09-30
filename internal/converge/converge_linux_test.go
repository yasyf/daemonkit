package converge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/supervise"
)

func servingNone(context.Context) (wire.HealthReport, proc.Identity, error) {
	return wire.HealthReport{}, proc.Identity{}, nil
}

func recordsNone(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }

// TestObserveFailsWithNoSupervisorToAsk pins Applied to a supervisor's own
// answer. A label with no supervisor is a boundary that could not be consulted,
// and reading it as merely "not applied" is what would send a repair ladder on
// to evict a serving daemon nothing could then replace.
func TestObserveFailsWithNoSupervisorToAsk(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := Observe(ctx, Sources{
		Serving:  servingNone,
		Recorded: recordsNone,
		Service: supervise.Service{
			Label: "com.example.observed", Program: "/bin/true", LogPath: "/tmp/observed.log",
			RestartPolicy: supervise.NoRestart,
		},
	})
	if !errors.Is(err, supervise.ErrNoSupervisor) {
		t.Fatalf("Observe() = %v, want supervise.ErrNoSupervisor", err)
	}
}

func TestObserveRefusesMissingObservers(t *testing.T) {
	for name, sources := range map[string]Sources{
		"no serving observer": {Recorded: recordsNone},
		"no record observer":  {Serving: servingNone},
	} {
		if _, err := Observe(t.Context(), sources); err == nil {
			t.Errorf("%s: Observe() error = nil, want a refusal", name)
		}
	}
}
