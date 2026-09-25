//go:build darwin

package launchd

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/internal/maintenance"
)

func TestPreservationUnavailableNeverInvokesLaunchctl(t *testing.T) {
	run := func(context.Context, string, ...string) (string, int, error) {
		t.Fatal("unsupported exclusion invoked launchctl")
		return "", 0, nil
	}
	lease, err := PauseRestarts(t.Context(), run, "com.example.never-touched", 0)
	if lease != nil || !errors.Is(err, maintenance.ErrPreservationUnavailable) {
		t.Fatalf("lease=%v err=%v", lease, err)
	}
	if err := (&Maintenance{}).Restore(t.Context()); !errors.Is(err, maintenance.ErrPreservationUnavailable) {
		t.Fatalf("Restore=%v", err)
	}
}
