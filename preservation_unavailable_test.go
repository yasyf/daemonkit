package daemonkit

import (
	"context"
	"errors"
	"testing"
)

func TestPreservationUnavailableBeforeControlOrLaunchd(t *testing.T) {
	client := &Client{daemon: Daemon{ShutdownPolicy: PreserveOwned}, launchctl: func(context.Context, string, ...string) (string, int, error) {
		t.Fatal("unavailable preservation invoked launchctl")
		return "", 0, nil
	}}
	if _, err := client.Ensure(t.Context()); !errors.Is(err, ErrPreservationUnavailable) {
		t.Fatalf("Ensure=%v", err)
	}
	if err := client.Stop(t.Context()); !errors.Is(err, ErrPreservationUnavailable) {
		t.Fatalf("Stop=%v", err)
	}
	control := &Control{requirePreservation: true}
	if _, err := control.Drain(t.Context(), Expect{}); !errors.Is(err, ErrPreservationUnavailable) {
		t.Fatalf("Drain=%v", err)
	}
}
