package daemonkit

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestPreservingStopDoesNotRemoveAnUnsupportedIncumbent(t *testing.T) {
	ladderHome(t)
	d := Daemon{Label: "dkstop-preserving", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second), ShutdownPolicy: PreserveOwned, Program: unrunProgram(t)}
	startControlChild(t, string(d.Label))
	path := installedAgentPlist(t, d.Label)
	client := openClient(t, d)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	control := awaitControl(ctx, t, client)
	defer func() { _ = control.Close(ctx) }()
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	if err := client.Stop(ctx); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("Stop=%v", err)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("launchctl=%v", rec.verbs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("agent changed: %v", err)
	}
	health, err := control.Health(ctx)
	if err != nil || health.Phase != PhaseReady {
		t.Fatalf("incumbent=%+v err=%v", health, err)
	}
}

func TestPreservingStopRequiresMoreThanHostAbsence(t *testing.T) {
	ladderHome(t)
	d := Daemon{Label: "dkstop-preserving-absent", ShutdownPolicy: PreserveOwned}
	path := installedAgentPlist(t, d.Label)
	client := openClient(t, d)
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := client.Stop(ctx); !errors.Is(err, ErrDrainBusy) {
		t.Fatalf("Stop=%v", err)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("launchctl=%v", rec.verbs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("agent changed: %v", err)
	}
}
