package daemonkit

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/supervise"
)

// TestStopIsSuccessBeforeTheProgramWasEverPlaced is the uninstall-before-
// install shape on linux: no supervisor answers, nothing is persisted, and the
// Stable program's bytes were never placed. Stop succeeds, succeeds again, and
// places nothing.
func TestStopIsSuccessBeforeTheProgramWasEverPlaced(t *testing.T) {
	shortHome(t)
	d, program := neverPlacedDaemon(t, "dkstopunplaced")
	stopTwiceWithoutPlacing(t, openClient(t, d), program)
}

// TestStopForgetsAPersistedServiceWhoseProgramWasNeverPlaced is the removal
// that follows the proof: a service an earlier Apply persisted, its
// supervisor gone, and a Stop whose program was never placed. The inventory
// clears over the never-placed query, and the removal after it deletes the
// persisted intent under the supervisor's own lock, so a supervisor started
// later resumes nothing.
func TestStopForgetsAPersistedServiceWhoseProgramWasNeverPlaced(t *testing.T) {
	shortHome(t)
	d, program := neverPlacedDaemon(t, "dkstopforget")
	script := filepath.Join(realPath(t, t.TempDir()), "service")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 600\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	persisted, err := Daemon{
		Label:    d.Label,
		Program:  Program{policy: bundled{file: script}},
		Restart:  RestartAlways,
		Shutdown: Grace(time.Second),
	}.agent()
	if err != nil {
		t.Fatalf("agent() = %v", err)
	}
	stop := superviseInBackground(t, d)
	if err := supervise.Apply(bounded(t, 20*time.Second), persisted); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	stop()

	stopTwiceWithoutPlacing(t, openClient(t, d), program)

	superviseInBackground(t, d)
	if applied, err := supervise.Verify(bounded(t, 5*time.Second), persisted); err != nil || applied {
		t.Fatalf("Verify() after Stop = %v, %v, want the persisted service forgotten and the supervisor answering", applied, err)
	}
}
