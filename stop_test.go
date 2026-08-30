package daemonkit

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

func TestStopRequiresDeadline(t *testing.T) {
	client := openClient(t, Daemon{Label: "com.example.stop"})
	if err := client.Stop(context.Background()); err == nil {
		t.Fatal("Stop() without a deadline succeeded")
	}
}

// unrunProgram is an executable no process has ever run, so the
// executable-scoped inventory over it proves a clear table. The directory is
// symlink-resolved because the program tree validation inside an observation
// refuses any symlinked component, /var included.
func unrunProgram(t *testing.T) Program {
	t.Helper()
	file := filepath.Join(realPath(t, t.TempDir()), "never-executed")
	if err := os.WriteFile(file, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	return Program{policy: bundled{file: file}}
}

// installedAgentPlist stands the rendered marker-carrying plist up at the
// label's own LaunchAgents path. It is not byte-exact with the test Daemon's
// own agent, so an observation never reaches launchctl print.
func installedAgentPlist(t *testing.T, label Label) string {
	t.Helper()
	agent := launchd.Agent{
		Label:         string(label),
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), "agent.log"),
		RestartPolicy: launchd.RestartOnFailure,
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	body, err := agent.Plist()
	if err != nil {
		t.Fatalf("Plist() error = %v", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
	return path
}

// TestStopRemovesTheMarkedAgentOfAStoppedDaemon is the absent arm: nothing
// serves, nothing is recorded, the inventory is clear, and the marked
// LaunchAgent goes through Remove's ordinary ownership proof.
func TestStopRemovesTheMarkedAgentOfAStoppedDaemon(t *testing.T) {
	ladderHome(t)
	label := Label("com.example.stopmarked")
	path := installedAgentPlist(t, label)
	client := openClient(t, Daemon{Label: label, Program: unrunProgram(t)})
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	if want := []string{"bootout"}; !slices.Equal(rec.verbs, want) {
		t.Fatalf("verbs = %v, want %v", rec.verbs, want)
	}
	for _, target := range rec.targets {
		if !strings.HasSuffix(target, "/"+string(label)) {
			t.Fatalf("launchctl target %q does not name label %q", target, label)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plist survived Stop: %v", err)
	}
}

// TestStopIsSuccessWithNothingInstalled pins the tolerated case: stopping a
// stopped daemon with no agent is a no-op that asks launchd nothing.
func TestStopIsSuccessWithNothingInstalled(t *testing.T) {
	shortHome(t)
	client := openClient(t, Daemon{Label: "com.example.stopempty", Program: unrunProgram(t)})
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("verbs = %v, want launchd asked nothing", rec.verbs)
	}
}

// TestStopHoldsTheStartLock is the D2/D3 regression: a Stop serialized behind
// another launcher's start lock removes nothing, so it can never take down the
// replacement an in-flight Ensure is starting.
func TestStopHoldsTheStartLock(t *testing.T) {
	ladderHome(t)
	label := Label("com.example.stoplock")
	path := installedAgentPlist(t, label)
	statePaths := paths.Agent(string(label))
	if err := statePaths.EnsureLockDir(); err != nil {
		t.Fatalf("create lock dir: %v", err)
	}
	held, err := flock.Spec{
		Path:     statePaths.StartLockPath(),
		Mode:     flock.Exclusive,
		Deadline: 2 * time.Second,
	}.Acquire(t.Context())
	if err != nil {
		t.Fatalf("hold the start lock: %v", err)
	}
	defer func() { _ = held.Close() }()

	client := openClient(t, Daemon{Label: label, Program: unrunProgram(t)})
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	if err := client.Stop(ctx); err == nil {
		t.Fatal("Stop() succeeded while another launcher held the start lock")
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("verbs = %v, want nothing touched under another launcher's lock", rec.verbs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("plist = %v, want it untouched", err)
	}
}

// TestStopDrainsTheLiveIncumbentAndRemovesItsAgent is the serving arm against
// a real daemon child: the drain verb drives a clean exit, departure is proven
// out of the process table, and only then does the LaunchAgent go.
func TestStopDrainsTheLiveIncumbentAndRemovesItsAgent(t *testing.T) {
	ladderHome(t)
	d := Daemon{Label: "dkstop", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second), Program: unrunProgram(t)}
	child := startControlChild(t, string(d.Label))
	path := installedAgentPlist(t, d.Label)
	client := openClient(t, d)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	warmup := awaitControl(ctx, t, client)
	if err := warmup.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	rec := &launchctlRecorder{}
	client.launchctl = rec.run

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	if err := child.Wait(); err != nil {
		t.Fatalf("child exit = %v, want a clean exit driven by the drain verb", err)
	}
	if want := []string{"bootout"}; !slices.Equal(rec.verbs, want) {
		t.Fatalf("verbs = %v, want %v", rec.verbs, want)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plist survived Stop: %v", err)
	}
}

// TestStopWorksWithoutAProgram is the launcher shape: a Daemon declaring only
// its Label — no Program to render or scan — with a plist at the label but no
// owner record and no listener. Stop proceeds without an inventory to consult
// and the removal's own bootout is what takes down anything launchd still runs
// under the label.
func TestStopWorksWithoutAProgram(t *testing.T) {
	ladderHome(t)
	label := Label("com.example.stopnoprog")
	path := installedAgentPlist(t, label)
	client := openClient(t, Daemon{Label: label})
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	if want := []string{"bootout"}; !slices.Equal(rec.verbs, want) {
		t.Fatalf("verbs = %v, want %v", rec.verbs, want)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plist survived Stop: %v", err)
	}
}

// TestStopSettlesTheRecordedIncumbent is the session-less arm: no listener,
// but the durable owner record names an incumbent, and Stop proves that exact
// identity out of the process table before it succeeds. The Daemon declares no
// Program — the record arm's proof reads the process table by recorded
// identity and needs none.
func TestStopSettlesTheRecordedIncumbent(t *testing.T) {
	d, _ := departedOwnerFixture(t)
	client := openClient(t, d)
	rec := &launchctlRecorder{}
	client.launchctl = rec.run
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("verbs = %v, want no agent to remove", rec.verbs)
	}
}

// TestStopPropagatesAnUntrustedSocketHolder pins what Stop must never do:
// classify a world it cannot trust as absence and take the LaunchAgent down
// over it.
func TestStopPropagatesAnUntrustedSocketHolder(t *testing.T) {
	ladderHome(t)
	d := Daemon{Label: "dkstoptrust", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second), Program: unrunProgram(t)}
	startControlChild(t, string(d.Label))
	path := installedAgentPlist(t, d.Label)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	warmup := awaitControl(ctx, t, openClient(t, d))
	if err := warmup.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	pinned := d
	pinned.Trust.Serving = ServingSigned(Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
	})
	client, err := Open(pinned)
	if err != nil {
		t.Fatalf("Open(pinned) = %v", err)
	}
	rec := &launchctlRecorder{}
	client.launchctl = rec.run

	if err := client.Stop(ctx); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Stop() = %v, want ErrUntrusted for a peer that cannot prove the pinned identity", err)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("verbs = %v, want nothing removed over an untrusted world", rec.verbs)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("plist = %v, want it untouched", err)
	}
}
