package daemonkit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/supervise"
)

// signRunnable has nothing to do on linux: the kernel runs a copied binary as
// readily as the original.
func signRunnable(*testing.T, string) {}

// supervisedDaemon declares a daemon whose program is a private copy of this
// test binary, and makes the supervisor's environment — which the service
// inherits — the one that turns that copy into a serving control child. The
// copy is what keeps the inventory honest: this process runs the original, and
// a daemon declared on the original could never be proven absent.
func supervisedDaemon(t *testing.T, label Label) (Daemon, string) {
	t.Helper()
	shortHome(t)
	body, err := os.ReadFile(selfPath(t))
	if err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(realPath(t, t.TempDir()), "daemon")
	if err := os.WriteFile(program, body, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(controlChildEnv, "1")
	t.Setenv(controlChildLabel, string(label))
	return Daemon{
		Label:    label,
		Program:  Program{policy: bundled{file: program}},
		Schemas:  []Schema{"test.v1"},
		Shutdown: Grace(5 * time.Second),
		Restart:  RestartOnFailure,
	}, program
}

// superviseInBackground runs the label's supervisor for the life of the test
// and waits until it answers.
func superviseInBackground(t *testing.T, d Daemon) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Supervise(ctx, d.Label) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Supervise() = %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("Supervise never returned")
		}
	})
	agent, err := d.agent()
	if err != nil {
		t.Fatalf("agent() = %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		verifyCtx, cancelVerify := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := supervise.Verify(verifyCtx, agent)
		cancelVerify()
		if err == nil {
			return
		}
		if !errors.Is(err, supervise.ErrNoSupervisor) || time.Now().After(deadline) {
			t.Fatalf("the supervisor never answered: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDaemonAgent(t *testing.T) {
	shortHome(t)
	program := filepath.Join(realPath(t, t.TempDir()), "daemon")
	d := Daemon{
		Label:    "com.example.agent",
		Program:  Program{policy: bundled{file: program}},
		Args:     []string{"serve"},
		Restart:  RestartOnFailure,
		Shutdown: Grace(2500 * time.Millisecond),
	}
	got, err := d.agent()
	if err != nil {
		t.Fatalf("agent() = %v", err)
	}
	if got.Label != "com.example.agent" || got.Program != program || len(got.Args) != 1 || got.Args[0] != "serve" {
		t.Fatalf("agent() = %+v, want the daemon's own label, program, and args", got)
	}
	if got.RestartPolicy != supervise.RestartOnFailure {
		t.Fatalf("RestartPolicy = %d, want RestartOnFailure", got.RestartPolicy)
	}
	if got.ExitTimeOut != 3*time.Second {
		t.Fatalf("ExitTimeOut = %v, want the shutdown grace rounded up to whole seconds", got.ExitTimeOut)
	}
	if got.LogPath != mustElement(t, d.Label).state().LogPath() {
		t.Fatalf("LogPath = %q, want the state directory's daemon.log", got.LogPath)
	}
	if got.Env != nil {
		t.Fatalf("Env = %v, want the service to inherit its supervisor's environment unchanged", got.Env)
	}
	for policy, want := range map[Restart]supervise.RestartPolicy{
		RestartNever: supervise.NoRestart, RestartOnFailure: supervise.RestartOnFailure, RestartAlways: supervise.RestartAlways,
	} {
		if mapped, err := policy.supervised(); err != nil || mapped != want {
			t.Errorf("Restart(%d).supervised() = %d, %v, want %d", policy, mapped, err, want)
		}
	}
	if _, err := Restart(9).supervised(); err == nil {
		t.Error("an unknown restart policy was mapped")
	}
}

// TestEnsureRefusesWithNoSupervisorBeforeTouchingTheDaemon is the
// workspace-owned contract at the verb a launcher calls. With no supervisor to
// start a replacement, Ensure must not evict what is serving: it refuses at its
// first observation and the live daemon keeps serving.
func TestEnsureRefusesWithNoSupervisorBeforeTouchingTheDaemon(t *testing.T) {
	d, _ := supervisedDaemon(t, "dknosup")
	child := startControlChild(t, string(d.Label))
	ctx := bounded(t, 60*time.Second)
	client := openClient(t, d)
	warmup := awaitControl(ctx, t, client)
	if err := warmup.Close(ctx); err != nil {
		t.Fatalf("Close() = %v", err)
	}

	if _, err := client.Ensure(bounded(t, 10*time.Second)); !errors.Is(err, supervise.ErrNoSupervisor) {
		t.Fatalf("Ensure() with no supervisor = %v, want supervise.ErrNoSupervisor", err)
	}
	control := awaitControl(ctx, t, client)
	health, err := control.Health(ctx)
	if err != nil || health.PID != child.Process.Pid || health.Phase != PhaseReady {
		t.Fatalf("Health() after the refused Ensure = %+v, %v, want the same daemon still ready", health, err)
	}
	if _, err := control.Drain(ctx, Expect{}); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
}

// TestEnsureAndStopConvergeThroughTheSupervisor is the whole ladder on linux,
// against a real supervisor and a real daemon: a cold start, a settled second
// pass, an upgrade that replaces the program under the running daemon, and a
// stop that leaves nothing serving and nothing applied.
func TestEnsureAndStopConvergeThroughTheSupervisor(t *testing.T) {
	d, program := supervisedDaemon(t, "dksup")
	superviseInBackground(t, d)
	client := openClient(t, d)
	agent, err := d.agent()
	if err != nil {
		t.Fatalf("agent() = %v", err)
	}
	want, err := d.Program.build()
	if err != nil {
		t.Fatalf("build() = %v", err)
	}

	started, err := client.Ensure(bounded(t, 60*time.Second))
	if err != nil {
		t.Fatalf("Ensure() = %v", err)
	}
	if started.Did != ActionStarted || started.After.Phase != PhaseReady || started.After.Build != want {
		t.Fatalf("Ensure() = %+v, want a cold start of build %q", started, want)
	}
	if applied, err := supervise.Verify(bounded(t, 5*time.Second), agent); err != nil || !applied {
		t.Fatalf("Verify() after Ensure = %v, %v, want the daemon's service applied", applied, err)
	}

	settled, err := client.Ensure(bounded(t, 60*time.Second))
	if err != nil {
		t.Fatalf("second Ensure() = %v", err)
	}
	if settled.Did != ActionNothing || settled.After.PID != started.After.PID {
		t.Fatalf("second Ensure() = %+v, want nothing done over pid %d", settled, started.After.PID)
	}

	body, err := os.ReadFile(program)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.WriteFile(program, append(body, "upgraded"...), 0o700); err != nil {
		t.Fatal(err)
	}
	next, err := d.Program.build()
	if err != nil {
		t.Fatalf("build() = %v", err)
	}
	upgraded, err := client.Ensure(bounded(t, 60*time.Second))
	if err != nil {
		t.Fatalf("Ensure() over a replaced program = %v", err)
	}
	if upgraded.Before.Build != want || upgraded.After.Build != next || upgraded.After.PID == started.After.PID {
		t.Fatalf("Ensure() over a replaced program = %+v, want build %q retired for %q in a new process", upgraded, want, next)
	}
	if alive(started.After.PID) {
		t.Fatalf("the retired daemon %d outlived its replacement", started.After.PID)
	}

	if err := client.Stop(bounded(t, 60*time.Second)); err != nil {
		t.Fatalf("Stop() = %v", err)
	}
	if _, err := client.Control(bounded(t, 5*time.Second)); !errors.Is(err, ErrAbsent) {
		t.Fatalf("Control() after Stop = %v, want ErrAbsent", err)
	}
	if alive(upgraded.After.PID) {
		t.Fatalf("the stopped daemon %d is still running", upgraded.After.PID)
	}
	if applied, err := supervise.Verify(bounded(t, 5*time.Second), agent); err != nil || applied {
		t.Fatalf("Verify() after Stop = %v, %v, want nothing applied and the supervisor still answering", applied, err)
	}
	if err := client.Stop(bounded(t, 60*time.Second)); err != nil {
		t.Fatalf("a repeated Stop() = %v, want success", err)
	}
}

// TestSuperviseRefusesALabelNoPathMayBeJoinedFrom holds the label rule at the
// supervisor's own door.
func TestSuperviseRefusesALabelNoPathMayBeJoinedFrom(t *testing.T) {
	shortHome(t)
	for _, label := range []Label{"", "../escape", "a/b", ".hidden"} {
		if err := Supervise(bounded(t, 5*time.Second), label); err == nil {
			t.Errorf("Supervise(%q) ran", label)
		}
	}
}
