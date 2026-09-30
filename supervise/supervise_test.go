//go:build linux

package supervise

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"golang.org/x/sys/unix"
)

const (
	// supervisorRoleEnv makes a spawned copy of this test binary run one
	// supervisor and exit instead of re-entering the suite — the re-exec
	// fork-bomb guard scripts/test.sh backstops.
	supervisorRoleEnv = "DAEMONKIT_SUPERVISE_TEST_LABEL"
	testThrottle      = 50 * time.Millisecond
)

func TestMain(m *testing.M) {
	if name := os.Getenv(supervisorRoleEnv); name != "" {
		if err := run(context.Background(), name, testThrottle); err != nil {
			fmt.Fprintf(os.Stderr, "supervisor child: %v\n", err)
			os.Exit(71)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// testHome relocates every daemonkit path under a short private home, short
// because the control socket has to fit sun_path.
func testHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", fmt.Sprintf("dks-%d-", os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv(realhome.EnvOverride, home)
	return home
}

func bounded(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// supervised runs one in-process supervisor for name and stops it, proving a
// clean return, when the test ends.
func supervised(t *testing.T, name string) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, name, testThrottle) }()
	stopped := false
	stop = func() error {
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(30 * time.Second):
			return errors.New("the supervisor never returned")
		}
	}
	t.Cleanup(func() {
		if !stopped {
			if err := stop(); err != nil {
				t.Errorf("supervisor returned %v", err)
			}
		}
	})
	awaitSupervisor(t, name)
	return stop
}

func awaitSupervisor(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := call(bounded(t, 5*time.Second), name, request{Op: opPrint})
		if err == nil {
			return
		}
		if !errors.Is(err, ErrNoSupervisor) || time.Now().After(deadline) {
			t.Fatalf("the supervisor for %q never answered: %v", name, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// script is a service program that appends its pid to a file each time it
// starts, then runs body.
func script(t *testing.T, body string) (program, pids string) {
	t.Helper()
	dir := t.TempDir()
	program, pids = filepath.Join(dir, "service"), filepath.Join(dir, "pids")
	source := "#!/bin/sh\necho $$ >> " + pids + "\n" + body + "\n"
	if err := os.WriteFile(program, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	return program, pids
}

func service(t *testing.T, name, program string, policy RestartPolicy) Service {
	t.Helper()
	return Service{
		Label:         name,
		Program:       program,
		LogPath:       filepath.Join(t.TempDir(), "service.log"),
		RestartPolicy: policy,
		ExitTimeOut:   2 * time.Second,
	}
}

// starts waits for the service to have recorded at least want starts and
// returns every pid it recorded.
func starts(t *testing.T, pids string, want int) []int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var recorded []int
		raw, err := os.ReadFile(pids)
		if err == nil {
			for _, line := range strings.Fields(string(raw)) {
				pid, err := strconv.Atoi(line)
				if err != nil {
					t.Fatalf("pid file line %q: %v", line, err)
				}
				recorded = append(recorded, pid)
			}
		}
		if len(recorded) >= want {
			return recorded
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service recorded %d starts, want %d", len(recorded), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func running(pid int) bool {
	id, err := proc.ProbeIdentity(pid)
	if err != nil {
		return false
	}
	_, settled, err := proc.Observe(id)
	return err == nil && !settled
}

func awaitGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for running(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("pid %d is still running", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServiceValidate(t *testing.T) {
	valid := Service{Label: "com.example.svc", Program: "/bin/true", LogPath: "/tmp/svc.log", RestartPolicy: NoRestart}
	tests := []struct {
		name    string
		mutate  func(*Service)
		wantErr string
	}{
		{"a complete service", func(*Service) {}, ""},
		{"a label that escapes the state root", func(s *Service) { s.Label = "../evil" }, "not canonical"},
		{"no label", func(s *Service) { s.Label = "" }, "not canonical"},
		{"a relative program", func(s *Service) { s.Program = "bin/true" }, "program path"},
		{"an unclean program", func(s *Service) { s.Program = "/bin/../bin/true" }, "program path"},
		{"no program", func(s *Service) { s.Program = "" }, "program path"},
		{"a relative log", func(s *Service) { s.LogPath = "svc.log" }, "log path"},
		{"no restart policy", func(s *Service) { s.RestartPolicy = 0 }, "restart policy is required"},
		{"an unknown restart policy", func(s *Service) { s.RestartPolicy = 9 }, "invalid restart policy"},
		{"a negative exit timeout", func(s *Service) { s.ExitTimeOut = -time.Second }, "exit timeout"},
		{"an argument carrying a NUL", func(s *Service) { s.Args = []string{"a\x00b"} }, "NUL"},
		{"an environment key carrying an equals sign", func(s *Service) { s.Env = map[string]string{"A=B": "c"} }, "environment variable"},
		{"an empty environment key", func(s *Service) { s.Env = map[string]string{"": "c"} }, "environment variable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := valid
			tt.mutate(&candidate)
			err := candidate.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want it to name %q", err, tt.wantErr)
			}
		})
	}
}

func TestServiceEnvironmentLaysItsEntriesOverTheSupervisors(t *testing.T) {
	t.Setenv("DAEMONKIT_SUPERVISE_INHERITED", "from-the-supervisor")
	t.Setenv("DAEMONKIT_SUPERVISE_OVERRIDDEN", "from-the-supervisor")
	env := Service{Env: map[string]string{
		"DAEMONKIT_SUPERVISE_OVERRIDDEN": "from-the-service",
		"DAEMONKIT_SUPERVISE_ADDED":      "from-the-service",
	}}.environment()
	seen := map[string]string{}
	for _, entry := range env {
		key, value, _ := strings.Cut(entry, "=")
		if _, repeated := seen[key]; repeated {
			t.Fatalf("environment repeats %s", key)
		}
		seen[key] = value
	}
	for key, want := range map[string]string{
		"DAEMONKIT_SUPERVISE_INHERITED":  "from-the-supervisor",
		"DAEMONKIT_SUPERVISE_OVERRIDDEN": "from-the-service",
		"DAEMONKIT_SUPERVISE_ADDED":      "from-the-service",
	} {
		if seen[key] != want {
			t.Errorf("%s = %q, want %q", key, seen[key], want)
		}
	}
}

// TestVerbsRefuseWithNoSupervisor is the workspace-owned contract: nothing here
// starts a supervisor, so every verb that needs one says it is missing.
func TestVerbsRefuseWithNoSupervisor(t *testing.T) {
	testHome(t)
	const name = "com.example.unsupervised"
	desired := service(t, name, "/bin/true", NoRestart)
	if err := Apply(bounded(t, 5*time.Second), desired); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("Apply() = %v, want ErrNoSupervisor", err)
	}
	if applied, err := Verify(bounded(t, 5*time.Second), desired); !errors.Is(err, ErrNoSupervisor) || applied {
		t.Fatalf("Verify() = %v, %v, want ErrNoSupervisor and not applied", applied, err)
	}
	if err := Remove(bounded(t, 5*time.Second), name); err != nil {
		t.Fatalf("Remove() of a label nothing was applied to = %v, want success", err)
	}
	if err := Apply(context.Background(), desired); err == nil {
		t.Fatal("Apply() accepted a context without a deadline")
	}
}

// TestApplyStartsTheServiceAndVerifyReportsIt is the happy path and its
// observation: the applied program runs in a session of its own under the
// supervisor, with the environment and working directory a service is owed.
func TestApplyStartsTheServiceAndVerifyReportsIt(t *testing.T) {
	testHome(t)
	const name = "com.example.applied"
	supervised(t, name)
	report := filepath.Join(t.TempDir(), "report")
	program, pids := script(t, "echo \"$PWD $MARKED $1\" > "+report+".tmp; mv "+report+".tmp "+report+"; exec sleep 600")
	desired := service(t, name, program, NoRestart)
	desired.Args = []string{"an-argument"}
	desired.Env = map[string]string{"MARKED": "by-the-service"}

	if applied, err := Verify(bounded(t, 5*time.Second), desired); err != nil || applied {
		t.Fatalf("Verify() before Apply = %v, %v, want not applied", applied, err)
	}
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	pid := starts(t, pids, 1)[0]
	if applied, err := Verify(bounded(t, 5*time.Second), desired); err != nil || !applied {
		t.Fatalf("Verify() after Apply = %v, %v, want applied", applied, err)
	}
	drifted := desired
	drifted.Args = []string{"another-argument"}
	if applied, err := Verify(bounded(t, 5*time.Second), drifted); err != nil || applied {
		t.Fatalf("Verify() of a different specification = %v, %v, want not applied", applied, err)
	}
	identity, err := proc.ProbeIdentity(pid)
	if err != nil {
		t.Fatalf("ProbeIdentity(%d) = %v", pid, err)
	}
	if session, err := unix.Getsid(pid); err != nil || session != pid {
		t.Fatalf("service session = %d, %v, want a session the service leads (%d)", session, err, pid)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		raw, err := os.ReadFile(report)
		if err == nil {
			if got := strings.TrimSpace(string(raw)); got != "/ by-the-service an-argument" {
				t.Fatalf("service reported %q, want its working directory, environment, and argument", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service never reported: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("second Apply() = %v", err)
	}
	if again := starts(t, pids, 1); len(again) != 1 {
		t.Fatalf("an identical Apply restarted a running service: starts = %v", again)
	}
	if _, settled, err := proc.Observe(identity); err != nil || settled {
		t.Fatalf("the running service did not survive an identical Apply: settled = %v, err = %v", settled, err)
	}
}

// TestApplyReplacesADifferentSpecification stops the child of the old
// specification before it starts the new one, so two never run at once.
func TestApplyReplacesADifferentSpecification(t *testing.T) {
	testHome(t)
	const name = "com.example.replaced"
	supervised(t, name)
	program, pids := script(t, "exec sleep 600")
	desired := service(t, name, program, NoRestart)
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	first := starts(t, pids, 1)[0]
	desired.Args = []string{"changed"}
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() of a changed specification = %v", err)
	}
	second := starts(t, pids, 2)[1]
	if running(first) {
		t.Fatalf("the old specification's child %d outlived its replacement %d", first, second)
	}
	if !running(second) {
		t.Fatalf("the new specification's child %d is not running", second)
	}
}

func TestApplyRefusesAProgramThatCannotRun(t *testing.T) {
	testHome(t)
	const name = "com.example.unrunnable"
	supervised(t, name)
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, program := range []string{filepath.Join(dir, "absent"), plain, dir} {
		if err := Apply(bounded(t, 20*time.Second), service(t, name, program, NoRestart)); err == nil {
			t.Errorf("Apply(%q) succeeded, want the unrunnable program refused", program)
		}
	}
	if applied, err := Verify(bounded(t, 5*time.Second), service(t, name, plain, NoRestart)); err != nil || applied {
		t.Fatalf("Verify() = %v, %v, want a refused service left unapplied", applied, err)
	}
}

// TestRestartPolicyDecidesWhatFollowsAnExit is the three policies against the
// two ways a service exits.
func TestRestartPolicyDecidesWhatFollowsAnExit(t *testing.T) {
	tests := []struct {
		name      string
		policy    RestartPolicy
		exit      string
		restarted bool
	}{
		{"always, after a clean exit", RestartAlways, "exit 0", true},
		{"always, after a failure", RestartAlways, "exit 3", true},
		{"on failure, after a clean exit", RestartOnFailure, "exit 0", false},
		{"on failure, after a failure", RestartOnFailure, "exit 3", true},
		{"on failure, after a fatal signal", RestartOnFailure, "kill -KILL $$", true},
		{"never, after a failure", NoRestart, "exit 3", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testHome(t)
			name := "com.example.policy" + strconv.Itoa(i)
			supervised(t, name)
			program, pids := script(t, tt.exit)
			if err := Apply(bounded(t, 20*time.Second), service(t, name, program, tt.policy)); err != nil {
				t.Fatalf("Apply() = %v", err)
			}
			if tt.restarted {
				starts(t, pids, 3)
				return
			}
			first := starts(t, pids, 1)[0]
			awaitGone(t, first)
			time.Sleep(10 * testThrottle)
			if recorded := starts(t, pids, 1); len(recorded) != 1 {
				t.Fatalf("the service was restarted against its policy: starts = %v", recorded)
			}
		})
	}
}

// TestApplyStartsAServiceItsPolicyLeftDown is the kickstart: a service that
// exited cleanly under RestartOnFailure stays down until the next Apply, which
// starts it at once even though the specification never changed.
func TestApplyStartsAServiceItsPolicyLeftDown(t *testing.T) {
	testHome(t)
	const name = "com.example.kickstart"
	supervised(t, name)
	program, pids := script(t, "exit 0")
	desired := service(t, name, program, RestartOnFailure)
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	awaitGone(t, starts(t, pids, 1)[0])
	if applied, err := Verify(bounded(t, 5*time.Second), desired); err != nil || !applied {
		t.Fatalf("Verify() of an applied service whose program exited = %v, %v, want applied", applied, err)
	}
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("second Apply() = %v", err)
	}
	starts(t, pids, 2)
}

// TestRemoveStopsTheServiceAndForgetsIt covers the supervised removal: the
// child and everything it forked are settled, the specification is gone from
// disk, and the supervisor stays up for the next Apply.
func TestRemoveStopsTheServiceAndForgetsIt(t *testing.T) {
	testHome(t)
	const name = "com.example.removed"
	supervised(t, name)
	descendant := filepath.Join(t.TempDir(), "descendant")
	program, pids := script(t, "sleep 600 & echo $! > "+descendant+".tmp; mv "+descendant+".tmp "+descendant+"; wait")
	desired := service(t, name, program, RestartAlways)
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	leader := starts(t, pids, 1)[0]
	var forked int
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		raw, err := os.ReadFile(descendant)
		if err == nil {
			if forked, err = strconv.Atoi(strings.TrimSpace(string(raw))); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service never reported its descendant: %v", err)
		}
	}
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(where.spec()); err != nil {
		t.Fatalf("the applied service was not persisted: %v", err)
	}

	if err := Remove(bounded(t, 20*time.Second), name); err != nil {
		t.Fatalf("Remove() = %v", err)
	}
	if running(leader) || running(forked) {
		t.Fatalf("Remove() returned with the service (%d running: %v) or its descendant (%d running: %v) alive",
			leader, running(leader), forked, running(forked))
	}
	if _, err := os.Stat(where.spec()); !os.IsNotExist(err) {
		t.Fatalf("the removed service is still persisted: %v", err)
	}
	if applied, err := Verify(bounded(t, 5*time.Second), desired); err != nil || applied {
		t.Fatalf("Verify() after Remove = %v, %v, want not applied and the supervisor still answering", applied, err)
	}
	time.Sleep(10 * testThrottle)
	if recorded := starts(t, pids, 1); len(recorded) != 1 {
		t.Fatalf("a removed RestartAlways service was started again: starts = %v", recorded)
	}
	if err := Remove(bounded(t, 20*time.Second), name); err != nil {
		t.Fatalf("a repeated Remove() = %v, want a no-op", err)
	}
}

// TestStoppingTheSupervisorStopsItsServiceAndKeepsItApplied is the workspace
// stopping its own process: the service goes down with it inside its exit
// timeout, and the specification stays so the next supervisor resumes it
// without another Apply.
func TestStoppingTheSupervisorStopsItsServiceAndKeepsItApplied(t *testing.T) {
	testHome(t)
	const name = "com.example.resumed"
	stop := supervised(t, name)
	program, pids := script(t, "exec sleep 600")
	desired := service(t, name, program, RestartOnFailure)
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	first := starts(t, pids, 1)[0]
	if err := stop(); err != nil {
		t.Fatalf("the supervisor returned %v", err)
	}
	if running(first) {
		t.Fatalf("the service %d outlived its supervisor", first)
	}
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(where.socket()); !os.IsNotExist(err) {
		t.Fatalf("the stopped supervisor left its socket behind: %v", err)
	}
	if applied, err := Verify(bounded(t, 5*time.Second), desired); !errors.Is(err, ErrNoSupervisor) || applied {
		t.Fatalf("Verify() with the supervisor stopped = %v, %v, want ErrNoSupervisor", applied, err)
	}

	supervised(t, name)
	second := starts(t, pids, 2)[1]
	if !running(second) {
		t.Fatalf("the resumed service %d is not running", second)
	}
	if applied, err := Verify(bounded(t, 5*time.Second), desired); err != nil || !applied {
		t.Fatalf("Verify() under the next supervisor = %v, %v, want the persisted service applied", applied, err)
	}
}

// TestASecondSupervisorIsRefused is the label's exclusive ownership.
func TestASecondSupervisorIsRefused(t *testing.T) {
	testHome(t)
	const name = "com.example.exclusive"
	supervised(t, name)
	if err := run(bounded(t, 30*time.Second), name, testThrottle); !errors.Is(err, ErrBusy) {
		t.Fatalf("a second supervisor for a held label returned %v, want ErrBusy", err)
	}
	awaitSupervisor(t, name)
}

// TestTheNextSupervisorReclaimsAnOrphanedService kills a supervisor outright.
// Its service keeps running with nobody to stop it, and the supervisor that
// takes the label next must settle that exact process before it starts
// another, from the record the dead one wrote before the service ran.
func TestTheNextSupervisorReclaimsAnOrphanedService(t *testing.T) {
	testHome(t)
	const name = "com.example.orphaned"
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	doomed := exec.Command(executable)
	doomed.Env = append(os.Environ(), supervisorRoleEnv+"="+name)
	doomed.Stderr = os.Stderr
	if err := doomed.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = doomed.Process.Kill()
		_ = doomed.Wait()
	})
	awaitSupervisor(t, name)
	program, pids := script(t, "exec sleep 600")
	desired := service(t, name, program, RestartOnFailure)
	if err := Apply(bounded(t, 20*time.Second), desired); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	orphan := starts(t, pids, 1)[0]
	t.Cleanup(func() { _ = syscall.Kill(orphan, syscall.SIGKILL) })

	if err := doomed.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = doomed.Wait()
	if !running(orphan) {
		t.Fatalf("the service %d died with its supervisor, so this run proved nothing", orphan)
	}
	if err := Apply(bounded(t, 5*time.Second), desired); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("Apply() against a dead supervisor's socket = %v, want ErrNoSupervisor", err)
	}

	supervised(t, name)
	if running(orphan) {
		t.Fatalf("the orphaned service %d survived the next supervisor's reclaim", orphan)
	}
	successor := starts(t, pids, 2)[1]
	if !running(successor) {
		t.Fatalf("the resumed service %d is not running", successor)
	}
}

// TestRemoveWithNoSupervisorForgetsThePersistedService keeps a stopped
// supervisor from resuming what a Stop took down while it was away.
func TestRemoveWithNoSupervisorForgetsThePersistedService(t *testing.T) {
	testHome(t)
	const name = "com.example.forgotten"
	stop := supervised(t, name)
	program, pids := script(t, "exec sleep 600")
	if err := Apply(bounded(t, 20*time.Second), service(t, name, program, RestartAlways)); err != nil {
		t.Fatalf("Apply() = %v", err)
	}
	starts(t, pids, 1)
	if err := stop(); err != nil {
		t.Fatalf("the supervisor returned %v", err)
	}
	if err := Remove(bounded(t, 20*time.Second), name); err != nil {
		t.Fatalf("Remove() with no supervisor = %v", err)
	}
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(where.spec()); !os.IsNotExist(err) {
		t.Fatalf("the removed service is still persisted: %v", err)
	}
	supervised(t, name)
	time.Sleep(10 * testThrottle)
	if recorded := starts(t, pids, 1); len(recorded) != 1 {
		t.Fatalf("the next supervisor resumed a removed service: starts = %v", recorded)
	}
}

// TestTheControlSocketIsPrivate pins the only thing standing between another
// user and the verbs: a 0600 socket inside a 0700 directory.
func TestTheControlSocketIsPrivate(t *testing.T) {
	testHome(t)
	const name = "com.example.private"
	supervised(t, name)
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	socket, err := os.Stat(where.socket())
	if err != nil {
		t.Fatal(err)
	}
	if socket.Mode().Type() != os.ModeSocket || socket.Mode().Perm() != 0o600 {
		t.Fatalf("control socket mode = %v, want a 0600 socket", socket.Mode())
	}
	dir, err := os.Stat(where.dir)
	if err != nil {
		t.Fatal(err)
	}
	if dir.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %v, want 0700", dir.Mode().Perm())
	}
}

// unsupervised opens a label's supervisor without running its loop, so a test
// can stand between the supervisor and what its child's driver reports.
func unsupervised(t *testing.T, name string) *supervisor {
	t.Helper()
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(where.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := proc.OpenStore(bounded(t, proc.SettleGrace), where.records())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &supervisor{label: name, where: where, store: store, throttle: testThrottle}
}

// unproven rewrites the exit the live child's driver publishes into one whose
// reap was never proven, which is what the driver reports when a member of the
// service's session outlives the ladder.
func unproven(s *supervisor) {
	driver, exited := s.exited, make(chan proc.Exit, 1)
	go func() {
		exit := <-driver
		var undetermined proc.Reap
		exit.Reap = undetermined
		exited <- exit
	}()
	s.exited = exited
}

// TestAStopThatCannotProveTheReapStartsNothingAfterIt is the stop path of the
// ownership rule: a service whose exit went unproven still has a record, so a
// retried Apply must not put a second service beside it.
func TestAStopThatCannotProveTheReapStartsNothingAfterIt(t *testing.T) {
	testHome(t)
	const name = "com.example.unprovenstop"
	s := unsupervised(t, name)
	program, pids := script(t, "exec sleep 600")
	first := service(t, name, program, RestartAlways)
	ctx := bounded(t, 30*time.Second)
	if err := s.apply(ctx, first); err != nil {
		t.Fatalf("apply() = %v", err)
	}
	starts(t, pids, 1)
	unproven(s)

	second := first
	second.Args = []string{"changed"}
	for range 2 {
		if err := s.apply(ctx, second); !errors.Is(err, proc.ErrUnsettled) {
			t.Fatalf("apply() over an unproven stop = %v, want ErrUnsettled", err)
		}
	}
	if err := s.start(ctx); !errors.Is(err, proc.ErrUnsettled) {
		t.Fatalf("start() after an unproven stop = %v, want ErrUnsettled", err)
	}
	if err := s.remove(); !errors.Is(err, proc.ErrUnsettled) {
		t.Fatalf("remove() after an unproven stop = %v, want ErrUnsettled", err)
	}
	if s.desired == nil || !s.desired.equal(first) {
		t.Fatalf("desired = %+v, want the first service still applied: the refused one was never persisted", s.desired)
	}
	if recorded := starts(t, pids, 1); len(recorded) != 1 {
		t.Fatalf("the service started %d times, want only the first", len(recorded))
	}
}

// TestAnExitThatCannotProveTheReapEndsTheSupervisor is the restart path: the
// policy asks for another start, and the supervisor returns instead of making
// it.
func TestAnExitThatCannotProveTheReapEndsTheSupervisor(t *testing.T) {
	testHome(t)
	const name = "com.example.unprovenexit"
	s := unsupervised(t, name)
	program, pids := script(t, "sleep 0.2; exit 1")
	desired := service(t, name, program, RestartAlways)
	ctx := bounded(t, 30*time.Second)
	if err := s.apply(ctx, desired); err != nil {
		t.Fatalf("apply() = %v", err)
	}
	unproven(s)
	if err := s.serve(ctx, make(chan verb)); !errors.Is(err, proc.ErrUnsettled) {
		t.Fatalf("serve() after an unproven exit = %v, want ErrUnsettled", err)
	}
	if s.restart != nil {
		t.Fatal("a restart is scheduled over an unproven exit")
	}
	if err := s.start(ctx); !errors.Is(err, proc.ErrUnsettled) {
		t.Fatalf("start() after an unproven exit = %v, want ErrUnsettled", err)
	}
	time.Sleep(4 * testThrottle)
	if recorded := starts(t, pids, 1); len(recorded) != 1 {
		t.Fatalf("the service started %d times, want only the first", len(recorded))
	}
}

// TestASupervisorThatCannotReclaimThePreviousChildStartsNothing is the restart
// half of the ownership rule. The record a previous supervisor left names this
// test process, which no reap ladder will settle, so the next supervisor must
// return before it starts the applied service beside that record.
func TestASupervisorThatCannotReclaimThePreviousChildStartsNothing(t *testing.T) {
	testHome(t)
	const name = "com.example.unreclaimed"
	previous := unsupervised(t, name)
	if _, err := previous.store.Adopt(bounded(t, proc.SettleGrace), os.Getpid()); err != nil {
		t.Fatalf("Adopt() = %v", err)
	}
	if err := previous.store.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	program, pids := script(t, "exec sleep 600")
	data, err := durable.Marshal(service(t, name, program, RestartAlways))
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.WriteFile(previous.where.spec(), data, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := run(bounded(t, 30*time.Second), name, testThrottle); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a supervisor over an unreclaimed record returned %v, want a refusal", err)
	}
	if started, err := os.ReadFile(pids); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the service started beside the unreclaimed record: pids %q, %v", started, err)
	}
}

// TestTheSupervisorRefusesAStateDirectoryOthersCanReach covers a home placed
// somewhere shared: whoever can write the state directory chooses the program
// the supervisor runs.
func TestTheSupervisorRefusesAStateDirectoryOthersCanReach(t *testing.T) {
	testHome(t)
	const name = "com.example.shared"
	where, err := layoutFor(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(where.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(where.dir, 0o770); err != nil {
		t.Fatal(err)
	}
	if err := run(bounded(t, 30*time.Second), name, testThrottle); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a supervisor over a group-writable state directory returned %v, want a refusal", err)
	}
	if _, err := os.Stat(where.records()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the refused supervisor left a record store behind: %v", err)
	}
}

// TestApplyRefusesALogPathThatIsALink keeps service output out of whatever a
// planted link points at.
func TestApplyRefusesALogPathThatIsALink(t *testing.T) {
	testHome(t)
	const name = "com.example.linkedlog"
	supervised(t, name)
	program, pids := script(t, "exec sleep 600")
	desired := service(t, name, program, NoRestart)
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, desired.LogPath); err != nil {
		t.Fatal(err)
	}
	if err := Apply(bounded(t, 20*time.Second), desired); err == nil {
		t.Fatal("Apply() over a log path that is a symlink succeeded, want it refused")
	}
	if started, err := os.ReadFile(pids); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the refused service ran anyway: pids %q, %v", started, err)
	}
	if body, err := os.ReadFile(victim); err != nil || len(body) != 0 {
		t.Fatalf("the link's target holds %q, %v, want it untouched", body, err)
	}
}

// TestTheSupervisorRefusesAMalformedVerb sends what a well-formed client never
// would, and the supervisor answers each with a refusal and stays up.
func TestTheSupervisorRefusesAMalformedVerb(t *testing.T) {
	testHome(t)
	const name = "com.example.malformed"
	supervised(t, name)
	other := Service{Label: "com.example.other", Program: "/bin/true", LogPath: "/tmp/other.log", RestartPolicy: NoRestart}
	for _, verb := range []request{
		{Op: "detonate"},
		{Op: opApply},
		{Op: opPrint, Service: &other},
	} {
		if _, err := call(bounded(t, 5*time.Second), name, verb); err == nil {
			t.Errorf("the supervisor accepted %+v", verb)
		}
	}
	if err := Apply(bounded(t, 5*time.Second), other); !errors.Is(err, ErrNoSupervisor) {
		t.Fatalf("Apply() under another label = %v, want ErrNoSupervisor: no supervisor holds it", err)
	}
	if _, err := call(bounded(t, 5*time.Second), name, request{Op: opApply, Service: &other}); err == nil {
		t.Fatal("the supervisor applied a service naming another label")
	}
	awaitSupervisor(t, name)
}
