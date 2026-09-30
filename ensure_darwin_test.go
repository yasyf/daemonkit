package daemonkit

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

func TestDaemonAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	program := filepath.Join(home, "bin", "daemon")
	tests := []struct {
		name    string
		daemon  Daemon
		want    launchd.Agent
		refused bool
	}{
		{
			name: "every field derives from the daemon",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Args:    []string{"daemon", "--serve"},
				Log:     filepath.Join(home, "custom.log"),
				Restart: RestartAlways,
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				Args:          []string{"daemon", "--serve"},
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.RestartAlways,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "the shutdown grace is the plist's exit timeout",
			daemon: Daemon{
				Label:    "com.example.ensure",
				Program:  Program{policy: bundled{file: program}},
				Log:      filepath.Join(home, "custom.log"),
				Shutdown: Grace(90 * time.Second),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   90 * time.Second,
			},
		},
		{
			name: "a sub-second grace rounds up rather than cutting the drain short",
			daemon: Daemon{
				Label:    "com.example.ensure",
				Program:  Program{policy: bundled{file: program}},
				Log:      filepath.Join(home, "custom.log"),
				Shutdown: Grace(1500 * time.Millisecond),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   2 * time.Second,
			},
		},
		{
			name: "an unset log sinks to the state directory",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Restart: RestartOnFailure,
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, ".daemonkit", "a", "com.example.ensure", "daemon.log"),
				RestartPolicy: launchd.RestartOnFailure,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "the zero restart never relaunches",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Log:     filepath.Join(home, "custom.log"),
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "custom.log"),
				RestartPolicy: launchd.NoRestart,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "an unknown restart policy is refused",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{policy: bundled{file: program}},
				Restart: Restart(9),
			},
			refused: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent, err := tt.daemon.agent()
			if tt.refused {
				if err == nil {
					t.Fatal("agent() error = nil, want a refusal")
				}
				return
			}
			if err != nil {
				t.Fatalf("agent() error = %v", err)
			}
			if agent.Label != tt.want.Label || agent.Program != tt.want.Program ||
				agent.LogPath != tt.want.LogPath || agent.RestartPolicy != tt.want.RestartPolicy ||
				agent.ExitTimeOut != tt.want.ExitTimeOut {
				t.Fatalf("agent() = %+v, want %+v", agent, tt.want)
			}
			if len(agent.Env) != 1 || agent.Env["PATH"] != AgentPath {
				t.Fatalf("agent() env = %q, want PATH=%q alone", agent.Env, AgentPath)
			}
			if len(agent.Args) != len(tt.want.Args) {
				t.Fatalf("agent() args = %q, want %q", agent.Args, tt.want.Args)
			}
			for i, arg := range tt.want.Args {
				if agent.Args[i] != arg {
					t.Fatalf("agent() args = %q, want %q", agent.Args, tt.want.Args)
				}
			}
		})
	}
}

func TestLaunchctlReportsExitCodesAsAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, code, err := launchctl(ctx, "/bin/sh", "-c", "echo spoke; exit 37")
	if err != nil {
		t.Fatalf("launchctl() error = %v, want an exit code instead", err)
	}
	if code != 37 {
		t.Fatalf("launchctl() code = %d, want 37", code)
	}
	if out != "spoke\n" {
		t.Fatalf("launchctl() out = %q, want %q", out, "spoke\n")
	}
	_, code, err = launchctl(ctx, filepath.Join(t.TempDir(), "absent"))
	if err == nil {
		t.Fatal("launchctl() of a missing binary reported no error")
	}
	if code >= 0 {
		t.Fatalf("launchctl() that never ran reported status %d, want no status at all", code)
	}
}

// launchctlRecorder answers launchctl and records every verb and every target,
// so a test can assert both what the ladder asked launchd to do and that it
// named no label but its own.
type launchctlRecorder struct {
	loaded  bool
	refuse  string
	verbs   []string
	targets []string
}

// TestEnsurePlacesTheProgramOnlyUnderTheStartLock is the second consequence of
// a constructor that writes: the write is decoupled from the one lock that
// serializes every transition of the live daemon, so two launchers racing the
// same fixed path both land bytes and the loser reaches "came up as build X" —
// an error moved() does not name, so Ensure hard-errors against a healthy
// daemon instead of re-observing. Under the lock the loser places nothing and
// waits its turn.
func TestEnsurePlacesTheProgramOnlyUnderTheStartLock(t *testing.T) {
	ladderHome(t)
	label := Label("com.example.race")
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

	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	client := openClient(t, Daemon{Label: label, Program: program})
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := client.Ensure(ctx); err == nil {
		t.Fatal("Ensure() succeeded while another launcher held the start lock")
	}

	target := programPath(t, client.daemon)
	if _, err := os.Stat(target); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the program was placed at %q while another launcher held the start lock (stat: %v)", target, err)
	}
}

func (r *launchctlRecorder) run(_ context.Context, _ string, args ...string) (string, int, error) {
	r.verbs = append(r.verbs, args[0])
	r.targets = append(r.targets, args[len(args)-1])
	switch {
	case args[0] == "print" && !r.loaded:
		return "Could not find service", 3, errors.New("exit status 3")
	case args[0] == r.refuse:
		return args[0] + " failed: 1: Operation not permitted", 1, errors.New("exit status 1")
	}
	return "", 0, nil
}

func ladderHome(t *testing.T) string {
	t.Helper()
	home := shortHome(t)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	return home
}

// signRunnable ad-hoc signs program, because a bare copy of a system binary
// carries a platform-binary signature the kernel honours only on the system
// volume.
func signRunnable(t *testing.T, program string) {
	t.Helper()
	if out, err := exec.Command("/usr/bin/codesign", "--sign", "-", "--force", program).CombinedOutput(); err != nil {
		t.Fatalf("codesign %q: %v\n%s", program, err, out)
	}
}

func programPath(t *testing.T, d Daemon) string {
	t.Helper()
	path, err := d.Program.path(mustElement(t, d.Label))
	if err != nil {
		t.Fatalf("Program.path() error = %v", err)
	}
	return path
}
