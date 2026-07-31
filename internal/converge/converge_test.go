package converge

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
)

func testAgent(t *testing.T) launchd.Agent {
	t.Helper()
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	return launchd.Agent{
		Label: "com.example.observed",
		// A real symlink-free executable: launchd refuses to consider an agent
		// applied whose program is not one, and every temp dir on darwin sits
		// behind the /var symlink.
		Program:       "/usr/bin/true",
		Args:          []string{"daemon"},
		LogPath:       filepath.Join(home, "daemon.log"),
		RestartPolicy: launchd.NoRestart,
	}
}

// launchctlLoaded answers `launchctl print` as if launchd had the job
// bootstrapped, and every other verb as a success.
func launchctlLoaded(context.Context, string, ...string) (string, int, error) { return "", 0, nil }

// launchctlUnknownLabel answers `launchctl print` with launchd's own "could not
// find service" status: the plist may be on disk, but no job was bootstrapped.
func launchctlUnknownLabel(_ context.Context, _ string, args ...string) (string, int, error) {
	if args[0] == "print" {
		return "Could not find service", 3, errors.New("exit status 3")
	}
	return "", 0, nil
}

func writePlist(t *testing.T, agent launchd.Agent, body []byte, perm os.FileMode) {
	t.Helper()
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	if err := os.WriteFile(path, body, perm); err != nil {
		t.Fatalf("write plist: %v", err)
	}
}

func servingNone(context.Context) (wire.HealthReport, error) { return wire.HealthReport{}, nil }

func recordsNone(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }

func TestObserveKeepsTheAttachRefusalVerbatim(t *testing.T) {
	agent := testAgent(t)
	refusal := errors.New("nothing is listening")
	world, err := Observe(t.Context(), Sources{
		Serving:   func(context.Context) (wire.HealthReport, error) { return wire.HealthReport{}, refusal },
		Recorded:  recordsNone,
		Agent:     agent,
		Launchctl: launchctlLoaded,
	})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !errors.Is(world.Attach, refusal) {
		t.Fatalf("Attach = %v, want %v", world.Attach, refusal)
	}
	if world.Serving() {
		t.Fatal("Serving() = true for a refused attach")
	}
	if !reflect.DeepEqual(world.Health, wire.HealthReport{}) {
		t.Fatalf("Health = %+v, want zero", world.Health)
	}
}

func TestObserveReportsTheServedHealthAndOwnerRecord(t *testing.T) {
	agent := testAgent(t)
	report := wire.HealthReport{Phase: wire.PhaseReady, Protocol: wire.ProtocolVersion, Generation: 7, PID: 42, Build: "b"}
	owner := proc.Owner{PID: 42, Start: 3, Boot: 9, Generation: 7, Build: "b"}
	world, err := Observe(t.Context(), Sources{
		Serving:    func(context.Context) (wire.HealthReport, error) { return report, nil },
		Recorded:   func(string) (proc.Owner, bool, error) { return owner, true, nil },
		RecordPath: "/records",
		Agent:      agent,
		Launchctl:  launchctlLoaded,
	})
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if !world.Serving() {
		t.Fatalf("Serving() = false, attach = %v", world.Attach)
	}
	if !reflect.DeepEqual(world.Health, report) {
		t.Fatalf("Health = %+v, want %+v", world.Health, report)
	}
	if !world.Recorded || world.Owner != owner {
		t.Fatalf("Owner = %+v recorded = %v, want %+v true", world.Owner, world.Recorded, owner)
	}
}

func TestObservePassesTheRecordPathToTheReader(t *testing.T) {
	agent := testAgent(t)
	var seen string
	if _, err := Observe(t.Context(), Sources{
		Serving: servingNone,
		Recorded: func(path string) (proc.Owner, bool, error) {
			seen = path
			return proc.Owner{}, false, nil
		},
		RecordPath: "/state/daemon.records",
		Agent:      agent,
		Launchctl:  launchctlLoaded,
	}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if seen != "/state/daemon.records" {
		t.Fatalf("record path = %q, want %q", seen, "/state/daemon.records")
	}
}

func TestObserveFailsWhenTheRecordCannotBeRead(t *testing.T) {
	agent := testAgent(t)
	unreadable := errors.New("record file is corrupt")
	_, err := Observe(t.Context(), Sources{
		Serving:   servingNone,
		Recorded:  func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, unreadable },
		Agent:     agent,
		Launchctl: launchctlLoaded,
	})
	if !errors.Is(err, unreadable) {
		t.Fatalf("Observe() error = %v, want %v", err, unreadable)
	}
}

func TestObserveRequiresEveryObserver(t *testing.T) {
	agent := testAgent(t)
	tests := []struct {
		name    string
		sources Sources
	}{
		{"no serving observer", Sources{Recorded: recordsNone, Agent: agent, Launchctl: launchctlLoaded}},
		{"no record observer", Sources{Serving: servingNone, Agent: agent, Launchctl: launchctlLoaded}},
		{"no launchctl observer", Sources{Serving: servingNone, Recorded: recordsNone, Agent: agent}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Observe(t.Context(), tt.sources); err == nil {
				t.Fatal("Observe() error = nil, want a refusal")
			}
		})
	}
}

func writeExactPlist(t *testing.T, agent launchd.Agent) {
	t.Helper()
	want, err := agent.Plist()
	if err != nil {
		t.Fatalf("Plist() error = %v", err)
	}
	writePlist(t, agent, want, 0o600)
}

// TestObserveAppliedAsksLaunchd pins Applied to what launchd itself reports.
// The plist triple alone is not the question a repair ladder asks: an agent
// whose plist is byte-exact but whose job launchd never bootstrapped is not
// applied, and reading it as applied is an Ensure that returns "nothing to do"
// over a daemon that will never start.
func TestObserveAppliedAsksLaunchd(t *testing.T) {
	tests := []struct {
		name      string
		write     func(t *testing.T, agent launchd.Agent)
		launchctl launchd.Runner
		want      bool
	}{
		{
			name:      "no plist at all",
			write:     func(*testing.T, launchd.Agent) {},
			launchctl: launchctlLoaded,
			want:      false,
		},
		{
			name:      "byte-exact, 0600, and loaded",
			write:     writeExactPlist,
			launchctl: launchctlLoaded,
			want:      true,
		},
		{
			name:      "byte-exact and 0600 but never bootstrapped",
			write:     writeExactPlist,
			launchctl: launchctlUnknownLabel,
			want:      false,
		},
		{
			name: "drifted bytes",
			write: func(t *testing.T, agent launchd.Agent) {
				want, err := agent.Plist()
				if err != nil {
					t.Fatalf("Plist() error = %v", err)
				}
				writePlist(t, agent, append(want, '\n'), 0o600)
			},
			launchctl: launchctlLoaded,
			want:      false,
		},
		{
			name: "exact bytes at a wider mode",
			write: func(t *testing.T, agent launchd.Agent) {
				want, err := agent.Plist()
				if err != nil {
					t.Fatalf("Plist() error = %v", err)
				}
				writePlist(t, agent, want, 0o644)
			},
			launchctl: launchctlLoaded,
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agent := testAgent(t)
			tt.write(t, agent)
			world, err := Observe(t.Context(), Sources{
				Serving:   servingNone,
				Recorded:  recordsNone,
				Agent:     agent,
				Launchctl: tt.launchctl,
			})
			if err != nil {
				t.Fatalf("Observe() error = %v", err)
			}
			if world.Applied != tt.want {
				t.Fatalf("Applied = %v, want %v", world.Applied, tt.want)
			}
		})
	}
}

func TestObserveAsksLaunchdOnlyAboutTheDesiredLabel(t *testing.T) {
	agent := testAgent(t)
	writeExactPlist(t, agent)
	var targets []string
	if _, err := Observe(t.Context(), Sources{
		Serving:  servingNone,
		Recorded: recordsNone,
		Agent:    agent,
		Launchctl: func(_ context.Context, _ string, args ...string) (string, int, error) {
			targets = append(targets, args[len(args)-1])
			return "", 0, nil
		},
	}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(targets) != 1 || !strings.HasSuffix(targets[0], "/"+agent.Label) {
		t.Fatalf("launchctl targets = %q, want exactly the desired label", targets)
	}
}

func TestObserveFailsWhenLaunchdCannotBeAsked(t *testing.T) {
	agent := testAgent(t)
	writeExactPlist(t, agent)
	unreachable := errors.New("launchctl is not on this machine")
	if _, err := Observe(t.Context(), Sources{
		Serving:   servingNone,
		Recorded:  recordsNone,
		Agent:     agent,
		Launchctl: func(context.Context, string, ...string) (string, int, error) { return "", -1, unreachable },
	}); !errors.Is(err, unreachable) {
		t.Fatalf("Observe() error = %v, want %v", err, unreachable)
	}
}

func TestObserveFailsOnAnUnrenderableAgent(t *testing.T) {
	agent := testAgent(t)
	agent.Program = "relative/daemon"
	if _, err := Observe(t.Context(), Sources{
		Serving:   servingNone,
		Recorded:  recordsNone,
		Agent:     agent,
		Launchctl: launchctlLoaded,
	}); err == nil {
		t.Fatal("Observe() error = nil, want a render refusal")
	}
}
