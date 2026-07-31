package daemonkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/launchd"
)

func TestEnsureRequiresDeadline(t *testing.T) {
	client := Open(Daemon{Label: "com.example.ensure"})
	if _, err := client.Ensure(context.Background()); err == nil {
		t.Fatal("Ensure() without a deadline succeeded")
	}
}

func TestWaitReadyRequiresDeadline(t *testing.T) {
	client := Open(Daemon{Label: "com.example.ensure"})
	if _, err := client.WaitReady(context.Background()); err == nil {
		t.Fatal("WaitReady() without a deadline succeeded")
	}
	control := &Control{}
	if _, err := control.WaitReady(context.Background()); err == nil {
		t.Fatal("Control.WaitReady() without a deadline succeeded")
	}
}

func TestProgramBuildIsTheContentDigestServePublishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon")
	body := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(path, body, 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	got, err := Program{path: path}.build()
	if err != nil {
		t.Fatalf("build() error = %v", err)
	}
	sum := sha256.Sum256(body)
	if want := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("build() = %q, want %q", got, want)
	}
	if _, err := (Program{path: filepath.Join(t.TempDir(), "missing")}).build(); err == nil {
		t.Fatal("build() of a missing program succeeded")
	}
}

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
				Program: Program{path: program},
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
				Program:  Program{path: program},
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
				Program:  Program{path: program},
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
				Program: Program{path: program},
				Restart: RestartOnFailure,
			},
			want: launchd.Agent{
				Label:         "com.example.ensure",
				Program:       program,
				LogPath:       filepath.Join(home, "com.example.ensure", "daemon.log"),
				RestartPolicy: launchd.RestartOnFailure,
				ExitTimeOut:   30 * time.Second,
			},
		},
		{
			name: "the zero restart never relaunches",
			daemon: Daemon{
				Label:   "com.example.ensure",
				Program: Program{path: program},
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
				Program: Program{path: program},
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

func TestRepairWedgedAddressesTheRecordedIdentity(t *testing.T) {
	path, owner := settleFixture(t)
	noRecord := filepath.Join(t.TempDir(), "absent.records")
	readErr := errors.New("record file is corrupt")
	tests := []struct {
		name       string
		recordPath string
		target     incumbent
		readOwner  func(string) (proc.Owner, bool, error)
		probe      func(int) (proc.Identity, error)
		killErr    error
		wantSignal bool
		wantErr    error
	}{
		{
			name:       "a record naming another build is not signalled",
			recordPath: path,
			target:     incumbent{build: "b2", generation: owner.Generation},
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "a record naming another instance is not signalled",
			recordPath: path,
			target:     incumbent{build: owner.Build, generation: owner.Generation + 1},
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrWrongIncumbent,
		},
		{
			name:       "no owner record names nobody to signal",
			recordPath: noRecord,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantErr:    ErrUnrecorded,
		},
		{
			name:       "an unreadable record propagates",
			recordPath: path,
			readOwner:  func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, readErr },
			probe:      proc.ProbeIdentity,
			wantErr:    readErr,
		},
		{
			name:       "a departed incumbent is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      func(int) (proc.Identity, error) { return proc.Identity{}, proc.ErrNoProcess },
		},
		{
			name:       "a reused pid is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe: func(pid int) (proc.Identity, error) {
				return proc.Identity{PID: pid, Start: owner.Start + 1, Boot: owner.Boot}, nil
			},
		},
		{
			name:       "a cross-boot pid is not signalled",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe: func(pid int) (proc.Identity, error) {
				return proc.Identity{PID: pid, Start: owner.Start, Boot: owner.Boot + 1}, nil
			},
		},
		{
			name:       "the matching identity is terminated",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			wantSignal: true,
		},
		{
			name:       "a race to exit is not a failure",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			killErr:    syscall.ESRCH,
			wantSignal: true,
		},
		{
			name:       "a refused signal is reported",
			recordPath: path,
			readOwner:  proc.ReadOwner,
			probe:      proc.ProbeIdentity,
			killErr:    syscall.EPERM,
			wantSignal: true,
			wantErr:    syscall.EPERM,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signalled := 0
			kill := func(pid int, sig syscall.Signal) error {
				signalled++
				if pid != owner.PID {
					t.Fatalf("signalled pid %d, want the recorded %d", pid, owner.PID)
				}
				if sig != syscall.SIGTERM {
					t.Fatalf("signalled %v, want SIGTERM", sig)
				}
				return tt.killErr
			}
			target := tt.target
			if target == (incumbent{}) {
				target = incumbent{build: owner.Build, generation: owner.Generation}
			}
			err := repairWedged(tt.recordPath, target, tt.readOwner, tt.probe, kill)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("repairWedged() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("repairWedged() error = %v", err)
			}
			if want := map[bool]int{true: 1, false: 0}[tt.wantSignal]; signalled != want {
				t.Fatalf("delivered %d signals, want %d", signalled, want)
			}
		})
	}
}

// realPath is a path in the form the kernel reports an executable, which is
// what an executable-scoped inventory compares against.
func realPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", path, err)
	}
	return resolved
}

func selfPath(t *testing.T) string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}
	return self
}

// liveAt answers the executable-scoped inventory as the kernel would for a
// process table holding exactly one process, running live. Every other path is
// clear, so a gate that queries the wrong path is caught rather than covered by
// whatever else the machine happens to be running.
func liveAt(live string) func(string) (proc.Report, error) {
	return func(path string) (proc.Report, error) {
		if path != live {
			return proc.Report{}, nil
		}
		return proc.Report{Matched: []proc.Identity{{PID: 4242, Start: 1, Boot: 1, Executable: path}}}, nil
	}
}

// huskAt answers the inventory as the kernel would for a process table whose
// only live process is one nothing can name: no query path matches it, and its
// pin is all there is to go on.
func huskAt(pin proc.Identity) func(string) (proc.Report, error) {
	return func(string) (proc.Report, error) {
		return proc.Report{Unnameable: []proc.Identity{pin}}, nil
	}
}

func TestInventoryClearProvesAbsenceOverTheProcessTable(t *testing.T) {
	unrun := filepath.Join(t.TempDir(), "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	idle := Open(Daemon{Program: Program{path: unrun}})
	idle.identities = liveAt(realPath(t, selfPath(t)))
	if err := idle.inventoryClear(); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory", err)
	}
	running := Open(Daemon{Program: Program{path: realPath(t, selfPath(t))}})
	if err := running.inventoryClear(); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("inventoryClear() over this very process = %v, want ErrUnsettled", err)
	}
}

// TestInventoryClearHoldsOnlyItsOwnUnnameableHusk is the gate's precision in
// both directions. A live process nothing could name belongs to this daemon
// exactly when it is one the ladder observed on its way here: its own husk — a
// daemon whose binary was unlinked under it — still refuses, while the
// long-lived stranger every machine carries is not this daemon's to answer for
// and cannot brick the gate forever.
func TestInventoryClearHoldsOnlyItsOwnUnnameableHusk(t *testing.T) {
	unrun := filepath.Join(t.TempDir(), "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	husk := proc.Identity{PID: 4242, Start: 77, Boot: 9}
	tests := []struct {
		name     string
		observed []proc.Identity
		wantErr  error
	}{
		{
			name:     "the husk this ladder observed",
			observed: []proc.Identity{husk},
			wantErr:  ErrUnsettled,
		},
		{
			name: "a husk this ladder never observed",
		},
		{
			name:     "an observation naming another instance at the same pid",
			observed: []proc.Identity{{PID: husk.PID, Start: husk.Start + 1, Boot: husk.Boot}},
		},
		{
			name:     "an observation from another boot session",
			observed: []proc.Identity{{PID: husk.PID, Start: husk.Start, Boot: husk.Boot + 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := Open(Daemon{Program: Program{path: unrun}})
			client.identities = huskAt(husk)
			err := client.inventoryClear(tt.observed...)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("inventoryClear() = %v, want a clear inventory", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("inventoryClear() = %v, want %v", err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), "pid 4242 (unnameable, start 77, boot 9)") {
				t.Fatalf("inventoryClear() = %v, want the surviving pin named", err)
			}
		})
	}
}

// TestInventoryClearQueriesTheProgramPathAlone pins the query set to this
// daemon's own program. Staged keys every build by the digest of its own
// bytes under a root every daemonkit consumer shares, so a sibling there is
// not this daemon by construction: a gate that guessed siblings by basename
// held another product's live daemon against this one, and still missed a
// build staged under a different basename. What covers a live build this
// path does not name is the recorded identity, not a guessed path.
func TestInventoryClearQueriesTheProgramPathAlone(t *testing.T) {
	staging := t.TempDir()
	body := []byte("#!/bin/sh\nexit 0\n")
	wanted := filepath.Join(staging, digest(body), "daemon")
	if err := os.MkdirAll(filepath.Dir(wanted), 0o700); err != nil {
		t.Fatalf("stage the wanted build: %v", err)
	}
	if err := os.WriteFile(wanted, body, 0o700); err != nil {
		t.Fatalf("stage the wanted build: %v", err)
	}
	stranger := filepath.Join(staging, strings.Repeat("f", 64), "daemon")
	if err := os.MkdirAll(filepath.Dir(stranger), 0o700); err != nil {
		t.Fatalf("stage the stranger: %v", err)
	}
	if err := os.WriteFile(stranger, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("stage the stranger: %v", err)
	}
	var queried []string
	client := Open(Daemon{Program: Program{path: wanted}})
	client.identities = func(path string) (proc.Report, error) {
		queried = append(queried, path)
		return proc.Report{}, nil
	}
	if err := client.inventoryClear(); err != nil {
		t.Fatalf("inventoryClear() error = %v, want a clear inventory", err)
	}
	if want := []string{realPath(t, wanted)}; !slices.Equal(queried, want) {
		t.Fatalf("inventoryClear queried %q, want %q", queried, want)
	}
}

// TestInventoryClearNeverPassesOnAnUnresolvedProgram is the fail-open gate's
// regression: the kernel reports a fully symlink-resolved executable, so an
// unresolved program path matches nothing and reports a clear inventory for a
// process that is very much running. os.Executable() is exactly such a path on
// darwin — /var/folders/… for a binary the kernel calls /private/var/folders/….
func TestInventoryClearNeverPassesOnAnUnresolvedProgram(t *testing.T) {
	self := selfPath(t)
	linked := filepath.Join(t.TempDir(), "daemon")
	if err := os.Symlink(realPath(t, self), linked); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	tests := []struct {
		name    string
		program string
		wantErr error
	}{
		{"the unresolved path of this very process", self, ErrUnsettled},
		{"a symlink to this very process", linked, ErrUnsettled},
		{"a program that resolves to nothing", filepath.Join(t.TempDir(), "absent"), os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := Open(Daemon{Program: Program{path: tt.program}})
			client.identities = liveAt(realPath(t, self))
			if err := client.inventoryClear(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("inventoryClear() error = %v, want %v", err, tt.wantErr)
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
	if _, _, err := launchctl(ctx, filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("launchctl() of a missing binary reported no error")
	}
}

func TestAttachCadenceDerivesFromTheCallerDeadline(t *testing.T) {
	tests := []struct {
		name    string
		budget  time.Duration
		wantMax time.Duration
	}{
		{"a generous budget is capped", time.Hour, maxObservationCadence},
		{"a tight budget is a fraction of itself", 640 * time.Millisecond, 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tt.budget)
			defer cancel()
			if got := attachCadence(ctx); got <= 0 || got > tt.wantMax {
				t.Fatalf("attachCadence() = %v, want (0, %v]", got, tt.wantMax)
			}
		})
	}
}

func TestAttachCadenceStaysPositiveOnAnExpiredContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	if got := attachCadence(ctx); got <= 0 {
		t.Fatalf("attachCadence() = %v, want a positive interval", got)
	}
}

// ladderHome stands up the passwd home every launchd path derives from, with
// the LaunchAgents directory a plist is written into already present. It is
// short because the daemon socket underneath it must fit sun_path.
func ladderHome(t *testing.T) string {
	t.Helper()
	home := shortHome(t)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatalf("create LaunchAgents dir: %v", err)
	}
	return home
}

// ladderAgent is the one LaunchAgent an Ensure ladder converges on. Its program
// is a real symlink-free executable: launchd refuses to consider an agent
// applied whose program is not one, and every temp dir on darwin sits behind
// the /var symlink.
func ladderAgent(t *testing.T, home string) launchd.Agent {
	t.Helper()
	return launchd.Agent{
		Label:         "com.example.ladder",
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(home, "daemon.log"),
		RestartPolicy: launchd.NoRestart,
		ExitTimeOut:   30 * time.Second,
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

func servedReport(phase wire.Phase, build string, generation uint64) wire.HealthReport {
	return wire.HealthReport{
		Phase:      phase,
		Protocol:   wire.ProtocolVersion,
		Generation: generation,
		PID:        4242,
		Build:      build,
	}
}

type observation struct {
	report wire.HealthReport
	err    error
}

// servingScript answers each observation from the script in turn and repeats
// its last entry, so a test states only the transitions it cares about.
func servingScript(script []observation, seen *int) func(context.Context) (wire.HealthReport, error) {
	return func(context.Context) (wire.HealthReport, error) {
		step := script[min(*seen, len(script)-1)]
		*seen++
		return step.report, step.err
	}
}

func TestSettleObservesUntilTheDecisionIsKnowable(t *testing.T) {
	const want = "wanted"
	tests := []struct {
		name        string
		budget      time.Duration
		script      []observation
		wantAction  Action
		wantServing bool
		wantSeen    int
		wantErr     error
	}{
		{
			name:       "an absent listener is a start",
			script:     []observation{{err: ErrAbsent}},
			wantAction: ActionStarted,
			wantSeen:   1,
		},
		{
			name:       "a draining incumbent is a start",
			script:     []observation{{err: ErrDraining}},
			wantAction: ActionStarted,
			wantSeen:   1,
		},
		{
			name:     "an untrusted server is returned, never waited out",
			script:   []observation{{err: ErrUntrusted}},
			wantSeen: 1,
			wantErr:  ErrUntrusted,
		},
		{
			name:        "the wanted build, ready, is nothing",
			script:      []observation{{report: servedReport(wire.PhaseReady, want, 7)}},
			wantAction:  ActionNothing,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name:        "another build is an upgrade whatever its phase",
			script:      []observation{{report: servedReport(wire.PhaseStarting, "stale", 7)}},
			wantAction:  ActionUpgraded,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name:        "a failed runtime is a restart",
			script:      []observation{{report: servedReport(wire.PhaseFailed, want, 7)}},
			wantAction:  ActionRestarted,
			wantServing: true,
			wantSeen:    1,
		},
		{
			name: "a starting incumbent is re-observed until it settles",
			script: []observation{
				{report: servedReport(wire.PhaseStarting, want, 7)},
				{report: servedReport(wire.PhaseStarting, want, 7)},
				{report: servedReport(wire.PhaseReady, want, 7)},
			},
			wantAction:  ActionNothing,
			wantServing: true,
			wantSeen:    3,
		},
		{
			name:        "an incumbent still transitioning at the end of the share is replaced",
			budget:      40 * time.Millisecond,
			script:      []observation{{report: servedReport(wire.PhaseDraining, want, 7)}},
			wantAction:  ActionRestarted,
			wantServing: true,
		},
		{
			name:     "an incumbent that cannot name itself is refused",
			script:   []observation{{report: servedReport(wire.PhaseReady, want, 0)}},
			wantSeen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := ladderHome(t)
			agent := ladderAgent(t, home)
			seen := 0
			client := Open(Daemon{Label: Label(agent.Label)})
			client.serving = servingScript(tt.script, &seen)
			client.readOwner = func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil }
			client.launchctl = (&launchctlRecorder{}).run
			budget := tt.budget
			if budget == 0 {
				budget = 2 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			world, action, err := client.settle(ctx, want, agent)
			if tt.wantErr != nil || tt.wantAction == actionInvalid {
				if err == nil {
					t.Fatalf("settle() action = %v, want a refusal", action)
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Fatalf("settle() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("settle() error = %v", err)
			}
			if action != tt.wantAction {
				t.Fatalf("settle() action = %v, want %v", action, tt.wantAction)
			}
			if world.Serving() != tt.wantServing {
				t.Fatalf("settle() serving = %v, want %v", world.Serving(), tt.wantServing)
			}
			if tt.wantSeen != 0 && seen != tt.wantSeen {
				t.Fatalf("settle() observed %d times, want %d", seen, tt.wantSeen)
			}
		})
	}
}

// ensureOnceHarness drives the whole ladder against injected boundaries: the
// socket observation, the owner record, the process table, the signal, and
// launchctl. Nothing it stands up can block past the caller's budget.
type ensureOnceHarness struct {
	client      *Client
	agent       launchd.Agent
	launchd     *launchctlRecorder
	signals     []int
	inventoried []string
	settling    bool
}

func newEnsureOnceHarness(t *testing.T, serving []observation, owners func(string) (proc.Owner, bool, error)) *ensureOnceHarness {
	t.Helper()
	home := ladderHome(t)
	unrun := filepath.Join(home, "never-executed")
	if err := os.WriteFile(unrun, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write program: %v", err)
	}
	h := &ensureOnceHarness{launchd: &launchctlRecorder{}}
	h.agent = ladderAgent(t, home)
	h.client = Open(Daemon{Label: Label(h.agent.Label), Program: Program{path: unrun}})
	seen := 0
	h.client.serving = servingScript(serving, &seen)
	h.client.readOwner = owners
	h.client.probe = func(pid int) (proc.Identity, error) { return proc.Identity{PID: pid, Start: 1, Boot: 1}, nil }
	h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
		if h.settling {
			return 0, false, nil
		}
		return proc.ReapAbsent, true, nil
	}
	h.client.identities = func(path string) (proc.Report, error) {
		h.inventoried = append(h.inventoried, path)
		return proc.Report{}, nil
	}
	h.client.kill = func(pid int, sig syscall.Signal) error {
		if sig != syscall.SIGTERM {
			t.Fatalf("delivered %v, want SIGTERM", sig)
		}
		h.signals = append(h.signals, pid)
		return nil
	}
	h.client.launchctl = h.launchd.run
	return h
}

// installPlist writes the exact plist launchd would read for the harness agent,
// which is half of what makes an agent applied; the other half is the recorder
// reporting the job loaded.
func (h *ensureOnceHarness) installPlist(t *testing.T) {
	t.Helper()
	plist, err := h.agent.Plist()
	if err != nil {
		t.Fatalf("Plist() error = %v", err)
	}
	path, err := h.agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
}

// driftPlist installs the harness agent's plist with one byte appended: still
// daemonkit's own job at the label, no longer the bytes this agent renders.
func (h *ensureOnceHarness) driftPlist(t *testing.T) {
	t.Helper()
	h.installPlist(t)
	path, err := h.agent.PlistPath()
	if err != nil {
		t.Fatalf("PlistPath() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		t.Fatalf("write plist: %v", err)
	}
}

func recordedOwner(build string, generation uint64) proc.Owner {
	return proc.Owner{PID: 4242, Start: 1, Boot: 1, Generation: generation, Build: build}
}

func TestEnsureOnceDoesNothingWhenTheWantedBuildIsReadyAndApplied(t *testing.T) {
	h := newEnsureOnceHarness(
		t,
		[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
		func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
	)
	h.launchd.loaded = true
	h.installPlist(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ensured, err := h.client.ensureOnce(ctx, "wanted", h.agent)
	if err != nil {
		t.Fatalf("ensureOnce() error = %v", err)
	}
	if ensured.Did != ActionNothing {
		t.Fatalf("Did = %v, want %v", ensured.Did, ActionNothing)
	}
	if !reflect.DeepEqual(ensured.Before, ensured.After) || ensured.After.Generation != 7 {
		t.Fatalf("Ensured = %+v, want Before restated as After", ensured)
	}
	if !slices.Equal(h.launchd.verbs, []string{"print"}) {
		t.Fatalf("launchctl verbs = %q, want only the applied-state observation", h.launchd.verbs)
	}
	if len(h.signals) != 0 {
		t.Fatalf("delivered %d signals, want none", len(h.signals))
	}
}

// TestEnsureOnceReAppliesUnlessLaunchdRunsExactlyTheAgent drives the ladder's
// whole read of the launchd surface. Applied is launchd's own answer about this
// one label, and every way it comes back false — a plist that was never
// written, one whose bytes drifted, and a byte-exact one launchd never
// bootstrapped — evicts the incumbent and re-applies rather than repairing the
// agent underneath a daemon that is already the wanted build.
func TestEnsureOnceReAppliesUnlessLaunchdRunsExactlyTheAgent(t *testing.T) {
	tests := []struct {
		name   string
		write  func(h *ensureOnceHarness, t *testing.T)
		loaded bool
	}{
		{name: "no plist where launchd reads it", loaded: true},
		{name: "the plist bytes drifted", write: (*ensureOnceHarness).driftPlist, loaded: true},
		{name: "byte-exact but never bootstrapped", write: (*ensureOnceHarness).installPlist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newEnsureOnceHarness(
				t,
				[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
				func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
			)
			h.launchd.loaded = tt.loaded
			h.launchd.refuse = "bootstrap"
			if tt.write != nil {
				tt.write(h, t)
			}
			evicted := 0
			h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
				evicted++
				return proc.ReapAbsent, true, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent)
			if err == nil || !strings.Contains(err.Error(), "daemonkit: apply") {
				t.Fatalf("ensureOnce() error = %v, want the apply refusal", err)
			}
			if !slices.Contains(h.launchd.verbs, "bootstrap") {
				t.Fatalf("launchctl verbs = %q, want the agent re-applied", h.launchd.verbs)
			}
			if evicted == 0 {
				t.Fatal("the incumbent was never proven out of the process table before the apply")
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at an incumbent that left on its own", h.signals)
			}
		})
	}
}

// TestEnsureOnceFailsWhenLaunchdCannotBeAsked pins the ladder to launchd's own
// answer about the label. A launchctl that could not be run at all is not an
// unapplied agent to repair, and reading that silence as drift would evict and
// restart a daemon that is already exactly the wanted build.
func TestEnsureOnceFailsWhenLaunchdCannotBeAsked(t *testing.T) {
	unreachable := errors.New("launchctl is not on this machine")
	h := newEnsureOnceHarness(
		t,
		[]observation{{report: servedReport(wire.PhaseReady, "wanted", 7)}},
		func(string) (proc.Owner, bool, error) { return recordedOwner("wanted", 7), true, nil },
	)
	h.client.launchctl = func(context.Context, string, ...string) (string, int, error) {
		return "", -1, unreachable
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent); !errors.Is(err, unreachable) {
		t.Fatalf("ensureOnce() error = %v, want %v", err, unreachable)
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no eviction on an unobservable agent", h.signals)
	}
}

func TestEnsureOnceTouchesNoLabelButItsOwn(t *testing.T) {
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil },
	)
	h.launchd.refuse = "bootstrap"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent); err == nil {
		t.Fatal("ensureOnce() error = nil, want the apply refusal")
	}
	if len(h.launchd.targets) == 0 {
		t.Fatal("launchctl was never asked anything")
	}
	for i, target := range h.launchd.targets {
		if target != h.agent.Label && !strings.HasSuffix(target, "/"+h.agent.Label) &&
			!strings.HasSuffix(target, "/"+h.agent.Label+".plist") {
			t.Fatalf("launchctl %s named %q, want only %q", h.launchd.verbs[i], target, h.agent.Label)
		}
	}
}

// TestEnsureOnceNeverSignalsARecordItDidNotObserve is the unpinned-settlement
// regression. Nothing serves, so the ladder has only the owner record to act
// on — and that record is same-UID writable. A record rewritten between the
// observation and the settlement names a runtime this Ensure never saw, and
// settling against it would deliver SIGTERM to whatever PID it now carries.
func TestEnsureOnceNeverSignalsARecordItDidNotObserve(t *testing.T) {
	reads := 0
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) {
			reads++
			if reads == 1 {
				return recordedOwner("observed", 7), true, nil
			}
			return recordedOwner("a stranger", 99), true, nil
		},
	)
	h.settling = true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := h.client.ensureOnce(ctx, "wanted", h.agent)
	if !errors.Is(err, ErrWrongIncumbent) {
		t.Fatalf("ensureOnce() error = %v, want ErrWrongIncumbent", err)
	}
	if !moved(err) {
		t.Fatal("the refusal does not re-observe: Ensure would abort instead of re-deciding")
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no signal at a record this Ensure never observed", h.signals)
	}
	if slices.Contains(h.launchd.verbs, "bootstrap") {
		t.Fatalf("launchctl verbs = %q, want no apply past a refused settlement", h.launchd.verbs)
	}
}

func TestEnsureOnceSignalsAWedgedIncumbentAtItsRecordedIdentity(t *testing.T) {
	owner := recordedOwner("observed", 7)
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return owner, true, nil },
	)
	h.settling = true
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("ensureOnce() error = %v, want ErrUnsettled", err)
	}
	if !slices.Equal(h.signals, []int{owner.PID}) {
		t.Fatalf("signalled %v, want exactly the recorded pid %d", h.signals, owner.PID)
	}
}

// TestProveLeavesBudgetPastAWedgedIncumbent pins the proof ladder's shares.
// Observing departure, signalling, and observing again are each a slice of the
// proof's own budget, so an incumbent that never leaves cannot spend the whole
// Ensure on being watched and starve the apply and the readiness wait after it.
func TestProveLeavesBudgetPastAWedgedIncumbent(t *testing.T) {
	owner := recordedOwner("observed", 7)
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return owner, true, nil },
	)
	h.settling = true
	const budget = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("ensureOnce() error = %v, want ErrUnsettled", err)
	}
	if spent := time.Since(start); spent > budget*9/10 {
		t.Fatalf("the proof spent %v of a %v budget, leaving nothing to apply and wait with", spent, budget)
	}
	if !slices.Equal(h.signals, []int{owner.PID}) {
		t.Fatalf("signalled %v, want exactly the recorded pid %d", h.signals, owner.PID)
	}
}

// TestEnsureOnceHoldsTheHuskItObserved pins the husk correlation where it is
// actually reachable. The ladder observed a recorded incumbent and the record
// was gone by the time it settled, so the gate runs with no record left to read
// and the identity the observation named is the whole of what says whose an
// unnameable process is: the husk it names refuses, and one it never saw does
// not brick the gate.
func TestEnsureOnceHoldsTheHuskItObserved(t *testing.T) {
	owner := recordedOwner("observed", 7)
	tests := []struct {
		name    string
		husk    proc.Identity
		wantErr error
	}{
		{
			name:    "the husk the observation named",
			husk:    owner.Identity(),
			wantErr: ErrUnsettled,
		},
		{
			name: "a husk this ladder never observed",
			husk: proc.Identity{PID: owner.PID + 1, Start: owner.Start, Boot: owner.Boot},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			h := newEnsureOnceHarness(
				t,
				[]observation{{err: ErrAbsent}},
				func(string) (proc.Owner, bool, error) {
					reads++
					return owner, reads == 1, nil
				},
			)
			h.client.identities = func(string) (proc.Report, error) {
				return proc.Report{Unnameable: []proc.Identity{tt.husk}}, nil
			}
			h.launchd.refuse = "bootstrap"
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := h.client.ensureOnce(ctx, "wanted", h.agent)
			if tt.wantErr == nil {
				if err == nil || errors.Is(err, ErrUnsettled) {
					t.Fatalf("ensureOnce() = %v, want a cleared gate and the apply refusal past it", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ensureOnce() = %v, want %v", err, tt.wantErr)
			}
			if len(h.signals) != 0 {
				t.Fatalf("signalled %v, want no signal at a record that names nobody", h.signals)
			}
		})
	}
}

// TestEnsureOnceProvesAbsenceByInventoryWhenNothingIsRecorded pins the one path
// with no incumbent to name: absence is the kernel's answer over the executable,
// never a settlement against whatever the record file says next.
func TestEnsureOnceProvesAbsenceByInventoryWhenNothingIsRecorded(t *testing.T) {
	settled := 0
	h := newEnsureOnceHarness(
		t,
		[]observation{{err: ErrAbsent}},
		func(string) (proc.Owner, bool, error) { return proc.Owner{}, false, nil },
	)
	h.client.observe = func(proc.Identity) (proc.Reap, bool, error) {
		settled++
		return proc.ReapAbsent, true, nil
	}
	h.launchd.refuse = "bootstrap"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := h.client.ensureOnce(ctx, "wanted", h.agent); err == nil {
		t.Fatal("ensureOnce() error = nil, want the apply refusal")
	}
	if settled != 0 {
		t.Fatalf("observed the process table %d times, want the inventory instead", settled)
	}
	if want := []string{realPath(t, h.client.daemon.Program.path)}; !slices.Equal(h.inventoried, want) {
		t.Fatalf("inventoried %q, want %q", h.inventoried, want)
	}
	if len(h.signals) != 0 {
		t.Fatalf("signalled %v, want no signal with nobody recorded", h.signals)
	}
}

func TestPinRefusesAnIncompleteIncumbent(t *testing.T) {
	tests := []struct {
		name       string
		build      string
		generation uint64
		refused    bool
	}{
		{name: "both named", build: "b1", generation: 7},
		{name: "no build", generation: 7, refused: true},
		{name: "no generation", build: "b1", refused: true},
		{name: "neither", refused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := pin(tt.build, tt.generation)
			if tt.refused {
				if err == nil {
					t.Fatalf("pin() = %+v, want a refusal", target)
				}
				return
			}
			if err != nil {
				t.Fatalf("pin() error = %v", err)
			}
			if want := (Expect{Build: tt.build, Generation: tt.generation}); target.expect() != want {
				t.Fatalf("expect() = %+v, want %+v", target.expect(), want)
			}
		})
	}
}

func TestMovedNamesTheRacesEnsureReObserves(t *testing.T) {
	control := &Control{pinned: proc.Identity{PID: 11}, generation: 7}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nothing went wrong", err: nil},
		{name: "the expectation disagreed", err: ErrWrongIncumbent, want: true},
		{name: "a wrapped expectation disagreed", err: fmt.Errorf("drain: %w", ErrWrongIncumbent), want: true},
		{name: "the pinned peer moved", err: control.pinnedBy(servedReport(wire.PhaseReady, "b", 9)), want: true},
		{name: "nothing is listening", err: ErrAbsent},
		{name: "the incumbent did not leave", err: ErrUnsettled},
		{name: "the peer is untrusted", err: ErrUntrusted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := moved(tt.err); got != tt.want {
				t.Fatalf("moved(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestEnsureReObservesWhenTheIncumbentMovesUnderIt drives the whole verb over a
// record that names a different runtime on every read: each pass refuses rather
// than evicting a stranger, and Ensure re-observes on its cadence until the
// caller's deadline instead of spinning on the lost race.
func TestEnsureReObservesWhenTheIncumbentMovesUnderIt(t *testing.T) {
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	client := Open(Daemon{Label: Label(agent.Label), Program: Program{path: agent.Program}})
	client.serving = servingScript([]observation{{report: servedReport(wire.PhaseReady, "stale", 7)}}, &seen)
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.kill = func(pid int, _ syscall.Signal) error {
		t.Fatalf("signalled pid %d, want no signal at a runtime Ensure never observed", pid)
		return nil
	}
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := client.Ensure(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ensure() error = %v, want the deadline joined in", err)
	}
	if !moved(err) {
		t.Fatalf("Ensure() error = %v, want the race it kept losing named", err)
	}
	if seen < 2 {
		t.Fatalf("observed %d times, want Ensure to have re-observed", seen)
	}
	if seen > 200 {
		t.Fatalf("observed %d times in 400ms: the retry spins instead of re-observing on a cadence", seen)
	}
}

// TestEnsureOnceReportsTheRaceWhenItsBudgetIsGone drives one pass on a budget
// that is already spent. Every deadline it derives is in the past, so the dial
// never leaves the process and net answers with its own i/o timeout — a fact
// about the clock and none about the incumbent. The pass reports the race it
// lost, not the transport's timeout.
func TestEnsureOnceReportsTheRaceWhenItsBudgetIsGone(t *testing.T) {
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	client := Open(Daemon{Label: Label(agent.Label), Program: Program{path: agent.Program}})
	client.serving = servingScript([]observation{{report: servedReport(wire.PhaseReady, "stale", 7)}}, &seen)
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.kill = func(pid int, _ syscall.Signal) error {
		t.Fatalf("signalled pid %d, want no signal at a runtime this pass never observed", pid)
		return nil
	}
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()
	_, err := client.ensureOnce(ctx, "wanted", agent)
	if !errors.Is(err, ErrWrongIncumbent) || !moved(err) {
		t.Fatalf("ensureOnce() error = %v, want the race the pass lost", err)
	}
}

// TestSpentNamesTheBudgetRatherThanThePeer pins the classification the tail of
// the ladder hangs on: an i/o timeout raised because the deadline was already
// gone is the budget ending, and never an answer about whatever was on the
// socket. The same timeout with budget still left is the peer's.
func TestSpentNamesTheBudgetRatherThanThePeer(t *testing.T) {
	dial := wire.UnixDialer(filepath.Join(t.TempDir(), "absent.sock"))
	expired, cancelExpired := context.WithTimeout(context.Background(), -time.Second)
	defer cancelExpired()
	live, cancelLive := context.WithTimeout(context.Background(), time.Minute)
	defer cancelLive()
	if _, err := dial(expired); !spent(expired, err) {
		t.Fatalf("spent(%v) = false, want the dial that never left the process named as the budget", err)
	}
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"a socket read past its own deadline", expired, os.ErrDeadlineExceeded, true},
		{"the same timeout with budget still left", live, os.ErrDeadlineExceeded, false},
		{"a refusal at the end of the budget", expired, ErrUntrusted, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := spent(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("spent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestEnsureRefusesAPassItCannotFinish pins the tail of the ladder. A slice too
// small to fund a pass funds none of the deadlines the pass derives from it, so
// re-entering on one observes nothing and evicts nothing — Ensure waits out the
// budget it was given and reports the race instead.
func TestEnsureRefusesAPassItCannotFinish(t *testing.T) {
	const budget = 400 * time.Millisecond
	home := ladderHome(t)
	agent := ladderAgent(t, home)
	seen := 0
	var last time.Duration
	client := Open(Daemon{Label: Label(agent.Label), Program: Program{path: agent.Program}})
	client.serving = func(ctx context.Context) (wire.HealthReport, error) {
		seen++
		last = left(ctx)
		return servedReport(wire.PhaseReady, "stale", 7), nil
	}
	client.readOwner = func(string) (proc.Owner, bool, error) { return recordedOwner("a stranger", 99), true, nil }
	client.observe = func(proc.Identity) (proc.Reap, bool, error) { return proc.ReapAbsent, true, nil }
	client.launchctl = (&launchctlRecorder{}).run
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := client.Ensure(ctx); !moved(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ensure() error = %v, want the race joined with the deadline", err)
	}
	if seen < 2 {
		t.Fatalf("observed %d times, want Ensure to have re-observed", seen)
	}
	if last < minPassSlice/4 {
		t.Fatalf("the last pass began with %v of a %v budget left, want at least %v", last, budget, minPassSlice/4)
	}
}
