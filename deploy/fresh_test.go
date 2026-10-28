//go:build darwin

package deploy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/launchd"
)

const launchctlUnknownLabel = 113

// vacantLaunchd stands launchctl up as launchd answers for labels nothing
// holds: print reports the label unknown until a bootstrap loads it, and the
// kickstart starts the daemon the agent's program would serve. The fixture's
// default runner answers print with success, which reads as loaded, so every
// fresh test that means "launchd holds nothing" says so here. Every argv is
// recorded.
func (f *fixture) vacantLaunchd() {
	f.t.Helper()
	loaded := false
	f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		switch args[0] {
		case "print":
			if !loaded {
				return "Could not find service", launchctlUnknownLabel, nil
			}
		case "bootstrap":
			loaded = true
		case "kickstart":
			f.startDaemonChild(0)
		}
		return "", 0, nil
	}
}

func (f *fixture) verbs() []string {
	verbs := make([]string, 0, len(f.launchctls))
	for _, args := range f.launchctls {
		verbs = append(verbs, args[0])
	}
	return verbs
}

// locked takes and releases the real deployment lock once, so the metadata
// directory and its lock exist before a test snapshots the tree: they are the
// one thing a refused InstallFresh may leave behind.
func (f *fixture) locked() {
	f.t.Helper()
	release, err := f.deploy.hold(f.ctx())
	if err != nil {
		f.t.Fatalf("hold: %v", err)
	}
	release()
}

func (f *fixture) snapshot() map[string]string {
	f.t.Helper()
	tree := map[string]string{}
	home := filepath.Dir(filepath.Dir(f.agentsDir))
	for _, root := range []string{f.root, home} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			state := info.Mode().String()
			if info.Mode().IsRegular() {
				content, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				state += " " + string(content)
			}
			tree[path] = state
			return nil
		})
		if err != nil {
			f.t.Fatalf("snapshot %q: %v", root, err)
		}
	}
	return tree
}

func (f *fixture) unchanged(before map[string]string) {
	f.t.Helper()
	after := f.snapshot()
	for path, state := range before {
		if got, ok := after[path]; !ok {
			f.t.Errorf("%q is gone", path)
		} else if got != state {
			f.t.Errorf("%q changed", path)
		}
	}
	for path := range after {
		if _, ok := before[path]; !ok {
			f.t.Errorf("%q appeared", path)
		}
	}
	if f.t.Failed() {
		f.t.FailNow()
	}
}

func (f *fixture) untouched(child *exec.Cmd) {
	f.t.Helper()
	found, err := Inventory(child.Path)
	if err != nil {
		f.t.Fatalf("Inventory(%q): %v", child.Path, err)
	}
	if !slices.ContainsFunc(found.Live, func(p LiveProcess) bool { return p.PID == child.Process.Pid }) {
		f.t.Fatalf("pid %d no longer runs %q: the refusal ended it", child.Process.Pid, child.Path)
	}
}

func (f *fixture) plant(path string) {
	f.t.Helper()
	if err := f.deploy.layout.ensureMetadata(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("planted\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) plantDir(path string) {
	f.t.Helper()
	if err := f.deploy.layout.ensureMetadata(); err != nil {
		f.t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(path, "Contents"), 0o700); err != nil {
		f.t.Fatal(err)
	}
}

var notFreshStates = []struct {
	name  string
	plant func(f *fixture)
}{
	{"activation record", func(f *fixture) { f.plant(f.deploy.layout.activation) }},
	{"services record", func(f *fixture) { f.plant(f.deploy.layout.services) }},
	{"removal record", func(f *fixture) { f.plant(f.deploy.layout.removal) }},
	{"swap record", func(f *fixture) { f.plant(f.deploy.layout.swap) }},
	{"installed canonical app", func(f *fixture) {
		rename(f.t, f.bundle("Installed", "1.0", "one"), f.deploy.layout.canonical)
	}},
	{"prior slot", func(f *fixture) { f.plantDir(f.deploy.layout.prior) }},
	{"removal slot", func(f *fixture) { f.plantDir(f.deploy.layout.removed) }},
	{"candidate slot", func(f *fixture) {
		rename(f.t, f.bundle("Staged", "2.0", "two"), f.deploy.layout.candidate)
	}},
	{"staging tree", func(f *fixture) {
		if err := f.deploy.layout.ensureMetadata(); err != nil {
			f.t.Fatal(err)
		}
		if _, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix); err != nil {
			f.t.Fatal(err)
		}
	}},
	{"foreign entry in the metadata directory", func(f *fixture) {
		f.plant(filepath.Join(f.deploy.layout.metadata, "stray"))
	}},
}

// TestInstallFreshRefusesAPendingPriorSwapBeforeRecover is the window the gate
// exists for: a supersede crashed with its prior moved aside and then reclaimed,
// its candidate staged, and a stray still running out of the candidate slot.
// Every default verb resumes that swap first, and the resume is gated — it ends
// the stray. InstallFresh refuses before any of that, so the stray keeps
// running, the swap record and both trees stay byte-for-byte as they were, and
// launchd is never asked anything. The recover run after it is the control: the
// same state, resumed, does end the stray, so a refusal that spared it is the
// gate and not a stray the ladder could not reach.
func TestInstallFreshRefusesAPendingPriorSwapBeforeRecover(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		if err := os.RemoveAll(f.deploy.layout.canonical); err != nil {
			t.Fatal(err)
		}
	})
	stray := f.liveSlot()
	f.locked()
	before := f.snapshot()

	if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	f.untouched(stray)
	f.unchanged(before)
	if len(f.launchctls) != 0 {
		t.Fatalf("launchctl ran %q on a refused install", f.launchctls)
	}
	if err := f.deploy.recover(f.ctx()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	f.terminated(stray)
}

// TestInstallFreshRefusesAPriorNilSwapWithoutLandingIt is the swap recover
// resumes with no gate at all, since a first install has no prior to lose: it
// would land a candidate another writer staged. InstallFresh refuses it with
// the canonical path still empty and the staged tree exactly where it was; the
// recover after it is the control, landing that very candidate.
func TestInstallFreshRefusesAPriorNilSwapWithoutLandingIt(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	f.crash("", "two", func(*testing.T, *fixture) {})
	staged, err := f.deploy.inspect(f.ctx(), f.deploy.layout.candidate)
	if err != nil {
		t.Fatal(err)
	}
	f.locked()
	before := f.snapshot()

	if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	if fileExists(f.app) {
		t.Fatal("InstallFresh landed a swap another writer recorded")
	}
	f.unchanged(before)
	still, err := f.deploy.inspect(f.ctx(), f.deploy.layout.candidate)
	if err != nil || !still.sameTree(staged) {
		t.Fatalf("candidate slot = %+v, %v; want the staged tree untouched", still, err)
	}
	if len(f.launchctls) != 0 {
		t.Fatalf("launchctl ran %q on a refused install", f.launchctls)
	}
	if err := f.deploy.recover(f.ctx()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	f.wantCanonical("two")
}

// TestInstallFreshRefusesEveryRecordAndSlot holds each piece of a deployment
// that was ever here to a refusal on its own, before launchd is asked anything,
// with every file under the install root and the home byte-identical after.
func TestInstallFreshRefusesEveryRecordAndSlot(t *testing.T) {
	for _, tt := range notFreshStates {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.vacantLaunchd()
			candidate := f.candidate("Source", "3.0", "three")
			tt.plant(f)
			f.locked()
			before := f.snapshot()

			if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
				t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
			}

			f.unchanged(before)
			if len(f.launchctls) != 0 {
				t.Fatalf("launchctl ran %q before the filesystem refused", f.launchctls)
			}
		})
	}
}

// TestInstallFreshRefusesALiveDaemon serves a daemon at the deployment's label
// with no record of it anywhere: its owner record is removed, and its
// executable is this test binary, which no inventory of the deployment names.
// The health probe is the only check that can see it, and it refuses without
// draining it — the same instance is still serving after.
func TestInstallFreshRefusesALiveDaemon(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	f.startDaemonChild(0)
	incumbent := f.serving(20 * time.Second)
	if err := os.Remove(f.deploy.config.Daemon.RecordPath()); err != nil {
		t.Fatal(err)
	}

	if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	if pid := f.serving(5 * time.Second); pid != incumbent {
		t.Fatalf("pid %d is serving, want the untouched incumbent %d", pid, incumbent)
	}
	if fileExists(f.app) {
		t.Fatal("InstallFresh landed over a live daemon")
	}
	if want := []string{"print"}; !slices.Equal(f.verbs(), want) {
		t.Fatalf("launchctl verbs = %v, want %v", f.verbs(), want)
	}
}

// TestInstallFreshRefusesASquattedSocket is the attach that never answers: a
// same-UID listener holding the socket path and accepting nothing. A clock that
// ran out proved nothing absent, so it is not fresh, and the refusal comes back
// inside the caller's budget.
func TestInstallFreshRefusesASquattedSocket(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	f.squatSocket()
	const budget = 2 * time.Second
	start := time.Now()

	if _, err := f.deploy.InstallFresh(f.within(budget), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	if took := time.Since(start); took > 2*budget {
		t.Fatalf("InstallFresh took %v against a squatted socket, want a refusal inside its %v budget", took, budget)
	}
	if fileExists(f.app) {
		t.Fatal("InstallFresh landed behind an attach that never answered")
	}
}

// TestInstallFreshRefusesAnOwnerRecord is a daemon of this deployment that
// recorded itself here once: the owner record alone refuses, on the filesystem,
// before launchd is asked anything.
func TestInstallFreshRefusesAnOwnerRecord(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	f.recordOwner(t)

	if err := f.deploy.Fresh(f.ctx()); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("Fresh err = %v, want ErrNotFresh", err)
	}
	if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	if fileExists(f.app) {
		t.Fatal("InstallFresh landed over a recorded owner")
	}
	if len(f.launchctls) != 0 {
		t.Fatalf("launchctl ran %q before the owner record refused", f.launchctls)
	}
}

// TestInstallFreshRefusesALoadedOrPlistedAgent is an agent label something
// already holds: a plist at it, refused before launchd is asked, or launchd
// reporting it loaded, refused on that one print. Neither boots anything out.
func TestInstallFreshRefusesALoadedOrPlistedAgent(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(f *fixture)
		verbs   []string
	}{
		{"a plist at the agent's label", func(f *fixture) {
			f.vacantLaunchd()
			f.markedAgent(f.agent.Label)
		}, []string{}},
		{"launchd reports the agent's label loaded", func(*fixture) {}, []string{"print"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			candidate := f.candidate("Source", "3.0", "three")
			tt.prepare(f)
			f.locked()
			before := f.snapshot()

			if _, err := f.deploy.InstallFresh(f.ctx(), candidate); !errors.Is(err, ErrNotFresh) {
				t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
			}

			f.unchanged(before)
			if !slices.Equal(f.verbs(), tt.verbs) {
				t.Fatalf("launchctl verbs = %v, want %v", f.verbs(), tt.verbs)
			}
		})
	}
}

// TestInstallFreshGateRunsUnderTheRealDeploymentHold pins the gate to the lock
// every other writer takes. InstallFresh started while another writer holds the
// real deployment lock blocks on it; that writer leaves a superseding swap with
// its prior moved aside and a stray running out of it, and releases. The gate
// that runs once the lock is InstallFresh's sees that swap and refuses before
// any resume: the stray still runs, nothing on disk moved, and launchd was never
// asked anything. The test goroutine is the writer holding the lock, since the
// fixture's helpers may only fail the test from it.
func TestInstallFreshGateRunsUnderTheRealDeploymentHold(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	ctx := f.within(60 * time.Second)
	release, err := f.deploy.hold(f.ctx())
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	refused := make(chan error, 1)
	go func() {
		_, err := f.deploy.InstallFresh(ctx, candidate)
		refused <- err
	}()
	select {
	case err := <-refused:
		release()
		t.Fatalf("InstallFresh returned %v while another writer held the deployment lock", err)
	case <-time.After(500 * time.Millisecond):
	}
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
	})
	stray := f.liveSlot()
	before := f.snapshot()
	release()

	select {
	case err = <-refused:
	case <-time.After(30 * time.Second):
		t.Fatal("InstallFresh never took the released deployment lock")
	}
	if !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}
	f.untouched(stray)
	f.unchanged(before)
	if len(f.launchctls) != 0 {
		t.Fatalf("launchctl ran %q on a refused install", f.launchctls)
	}
}

// TestInstallFreshRefusesAHeldLock is the hold that cannot be acquired: a lock
// another writer keeps past the caller's budget is lock contention, and
// contention is not fresh.
func TestInstallFreshRefusesAHeldLock(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	release, err := f.deploy.hold(f.ctx())
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	defer release()

	if _, err := f.deploy.InstallFresh(f.within(500*time.Millisecond), candidate); !errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh", err)
	}

	if fileExists(f.app) || len(f.launchctls) != 0 {
		t.Fatal("InstallFresh acted without the deployment lock")
	}
}

// TestInstallFreshReturnsAMetadataConflictAsItself is a metadata directory not
// private to its owner: the fault every verb refuses on, not lock contention,
// so it comes back as ErrConflict and never as ErrNotFresh.
func TestInstallFreshReturnsAMetadataConflictAsItself(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")
	if err := os.MkdirAll(f.deploy.layout.metadata, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.deploy.layout.metadata, 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := f.deploy.InstallFresh(f.ctx(), candidate)

	if !errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFresh) {
		t.Fatalf("InstallFresh err = %v, want ErrConflict and not ErrNotFresh", err)
	}
	if fileExists(f.deploy.layout.lock) || len(f.launchctls) != 0 {
		t.Fatal("InstallFresh acted past a metadata directory it could not trust")
	}
}

// TestFreshVerbsRequireAContextDeadline holds a missing deadline to the plain
// error every control-lane verb returns: a caller's mistake is not an
// incumbent, so it is never ErrNotFresh, and nothing is touched or asked first.
func TestFreshVerbsRequireAContextDeadline(t *testing.T) {
	f := newFixture(t)
	f.vacantLaunchd()
	candidate := f.candidate("Source", "3.0", "three")

	if err := f.deploy.Fresh(context.Background()); err == nil || err.Error() != "deploy: Fresh requires a context deadline" {
		t.Fatalf("Fresh err = %v, want the missing-deadline error", err)
	}
	if _, err := f.deploy.InstallFresh(context.Background(), candidate); err == nil || err.Error() != "deploy: InstallFresh requires a context deadline" {
		t.Fatalf("InstallFresh err = %v, want the missing-deadline error", err)
	}

	if fileExists(filepath.Dir(f.deploy.layout.metadata)) || len(f.launchctls) != 0 {
		t.Fatal("a fresh verb without a deadline acted before refusing")
	}
}

// TestInstallFreshRefusesWhenALabelAppearsBeforeActivation is the race behind
// both gates: launchd takes the agent's label between the second gate and the
// bootstrap, and its own bootstrap refusal is the atomic check. That is
// ErrNotFresh over launchd.ErrPresent with nothing booted out and no kickstart;
// the landed generation stays installed, no activation is sealed, the plist
// ApplyFresh published is gone, and the services record, which named the label
// before launchd was asked to load it, is rolled back to no label at all.
func TestInstallFreshRefusesWhenALabelAppearsBeforeActivation(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("Source", "3.0", "three")
	bootstrapped := false
	f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		switch args[0] {
		case "print":
			if !bootstrapped {
				return "Could not find service", launchctlUnknownLabel, nil
			}
		case "bootstrap":
			bootstrapped = true
			return "Bootstrap failed: 5: Input/output error", 5, nil
		}
		return "", 0, nil
	}

	_, err := f.deploy.InstallFresh(f.ctx(), candidate)

	if !errors.Is(err, ErrNotFresh) || !errors.Is(err, launchd.ErrPresent) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh over launchd.ErrPresent", err)
	}
	if want := []string{"print", "print", "print", "enable", "bootstrap", "print"}; !slices.Equal(f.verbs(), want) {
		t.Fatalf("launchctl verbs = %v, want %v and never bootout or kickstart", f.verbs(), want)
	}
	f.wantCanonical("three")
	if fileExists(f.deploy.layout.activation) {
		t.Fatal("InstallFresh sealed an activation for a label it never got")
	}
	if fileExists(f.deploy.layout.services) {
		t.Fatal("InstallFresh kept a services record naming a label it never got")
	}
	if fileExists(filepath.Join(f.agentsDir, f.agent.Label+".plist")) {
		t.Fatal("InstallFresh left the plist it published at a label it never got")
	}
}

// TestInstallFreshRefusesOnlyBeforeLaunchdChangesAnything draws the line
// between a refusal and a failure inside ApplyFresh. Its own presence check
// failing to answer comes before launchd is asked to change anything, so it is
// uncertain and therefore not fresh, and the services record is rolled back
// with no plist of this call's at the label. A kickstart launchd refuses after
// the bootstrap loaded the job is a failure of an activation already under
// way, and is reported as itself: the job stays loaded, so its plist stays at
// the label, the services record keeps naming it, and nothing is booted out.
func TestInstallFreshRefusesOnlyBeforeLaunchdChangesAnything(t *testing.T) {
	tests := []struct {
		name      string
		answer    func(args []string, prints int) (string, int, error)
		verbs     []string
		wantFresh bool
		recorded  bool
	}{
		{
			name: "ApplyFresh's own presence check cannot answer",
			answer: func(args []string, prints int) (string, int, error) {
				if args[0] == "print" && prints == 3 {
					return "print failed: 1: Operation not permitted", 1, nil
				}
				if args[0] == "print" {
					return "Could not find service", launchctlUnknownLabel, nil
				}
				return "", 0, nil
			},
			verbs:     []string{"print", "print", "print"},
			wantFresh: true,
		},
		{
			name: "the kickstart is refused after the bootstrap",
			answer: func(args []string, _ int) (string, int, error) {
				switch args[0] {
				case "print":
					return "Could not find service", launchctlUnknownLabel, nil
				case "kickstart":
					return "Kickstart failed: 1: Operation not permitted", 1, nil
				}
				return "", 0, nil
			},
			verbs:    []string{"print", "print", "print", "enable", "bootstrap", "kickstart"},
			recorded: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			candidate := f.candidate("Source", "3.0", "three")
			prints := 0
			f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
				f.launchctls = append(f.launchctls, args)
				if args[0] == "print" {
					prints++
				}
				return tt.answer(args, prints)
			}

			_, err := f.deploy.InstallFresh(f.ctx(), candidate)

			if err == nil {
				t.Fatal("InstallFresh = nil, want the failure launchctl was made to answer")
			}
			if errors.Is(err, ErrNotFresh) != tt.wantFresh {
				t.Fatalf("InstallFresh err = %v, want ErrNotFresh=%t", err, tt.wantFresh)
			}
			if !slices.Equal(f.verbs(), tt.verbs) {
				t.Fatalf("launchctl verbs = %v, want %v", f.verbs(), tt.verbs)
			}
			if fileExists(f.deploy.layout.activation) {
				t.Fatal("InstallFresh sealed an activation it never proved")
			}
			var recorded []string
			if tt.recorded {
				recorded = []string{f.agent.Label}
			}
			applied, err := f.deploy.appliedServices()
			if err != nil || !slices.Equal(applied, recorded) {
				t.Fatalf("applied services = %v, %v; want %v", applied, err, recorded)
			}
			if got := fileExists(filepath.Join(f.agentsDir, f.agent.Label+".plist")); got != tt.recorded {
				t.Fatalf("plist at the label = %t, want %t", got, tt.recorded)
			}
		})
	}
}

// TestInstallFreshRefusesACandidateSlotThatAppearsAfterTheGate is a writer
// outside the hold — a legacy-lock install sharing the slot path — publishing
// another generation into the candidate slot once the first gate's filesystem
// half has passed; the gate's own launchd print is where it lands. Staging's
// up-front check refuses it at gate stage, naming the slot that exists, rather
// than discarding it: every file under the install root and the home, the
// foreign slot among them, is byte-identical after, and launchd was asked
// nothing but the gate's print.
func TestInstallFreshRefusesACandidateSlotThatAppearsAfterTheGate(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("Source", "3.0", "three")
	foreign := f.bundle("Foreign", "2.0", "two")
	var before map[string]string
	f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		if args[0] == "print" && before == nil {
			rename(f.t, foreign, f.deploy.layout.candidate)
			before = f.snapshot()
		}
		return "Could not find service", launchctlUnknownLabel, nil
	}

	_, err := f.deploy.InstallFresh(f.ctx(), candidate)

	exists := fmt.Sprintf("gate stage: %v: %q exists", ErrNotFresh, f.deploy.layout.candidate)
	if !errors.Is(err, ErrNotFresh) || !strings.Contains(err.Error(), exists) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh at gate stage reading %q", err, exists)
	}
	if before == nil {
		t.Fatal("the gate never asked launchd, so no slot appeared")
	}
	f.unchanged(before)
	if want := []string{"print"}; !slices.Equal(f.verbs(), want) {
		t.Fatalf("launchctl verbs = %v, want %v", f.verbs(), want)
	}
}

// TestStageFreshRefusesACandidateSlotThatAppearsAtPublish is the same writer
// landing later: past staging's up-front check, while this call's copy sits
// staged and ready to publish. The create-only publish refuses the occupied
// slot at gate stage, the foreign generation stays byte-identical in the slot,
// the staging tree this call made is gone, and nothing else changes.
func TestStageFreshRefusesACandidateSlotThatAppearsAtPublish(t *testing.T) {
	f := newFixture(t)
	if err := f.deploy.layout.ensureMetadata(); err != nil {
		t.Fatal(err)
	}
	candidate := f.candidate("Source", "3.0", "three")
	foreign := f.bundle("Foreign", "2.0", "two")
	var staged string
	var before map[string]string
	publish := func(from, to string) error {
		rename(f.t, foreign, to)
		staged, before = from, f.snapshot()
		maps.DeleteFunc(before, func(path, _ string) bool {
			return path == from || strings.HasPrefix(path, from+string(filepath.Separator))
		})
		return durable.RenameNew(from, to)
	}

	_, err := f.deploy.stageFresh(f.ctx(), candidate, publish)

	if !errors.Is(err, ErrNotFresh) || !errors.Is(err, os.ErrExist) ||
		!strings.Contains(err.Error(), "gate stage") || !strings.Contains(err.Error(), "a candidate slot appeared") {
		t.Fatalf("stageFresh err = %v, want ErrNotFresh over os.ErrExist at gate stage's publish", err)
	}
	if before == nil {
		t.Fatal("stageFresh never reached its publish, so no slot appeared there")
	}
	if fileExists(staged) {
		t.Fatalf("the staging tree %q outlived the refusal", staged)
	}
	f.unchanged(before)
}

// TestInstallFreshRollsBackARefusedSecondAgent is the second of two agents
// refused at its bootstrap, with launchd then reporting its label loaded. Each
// label joins the services record just before its own bootstrap, and the
// refusal rolls the record back to the first label alone: the first agent
// stays bootstrapped with its plist in place, the second's plist is gone, and
// nothing is booted out.
func TestInstallFreshRollsBackARefusedSecondAgent(t *testing.T) {
	f := newFixture(t)
	second := f.agent
	second.Label = f.agent.Label + ".second"
	f.deploy.config.Agents = append(f.deploy.config.Agents, second)
	candidate := f.candidate("Source", "3.0", "three")
	loaded := map[string]bool{}
	recorded := map[string][]string{}
	f.deploy.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		label := strings.TrimSuffix(filepath.Base(args[len(args)-1]), ".plist")
		switch args[0] {
		case "print":
			if !loaded[label] {
				return "Could not find service", launchctlUnknownLabel, nil
			}
		case "bootstrap":
			applied, err := f.deploy.appliedServices()
			if err != nil {
				t.Errorf("read the services record at %q's bootstrap: %v", label, err)
			}
			recorded[label] = applied
			loaded[label] = true
			if label == second.Label {
				return "Bootstrap failed: 5: Input/output error", 5, nil
			}
		}
		return "", 0, nil
	}

	_, err := f.deploy.InstallFresh(f.ctx(), candidate)

	if !errors.Is(err, ErrNotFresh) || !errors.Is(err, launchd.ErrPresent) {
		t.Fatalf("InstallFresh err = %v, want ErrNotFresh over launchd.ErrPresent", err)
	}
	want := []string{
		"print", "print", "print", "print",
		"print", "enable", "bootstrap", "kickstart",
		"print", "enable", "bootstrap", "print",
	}
	if !slices.Equal(f.verbs(), want) {
		t.Fatalf("launchctl verbs = %v, want %v and never bootout", f.verbs(), want)
	}
	if got := recorded[f.agent.Label]; !slices.Equal(got, []string{f.agent.Label}) {
		t.Fatalf("services at the first bootstrap = %v, want [%q]", got, f.agent.Label)
	}
	if got := recorded[second.Label]; !slices.Equal(got, []string{f.agent.Label, second.Label}) {
		t.Fatalf("services at the second bootstrap = %v, want [%q %q]", got, f.agent.Label, second.Label)
	}
	applied, err := f.deploy.appliedServices()
	if err != nil || !slices.Equal(applied, []string{f.agent.Label}) {
		t.Fatalf("applied services = %v, %v; want [%q]", applied, err, f.agent.Label)
	}
	if !fileExists(filepath.Join(f.agentsDir, f.agent.Label+".plist")) {
		t.Fatal("the rollback took the bootstrapped first agent's plist")
	}
	if fileExists(filepath.Join(f.agentsDir, second.Label+".plist")) {
		t.Fatal("the refused second agent's plist is still at its label")
	}
	if fileExists(f.deploy.layout.activation) {
		t.Fatal("InstallFresh sealed an activation for an agent it never got")
	}
}

// TestInstallFreshLandsAndActivatesACleanRoot is the one state InstallFresh
// acts on, from nothing on disk and from the bare metadata directory a refused
// attempt leaves. launchd is asked whether the label is held at each gate and
// by ApplyFresh, then enable, bootstrap, and kickstart, and never bootout; the
// activation sealed is the one returned, its readiness the daemon's own.
func TestInstallFreshLandsAndActivatesACleanRoot(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(f *fixture)
	}{
		{"nothing on disk", func(*fixture) {}},
		{"a bare metadata directory and its lock", func(f *fixture) { f.locked() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.vacantLaunchd()
			tt.prepare(f)
			candidate := f.candidate("Source", "1.0", "one")

			activation, err := f.deploy.InstallFresh(f.within(30*time.Second), candidate)
			if err != nil {
				t.Fatalf("InstallFresh: %v", err)
			}

			if want := []string{"print", "print", "print", "enable", "bootstrap", "kickstart"}; !slices.Equal(f.verbs(), want) {
				t.Fatalf("launchctl verbs = %v, want %v", f.verbs(), want)
			}
			generation := activation.Generation
			if generation.Path != f.app || generation.Version != "1.0" || generation.BundleDigest != candidate.Digest.String() {
				t.Fatalf("activation generation = %+v, want version 1.0 of %v at %q", generation, candidate.Digest, f.app)
			}
			var sealed activationRecord
			if err := readRecord(f.deploy.layout.activation, &sealed); err != nil {
				t.Fatalf("read sealed activation: %v", err)
			}
			want := activationRecord{
				Identity: activationIdentity, Schema: recordSchema,
				Generation: generation, Readiness: activation.Readiness.stored(),
			}
			if sealed != want {
				t.Fatalf("sealed activation = %+v, want %+v", sealed, want)
			}
			health, err := f.deploy.client.WaitReady(f.within(5 * time.Second))
			if err != nil {
				t.Fatalf("WaitReady: %v", err)
			}
			if activation.Readiness.Build() != health.Build || activation.Readiness.Generation() != health.Generation {
				t.Fatalf("activation readiness = %q/%d, want the serving %q/%d",
					activation.Readiness.Build(), activation.Readiness.Generation(), health.Build, health.Generation)
			}
			applied, err := f.deploy.appliedServices()
			if err != nil || !slices.Equal(applied, []string{f.agent.Label}) {
				t.Fatalf("applied services = %v, %v; want [%q]", applied, err, f.agent.Label)
			}
			if fileExists(f.deploy.layout.swap) || fileExists(f.deploy.layout.candidate) {
				t.Fatal("InstallFresh left the swap record or the candidate slot behind")
			}
			if err := f.deploy.Fresh(f.within(5 * time.Second)); !errors.Is(err, ErrNotFresh) {
				t.Fatalf("Fresh after InstallFresh = %v, want ErrNotFresh", err)
			}
		})
	}
}

// TestFreshIsReadOnly holds the pre-flight to observing alone. On a clean root
// it answers fresh and creates no metadata directory; the bare directory and
// lock a refused install leaves are still fresh; and on every state the
// filesystem refuses, nothing under the install root or the home changes and
// launchd is never asked.
func TestFreshIsReadOnly(t *testing.T) {
	t.Run("clean root", func(t *testing.T) {
		f := newFixture(t)
		f.vacantLaunchd()
		before := f.snapshot()
		if err := f.deploy.Fresh(f.ctx()); err != nil {
			t.Fatalf("Fresh on a clean root = %v, want nil", err)
		}
		f.unchanged(before)
		if fileExists(filepath.Dir(f.deploy.layout.metadata)) {
			t.Fatal("Fresh created the metadata directory")
		}
	})
	t.Run("bare metadata directory and lock", func(t *testing.T) {
		f := newFixture(t)
		f.vacantLaunchd()
		f.locked()
		before := f.snapshot()
		if err := f.deploy.Fresh(f.ctx()); err != nil {
			t.Fatalf("Fresh over a bare metadata directory = %v, want nil", err)
		}
		f.unchanged(before)
	})
	for _, tt := range notFreshStates {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.vacantLaunchd()
			tt.plant(f)
			before := f.snapshot()
			if err := f.deploy.Fresh(f.ctx()); !errors.Is(err, ErrNotFresh) {
				t.Fatalf("Fresh err = %v, want ErrNotFresh", err)
			}
			f.unchanged(before)
			if len(f.launchctls) != 0 {
				t.Fatalf("launchctl ran %q before the filesystem refused", f.launchctls)
			}
		})
	}
}
