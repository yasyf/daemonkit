//go:build darwin

package launchd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/realhome"
)

func okRunner(context.Context, string, ...string) (string, int, error) { return "", 0, nil }

func noWait(context.Context, time.Duration) error { return nil }

// recorder is a Runner that records each launchctl verb and answers `print`
// with the loaded state the test is standing up. A refusing recorder answers
// every verb the way launchd refuses one.
type recorder struct {
	verbs  []string
	loaded bool
	refuse bool
}

func (r *recorder) run(_ context.Context, _ string, args ...string) (string, int, error) {
	r.verbs = append(r.verbs, args[0])
	if r.refuse {
		return args[0] + " failed: 1: Operation not permitted", 1, launchctlErr(1)
	}
	if args[0] == "print" && !r.loaded {
		return "Could not find service", launchctlNotLoadedExit, launchctlErr(launchctlNotLoadedExit)
	}
	return "", 0, nil
}

func TestReloadRetriesOnlyInFlux(t *testing.T) {
	tests := []struct {
		name            string
		bootstrap       func(attempt int) (string, int, error)
		wantErr         bool
		wantErrContains string
		wantAttempts    int
	}{
		{
			name: "gives up after retrying persistent in-flux",
			bootstrap: func(int) (string, int, error) {
				return "Bootstrap failed: 37: Operation already in progress", launchctlAlreadyExit, launchctlErr(37)
			},
			wantErr: true, wantErrContains: "gave up after 3 attempts", wantAttempts: bootstrapAttempts,
		},
		{
			name: "succeeds after transient in-flux",
			bootstrap: func(attempt int) (string, int, error) {
				if attempt < bootstrapAttempts {
					return "Boot-out failed: 36: Operation now in progress", launchctlInProgressExit, launchctlErr(36)
				}
				return "", 0, nil
			},
			wantErr: false, wantAttempts: bootstrapAttempts,
		},
		{
			name: "does not retry a refusal",
			bootstrap: func(int) (string, int, error) {
				return "Bootstrap failed: 1: Operation not permitted", 1, launchctlErr(1)
			},
			wantErr: true, wantErrContains: "launchd refused", wantAttempts: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			run := func(_ context.Context, _ string, args ...string) (string, int, error) {
				if args[0] == "bootstrap" {
					attempts++
					return test.bootstrap(attempts)
				}
				return "", 0, nil
			}
			c := applier{run: run, wait: noWait}
			err := c.reload(context.Background(), Agent{Label: "com.example.worker"}, "/x.plist")
			if (err != nil) != test.wantErr {
				t.Fatalf("reload err = %v, want error=%t", err, test.wantErr)
			}
			if test.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), test.wantErrContains)) {
				t.Fatalf("reload err = %v, want it to contain %q", err, test.wantErrContains)
			}
			if attempts != test.wantAttempts {
				t.Fatalf("bootstrap attempts = %d, want %d", attempts, test.wantAttempts)
			}
		})
	}
}

func launchAgentsDir(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func applyAgent(t *testing.T, label string) Agent {
	t.Helper()
	return Agent{
		Label:         label,
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), label+".log"),
		RestartPolicy: RestartOnFailure,
	}
}

func installPlist(t *testing.T, dir string, agent Agent) string {
	t.Helper()
	plist, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, agent.Label+".plist")
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestApplyKickstartsAByteExactLoadedAgent pins the self-heal: a
// RestartOnFailure job that exited cleanly leaves a byte-exact plist and a
// loaded-but-dead job, so skipping the install must not skip the kickstart.
func TestApplyKickstartsAByteExactLoadedAgent(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.exact")
	path := installPlist(t, dir, agent)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{loaded: true}

	if err := Apply(context.Background(), rec.run, agent); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if want := []string{"print", "kickstart"}; !slices.Equal(rec.verbs, want) {
		t.Fatalf("verbs = %v, want %v", rec.verbs, want)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("a byte-exact plist was rewritten\n%s", after)
	}
}

func TestApplyRetriesOnlyAnInFluxKickstart(t *testing.T) {
	tests := []struct {
		name         string
		kickstart    func() (string, int, error)
		wantErr      string
		wantAttempts int
	}{
		{
			name: "in flux",
			kickstart: func() (string, int, error) {
				return "Kickstart failed: 37: Operation already in progress", launchctlAlreadyExit, launchctlErr(37)
			},
			wantErr: "gave up after 3 attempts", wantAttempts: bootstrapAttempts,
		},
		{
			name: "refused",
			kickstart: func() (string, int, error) {
				return "Kickstart failed: 1: Operation not permitted", 1, launchctlErr(1)
			},
			wantErr: "launchd refused", wantAttempts: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			run := func(_ context.Context, _ string, args ...string) (string, int, error) {
				if args[0] != "kickstart" {
					return "", 0, nil
				}
				attempts++
				return test.kickstart()
			}
			c := applier{run: run, wait: noWait}
			err := c.kickstart(context.Background(), "com.example.worker")
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("kickstart err = %v, want it to contain %q", err, test.wantErr)
			}
			if attempts != test.wantAttempts {
				t.Fatalf("kickstart attempts = %d, want %d", attempts, test.wantAttempts)
			}
		})
	}
}

func TestApplyInstallsAndReloadsAnAgentThatIsNotAlreadyExactAndLoaded(t *testing.T) {
	reload := []string{"bootout", "enable", "bootstrap", "kickstart"}
	tests := []struct {
		name      string
		loaded    bool
		onDisk    func(t *testing.T, dir string, agent Agent)
		wantVerbs []string
	}{
		{
			name:      "no plist at the label",
			loaded:    true,
			onDisk:    func(*testing.T, string, Agent) {},
			wantVerbs: reload,
		},
		{
			name:   "the plist drifted",
			loaded: true,
			onDisk: func(t *testing.T, dir string, agent Agent) {
				drifted := agent
				drifted.Args = []string{"stale"}
				installPlist(t, dir, drifted)
			},
			wantVerbs: reload,
		},
		{
			name:   "the plist is exact but launchd does not know the label",
			loaded: false,
			onDisk: func(t *testing.T, dir string, agent Agent) {
				installPlist(t, dir, agent)
			},
			wantVerbs: append([]string{"print"}, reload...),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := launchAgentsDir(t)
			agent := applyAgent(t, "com.example.worker")
			test.onDisk(t, dir, agent)
			rec := &recorder{loaded: test.loaded}

			if err := Apply(context.Background(), rec.run, agent); err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if !slices.Equal(rec.verbs, test.wantVerbs) {
				t.Fatalf("verbs = %v, want %v", rec.verbs, test.wantVerbs)
			}
			got, err := os.ReadFile(filepath.Join(dir, agent.Label+".plist"))
			if err != nil {
				t.Fatal(err)
			}
			want, err := agent.Plist()
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("installed plist is not the rendered plist\n%s", got)
			}
		})
	}
}

func TestApplyAdoptsPreCutMarkerlessAgent(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.worker")
	plistFile := filepath.Join(dir, agent.Label+".plist")
	preCut := "<?xml version=\"1.0\"?>\n<plist><dict><key>Label</key><string>com.example.worker</string></dict></plist>\n"
	if err := os.WriteFile(plistFile, []byte(preCut), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Apply(context.Background(), okRunner, agent); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, err := os.ReadFile(plistFile)
	if err != nil {
		t.Fatal(err)
	}
	want, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("adopted plist was not rewritten with the marker\n%s", got)
	}
	backups, err := filepath.Glob(plistFile + ".daemonkit-archived-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 1 {
		t.Fatalf("markerless plist not archived aside: %v", backups)
	}
	archived, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(archived) != preCut {
		t.Fatalf("archived backup does not preserve the pre-cut plist\n%s", archived)
	}
}

func TestApplyInstallsFreshAgentWithoutArchiving(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.fresh")
	plistFile := filepath.Join(dir, agent.Label+".plist")

	if err := Apply(context.Background(), okRunner, agent); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, err := os.ReadFile(plistFile)
	if err != nil {
		t.Fatalf("fresh agent plist not installed: %v", err)
	}
	if !plistHasOwnerMarker(got) {
		t.Fatalf("installed plist lacks the ownership marker\n%s", got)
	}
	backups, err := filepath.Glob(plistFile + ".daemonkit-archived-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 0 {
		t.Fatalf("fresh install archived a nonexistent plist: %v", backups)
	}
}

// TestApplyAndRemoveTouchOnlyTheNamedLabel is the ownership model itself: with
// machine-wide discovery gone, another daemonkit product's agent sitting in the
// same directory is invisible to both verbs, so no consumer can evict another's.
func TestApplyAndRemoveTouchOnlyTheNamedLabel(t *testing.T) {
	dir := launchAgentsDir(t)
	mine := applyAgent(t, "com.example.mine")
	theirs := applyAgent(t, "com.other.theirs")
	theirPath := installPlist(t, dir, theirs)
	theirPlist, err := os.ReadFile(theirPath)
	if err != nil {
		t.Fatal(err)
	}

	var targets []string
	run := func(_ context.Context, _ string, args ...string) (string, int, error) {
		targets = append(targets, args[len(args)-1])
		return "", 0, nil
	}
	if err := Apply(context.Background(), run, mine); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := Remove(context.Background(), run, mine.Label); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	for _, target := range targets {
		if strings.Contains(target, theirs.Label) {
			t.Fatalf("launchctl targets = %v, want none naming %q", targets, theirs.Label)
		}
	}
	survivor, err := os.ReadFile(theirPath)
	if err != nil {
		t.Fatalf("another product's marked agent was swept: %v", err)
	}
	if string(survivor) != string(theirPlist) {
		t.Fatalf("another product's marked agent was rewritten\n%s", survivor)
	}
}

func TestRemoveBootsOutAndDeletesTheMarkedLabel(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.stale")
	path := installPlist(t, dir, agent)
	rec := &recorder{loaded: true}

	if err := Remove(context.Background(), rec.run, agent.Label); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	if want := []string{"bootout"}; !slices.Equal(rec.verbs, want) {
		t.Fatalf("verbs = %v, want %v", rec.verbs, want)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("plist survived removal: err=%v", err)
	}
}

// TestRemoveIsIdempotent proves the second Remove of the same label is a
// success that asks launchd nothing: with its own plist gone daemonkit owns
// nothing at that label, and a job launchd still holds there belongs to
// whoever registered it from somewhere else.
func TestRemoveIsIdempotent(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.gone")
	installPlist(t, dir, agent)
	first := &recorder{loaded: true}
	if err := Remove(context.Background(), first.run, agent.Label); err != nil {
		t.Fatalf("first Remove: %v", err)
	}
	if want := []string{"bootout"}; !slices.Equal(first.verbs, want) {
		t.Fatalf("first Remove verbs = %v, want %v", first.verbs, want)
	}

	second := &recorder{loaded: true}
	if err := Remove(context.Background(), second.run, agent.Label); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if len(second.verbs) != 0 {
		t.Fatalf("second Remove verbs = %v, want a label daemonkit no longer owns left alone", second.verbs)
	}
}

// TestRemoveSucceedsWhenLaunchdDoesNotKnowTheLabel holds the other half of the
// no-op: daemonkit owns the plist, launchd has never heard of the job, and the
// plist still goes.
func TestRemoveSucceedsWhenLaunchdDoesNotKnowTheLabel(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.unknown")
	path := installPlist(t, dir, agent)
	unknown := func(context.Context, string, ...string) (string, int, error) {
		return "Boot-out failed: 3: No such process", launchctlNotLoadedExit, launchctlErr(launchctlNotLoadedExit)
	}

	if err := Remove(context.Background(), unknown, agent.Label); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("plist survived removal: err=%v", err)
	}
}

func TestRemoveRefusesAPlistWithoutTheOwnershipMarker(t *testing.T) {
	dir := launchAgentsDir(t)
	label := "com.other.worker"
	path := filepath.Join(dir, label+".plist")
	foreign := "<?xml version=\"1.0\"?>\n<plist><dict><key>Label</key><string>com.other.worker</string></dict></plist>\n"
	if err := os.WriteFile(path, []byte(foreign), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{loaded: true}

	err := Remove(context.Background(), rec.run, label)
	if !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Remove of a foreign plist = %v, want %v", err, ErrNotOwned)
	}
	if len(rec.verbs) != 0 {
		t.Fatalf("verbs = %v, want a foreign job left loaded", rec.verbs)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != foreign {
		t.Fatalf("foreign plist = %q (err %v), want it untouched", got, err)
	}
}

// TestVerify pins the observation Apply decides from: what launchd says about
// the label, what sits at daemonkit's path, and nothing mutated either way.
func TestVerify(t *testing.T) {
	tests := []struct {
		name    string
		program string
		plist   func(t *testing.T, dir string, agent Agent)
		rec     *recorder
		want    bool
		wantErr string
		verbs   []string
	}{
		{
			name:  "byte-exact plist launchd reports loaded",
			plist: func(t *testing.T, dir string, agent Agent) { installPlist(t, dir, agent) },
			rec:   &recorder{loaded: true},
			want:  true,
			verbs: []string{"print"},
		},
		{
			name:  "byte-exact plist launchd does not know",
			plist: func(t *testing.T, dir string, agent Agent) { installPlist(t, dir, agent) },
			rec:   &recorder{},
			verbs: []string{"print"},
		},
		{
			name:    "launchd refuses to answer",
			plist:   func(t *testing.T, dir string, agent Agent) { installPlist(t, dir, agent) },
			rec:     &recorder{refuse: true},
			wantErr: "launchd refused",
			verbs:   []string{"print"},
		},
		{
			name: "no plist where launchd reads",
			rec:  &recorder{loaded: true},
		},
		{
			name: "plist that drifted from the agent",
			plist: func(t *testing.T, dir string, agent Agent) {
				path := filepath.Join(dir, agent.Label+".plist")
				if err := os.WriteFile(path, []byte("<plist/>\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			rec: &recorder{loaded: true},
		},
		{
			name: "plist other users can read",
			plist: func(t *testing.T, dir string, agent Agent) {
				if err := os.Chmod(installPlist(t, dir, agent), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			rec: &recorder{loaded: true},
		},
		{
			name:    "program that is not installed",
			program: "/usr/bin/daemonkit-not-installed",
			plist:   func(t *testing.T, dir string, agent Agent) { installPlist(t, dir, agent) },
			rec:     &recorder{loaded: true},
		},
		{
			name:    "no runner",
			wantErr: "runner is required",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := launchAgentsDir(t)
			agent := applyAgent(t, "com.example.verify")
			if test.program != "" {
				agent.Program = test.program
			}
			if test.plist != nil {
				test.plist(t, dir, agent)
			}
			var run Runner
			if test.rec != nil {
				run = test.rec.run
			}

			got, err := Verify(context.Background(), run, agent)

			if test.wantErr == "" && err != nil {
				t.Fatalf("Verify = %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("Verify err = %v, want it to contain %q", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("Verify = %t, want %t", got, test.want)
			}
			if test.rec != nil && !slices.Equal(test.rec.verbs, test.verbs) {
				t.Fatalf("verbs = %v, want %v", test.rec.verbs, test.verbs)
			}
		})
	}
}

func TestApplyAndRemoveRequireARunner(t *testing.T) {
	if err := Apply(context.Background(), nil, Agent{}); err == nil ||
		!strings.Contains(err.Error(), "runner is required") {
		t.Fatalf("Apply with nil runner = %v, want a runner-required error", err)
	}
	if err := Remove(context.Background(), nil, "com.example.worker"); err == nil ||
		!strings.Contains(err.Error(), "runner is required") {
		t.Fatalf("Remove with nil runner = %v, want a runner-required error", err)
	}
}

func TestRemoveRequiresACanonicalLabel(t *testing.T) {
	launchAgentsDir(t)
	if err := Remove(context.Background(), okRunner, "../escape"); err == nil ||
		!strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("Remove of a traversing label = %v, want a canonical-label refusal", err)
	}
}

// TestApplyInstallsIntoAnAbsentLaunchAgentsDirectory pins the applier's
// ownership of the plist directory: a fresh macOS account has ~/Library but no
// ~/Library/LaunchAgents, and durable.WriteFile creates no directories, so an
// applier that does not create it cannot install on a new machine.
func TestApplyInstallsIntoAnAbsentLaunchAgentsDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	if err := os.Mkdir(filepath.Join(home, "Library"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture: %q already exists (stat err = %v)", dir, err)
	}
	agent := applyAgent(t, "com.example.fresh")

	if err := Apply(context.Background(), okRunner, agent); err != nil {
		t.Fatalf("Apply onto a fresh account: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, agent.Label+".plist"))
	if err != nil {
		t.Fatalf("agent plist not installed: %v", err)
	}
	want, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("installed plist is not the rendered plist\n%s", got)
	}
}
