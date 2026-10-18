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
	if err := ApplyFresh(context.Background(), nil, Agent{}); err == nil ||
		!strings.Contains(err.Error(), "runner is required") {
		t.Fatalf("ApplyFresh with nil runner = %v, want a runner-required error", err)
	}
	if err := RequireAbsent(context.Background(), nil, "com.example.worker"); err == nil ||
		!strings.Contains(err.Error(), "runner is required") {
		t.Fatalf("RequireAbsent with nil runner = %v, want a runner-required error", err)
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

// TestRequireAbsent pins the read-only probe ApplyFresh refuses on and a
// fresh-only deploy gates on: a plist at the label answers before launchd is
// asked anything, a loaded label is ErrPresent, and a launchd that could not
// answer is an error that is never mistaken for an absence.
func TestRequireAbsent(t *testing.T) {
	tests := []struct {
		name        string
		plist       bool
		rec         *recorder
		wantPresent bool
		wantErr     string
		verbs       []string
	}{
		{name: "nothing at the label", rec: &recorder{}, verbs: []string{"print"}},
		{name: "a plist at the label", plist: true, rec: &recorder{}, wantPresent: true},
		{name: "launchd reports the label loaded", rec: &recorder{loaded: true}, wantPresent: true, verbs: []string{"print"}},
		{name: "launchd refuses to answer", rec: &recorder{refuse: true}, wantErr: "launchd refused", verbs: []string{"print"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := launchAgentsDir(t)
			agent := applyAgent(t, "com.example.absent")
			if test.plist {
				installPlist(t, dir, agent)
			}

			err := RequireAbsent(context.Background(), test.rec.run, agent.Label)

			if errors.Is(err, ErrPresent) != test.wantPresent {
				t.Fatalf("RequireAbsent = %v, want ErrPresent=%t", err, test.wantPresent)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("RequireAbsent = %v, want it to contain %q", err, test.wantErr)
			}
			if !test.wantPresent && test.wantErr == "" && err != nil {
				t.Fatalf("RequireAbsent = %v, want nil", err)
			}
			if !slices.Equal(test.rec.verbs, test.verbs) {
				t.Fatalf("verbs = %v, want %v", test.rec.verbs, test.verbs)
			}
		})
	}
	if err := RequireAbsent(context.Background(), okRunner, "../escape"); err == nil ||
		!strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("RequireAbsent of a traversing label = %v, want a canonical-label refusal", err)
	}
}

// TestApplyFreshNeverBootsOut is the whole of what a bootstrap-only apply asks
// of launchd: whether the label is loaded, then enable, bootstrap, and a
// kickstart that starts the job without killing one. The plist it publishes is
// the rendered one at 0600, and the temp it was staged through is gone.
func TestApplyFreshNeverBootsOut(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.fresh")
	var argv [][]string
	run := func(_ context.Context, _ string, args ...string) (string, int, error) {
		argv = append(argv, args)
		if args[0] == "print" {
			return "Could not find service", launchctlNotFoundExit, nil
		}
		return "", 0, nil
	}

	if err := ApplyFresh(context.Background(), run, agent); err != nil {
		t.Fatalf("ApplyFresh: %v", err)
	}

	want := [][]string{
		{"print", serviceTarget(agent.Label)},
		{"enable", serviceTarget(agent.Label)},
		{"bootstrap", domainTarget(), filepath.Join(dir, agent.Label+".plist")},
		{"kickstart", serviceTarget(agent.Label)},
	}
	if !slices.EqualFunc(argv, want, slices.Equal[[]string]) {
		t.Fatalf("launchctl argv = %q, want %q", argv, want)
	}
	path := filepath.Join(dir, agent.Label+".plist")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("agent plist not published: %v", err)
	}
	rendered, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(rendered) {
		t.Fatalf("published plist is not the rendered plist\n%s", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("published plist mode = %#o, want 0600", info.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("LaunchAgents holds %d entries, want only the published plist", len(entries))
	}
}

// TestApplyFreshRefusesAPresentAgent holds the refusal to leaving the label
// exactly as it was found: no plist written, archived, or rewritten, and no
// launchctl verb that changes anything.
func TestApplyFreshRefusesAPresentAgent(t *testing.T) {
	foreign := "<?xml version=\"1.0\"?>\n<plist><dict><key>Label</key><string>com.example.present</string></dict></plist>\n"
	tests := []struct {
		name   string
		plist  func(t *testing.T, dir string, agent Agent)
		loaded bool
		verbs  []string
	}{
		{
			name:  "a daemonkit plist at the label",
			plist: func(t *testing.T, dir string, agent Agent) { installPlist(t, dir, agent) },
		},
		{
			name: "a foreign plist at the label",
			plist: func(t *testing.T, dir string, agent Agent) {
				if err := os.WriteFile(filepath.Join(dir, agent.Label+".plist"), []byte(foreign), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:   "launchd reports the label loaded",
			loaded: true,
			verbs:  []string{"print"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := launchAgentsDir(t)
			agent := applyAgent(t, "com.example.present")
			path := filepath.Join(dir, agent.Label+".plist")
			if test.plist != nil {
				test.plist(t, dir, agent)
			}
			before, beforeErr := os.ReadFile(path)
			rec := &recorder{loaded: test.loaded}

			err := ApplyFresh(context.Background(), rec.run, agent)

			if !errors.Is(err, ErrPresent) {
				t.Fatalf("ApplyFresh = %v, want ErrPresent", err)
			}
			if !slices.Equal(rec.verbs, test.verbs) {
				t.Fatalf("verbs = %v, want %v", rec.verbs, test.verbs)
			}
			after, afterErr := os.ReadFile(path)
			if string(after) != string(before) || errors.Is(beforeErr, os.ErrNotExist) != errors.Is(afterErr, os.ErrNotExist) {
				t.Fatalf("plist at the label = %q (err %v), want it as found: %q (err %v)", after, afterErr, before, beforeErr)
			}
			backups, err := filepath.Glob(path + ".daemonkit-archived-*")
			if err != nil {
				t.Fatal(err)
			}
			if len(backups) != 0 {
				t.Fatalf("ApplyFresh archived a plist aside: %v", backups)
			}
		})
	}
}

// TestApplyFreshRefusesALabelLoadedMidway is the race the checks cannot close
// on their own: launchd takes the label between the print and the bootstrap,
// and its own bootstrap refusal is the atomic check. That refusal is
// ErrPresent with the job left loaded — never a bootout to make room, and never
// a kickstart of a job this call did not load. A bootstrap refused with the
// label still unloaded is launchd's own refusal, not a presence.
func TestApplyFreshRefusesALabelLoadedMidway(t *testing.T) {
	tests := []struct {
		name        string
		out         string
		code        int
		loads       bool
		wantPresent bool
		bootstraps  int
	}{
		{
			name: "launchd answers its batch status", out: "Bootstrap failed: 5: Input/output error",
			code: launchctlAggregateExit, loads: true, wantPresent: true, bootstraps: 1,
		},
		{
			name: "launchd reports the bootstrap already in progress", out: "Bootstrap failed: 37: Operation already in progress",
			code: launchctlAlreadyExit, loads: true, wantPresent: true, bootstraps: bootstrapAttempts,
		},
		{
			name: "launchd refuses with the label still unloaded", out: "Bootstrap failed: 1: Operation not permitted",
			code: 1, bootstraps: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			launchAgentsDir(t)
			agent := applyAgent(t, "com.example.raced")
			var verbs []string
			loaded := false
			run := func(_ context.Context, _ string, args ...string) (string, int, error) {
				verbs = append(verbs, args[0])
				switch args[0] {
				case "print":
					if !loaded {
						return "Could not find service", launchctlNotFoundExit, nil
					}
				case "bootstrap":
					loaded = test.loads
					return test.out, test.code, nil
				}
				return "", 0, nil
			}

			err := applier{run: run, wait: noWait}.applyFresh(context.Background(), agent)

			if err == nil {
				t.Fatal("applyFresh = nil, want the bootstrap's refusal")
			}
			if errors.Is(err, ErrPresent) != test.wantPresent {
				t.Fatalf("applyFresh = %v, want ErrPresent=%t", err, test.wantPresent)
			}
			if slices.Contains(verbs, "bootout") || slices.Contains(verbs, "kickstart") {
				t.Fatalf("verbs = %v, want no bootout and no kickstart", verbs)
			}
			bootstraps := 0
			for _, verb := range verbs {
				if verb == "bootstrap" {
					bootstraps++
				}
			}
			if bootstraps != test.bootstraps {
				t.Fatalf("bootstrap attempts = %d, want %d", bootstraps, test.bootstraps)
			}
		})
	}
}

// TestApplyFreshLeavesARacedForeignPlistUntouched is the race on the plist
// itself: another actor publishes one at the label after the presence check
// found none. The create-only publication refuses the occupied name, so those
// bytes stay exactly as written and launchd is never asked to do anything.
func TestApplyFreshLeavesARacedForeignPlistUntouched(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := applyAgent(t, "com.example.raced")
	path := filepath.Join(dir, agent.Label+".plist")
	foreign := []byte("<?xml version=\"1.0\"?>\n<plist><dict><key>Label</key><string>com.example.raced</string></dict></plist>\n")
	var verbs []string
	run := func(_ context.Context, _ string, args ...string) (string, int, error) {
		verbs = append(verbs, args[0])
		if args[0] == "print" {
			if err := os.WriteFile(path, foreign, 0o644); err != nil {
				t.Errorf("plant the raced plist: %v", err)
			}
			return "Could not find service", launchctlNotFoundExit, nil
		}
		return "", 0, nil
	}

	if err := ApplyFresh(context.Background(), run, agent); !errors.Is(err, ErrPresent) {
		t.Fatalf("ApplyFresh = %v, want ErrPresent", err)
	}

	if want := []string{"print"}; !slices.Equal(verbs, want) {
		t.Fatalf("verbs = %v, want %v and no enable, bootstrap, kickstart, or bootout", verbs, want)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(foreign) {
		t.Fatalf("the raced plist was rewritten\n%s", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("the raced plist mode = %#o, want its own 0644", info.Mode().Perm())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("LaunchAgents holds %d entries, want only the raced plist and no staged temp", len(entries))
	}
}
