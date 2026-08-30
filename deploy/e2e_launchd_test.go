//go:build darwin && dke2e

// This file is deploy's real-machine coverage. The rest of the package drives
// a launchctl double, so the verb chain never drives /bin/launchctl, never has
// launchd exec a daemon, and never has its quiesce gate read the real process
// table.
//
// One relaxation, forced rather than chosen: the designated requirement deploy
// renders demands a Developer ID Application anchor (`anchor apple generic` +
// the Team OU + the DevID leaf OIDs), and no machine that builds this module
// holds an identity that can mint one. The bundle is ad-hoc signed and the DR
// narrows to its identifier clause, exactly as every other test in the package
// does. Everything else is real: codesign, the tree copy, the swap record, the
// rename pair, launchctl bootstrap/bootout, the launchd-spawned daemon,
// Control.Drain, and the executable-scoped inventory.
//
// The dke2e tag keeps it out of scripts/test.sh, because bootstrapping a real
// LaunchAgent leaks a permanent row into the root-owned
// /var/db/com.apple.xpc.launchd/disabled.<uid>.plist that no launchctl verb
// removes. Its sibling at _e2e/launchd_test.go needs no tag — the go tool
// skips an underscore-prefixed directory — but this one reads
// deployment.requirement, which is package-private, so it cannot move out of
// package deploy. scripts/e2e-launchd.sh is the only thing that runs it, and a
// human invokes that script. Nothing here is conditional: no env gate, no skip.
package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/launchd"
)

const (
	e2eAgentLabel   = "com.yasyf.daemonkit.e2e.deployagent"
	e2eDaemonLabel  = "com.yasyf.daemonkit.e2e.deploy"
	e2eHelperRel    = "Contents/MacOS/dkhelper"
	e2eBundleName   = "Example"
	e2eLabelPrefix  = "com.yasyf.daemonkit.e2e."
	e2eLaunchctlBin = "/bin/launchctl"
)

// e2eFixture is one deployment over a real install root with a real launchd.
type e2eFixture struct {
	t      *testing.T
	root   string
	home   string
	app    string
	deploy *Deployment
	agent  launchd.Agent
}

func newE2EFixture(t *testing.T) *e2eFixture {
	t.Helper()
	bootoutE2E(t, e2eAgentLabel)

	// The install root must be a canonical real path — layout.ensureMetadata
	// refuses one EvalSymlinks rewrites — and both /tmp and /var are symlinks.
	root, err := os.MkdirTemp("/private/tmp", "dkd-")
	if err != nil {
		t.Fatalf("install root: %v", err)
	}
	home := shortHome(t)
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(root, e2eBundleName+".app")
	agent := launchd.Agent{
		Label:         e2eAgentLabel,
		Program:       filepath.Join(app, e2eHelperRel),
		Args:          []string{home, e2eDaemonLabel},
		LogPath:       filepath.Join(root, "daemon.log"),
		RestartPolicy: launchd.NoRestart,
		ExitTimeOut:   6 * time.Second,
	}
	deployment, err := Open(Config{
		App:         app,
		Requirement: daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning},
		Daemon: daemonkit.Daemon{
			Label:    daemonkit.Label(e2eDaemonLabel),
			Schemas:  []daemonkit.Schema{"daemonkit.e2e.v1"},
			Trust:    daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
			Shutdown: daemonkit.Grace(6 * time.Second),
		},
		Agents: []launchd.Agent{agent},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// codesign, launchctl, and the process table are all real here. The one
	// relaxation is the designated requirement: the bundle is ad-hoc signed,
	// because no Developer ID identity exists on a developer machine or a CI
	// runner to sign it any other way. See adhocRequirement.
	deployment.requirement = adhocRequirement

	f := &e2eFixture{t: t, root: root, home: home, app: app, deploy: deployment, agent: agent}
	t.Cleanup(f.teardown)
	return f
}

// e2eCandidate builds a real helper daemon into an ad-hoc signed .app and
// returns the candidate naming it. variant is stamped through -ldflags so two
// candidates differ in every byte the tree digest reads.
func (f *e2eFixture) e2eCandidate(name, version, variant string) Candidate {
	f.t.Helper()
	source := filepath.Join(f.root, name+".app")
	macOS := filepath.Join(source, "Contents", "MacOS")
	if err := os.MkdirAll(macOS, 0o755); err != nil {
		f.t.Fatal(err)
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleShortVersionString</key><string>%s</string>
<key>CFBundleExecutable</key><string>dkhelper</string>
<key>CFBundleIdentifier</key><string>%s</string>
</dict></plist>`, version, testSigning)
	if err := os.WriteFile(filepath.Join(source, "Contents", "Info.plist"), []byte(plist), 0o644); err != nil {
		f.t.Fatal(err)
	}
	build := exec.Command(
		"go", "build",
		"-ldflags", "-X main.variant="+variant,
		"-o", filepath.Join(macOS, "dkhelper"),
		"github.com/yasyf/daemonkit/internal/e2ehelper",
	)
	build.Dir = ".."
	build.Env = append(os.Environ(), "GOFLAGS=")
	if out, err := build.CombinedOutput(); err != nil {
		f.t.Fatalf("build helper %q: %v\n%s", variant, err, out)
	}
	signBundle(f.t, source)
	digest, err := BundleDigest(source)
	if err != nil {
		f.t.Fatal(err)
	}
	return Candidate{Source: source, Version: version, Digest: digest}
}

func (f *e2eFixture) e2eCtx(budget time.Duration) context.Context {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	f.t.Cleanup(cancel)
	return ctx
}

func (f *e2eFixture) events() string {
	data, err := os.ReadFile(filepath.Join(f.home, "events.log"))
	if err != nil {
		return "(none)"
	}
	return "\n  " + strings.ReplaceAll(strings.TrimSpace(string(data)), "\n", "\n  ")
}

func (f *e2eFixture) teardown() {
	bootoutE2E(f.t, e2eAgentLabel)
	plist := filepath.Join(f.home, "Library", "LaunchAgents", e2eAgentLabel+".plist")
	_ = os.Remove(plist)
	_ = os.RemoveAll(f.root)
}

func e2eLaunchctl(ctx context.Context, args ...string) (string, int, error) {
	out, err := exec.CommandContext(ctx, e2eLaunchctlBin, args...).CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}

// bootoutE2E boots one label out unconditionally, before the fixture installs
// anything and again in teardown: converge proves ownership from the plist at
// daemonkit's own path, so a run whose sandbox home is already gone would
// leave the bootstrapped job behind.
func bootoutE2E(t *testing.T, label string) {
	t.Helper()
	if !strings.HasPrefix(label, e2eLabelPrefix) {
		t.Fatalf("refusing to boot out %q: not an e2e label", label)
	}
	_, _, _ = e2eLaunchctl(context.Background(), "bootout", fmt.Sprintf("gui/%d/%s", os.Getuid(), label))
}

// TestE2EDeployInstallActivateSupersedeUninstall is the whole verb chain
// against real launchd: Install lands the bytes, Activate bootstraps the agent
// and seals what the launchd-spawned daemon proves about itself, Supersede
// quiesces that live daemon through Control.Drain and the real process-table
// inventory before it swaps, a second Activate re-proves readiness on the new
// generation, and Uninstall gates on both halves before the bytes go.
func TestE2EDeployInstallActivateSupersedeUninstall(t *testing.T) {
	f := newE2EFixture(t)

	first := f.e2eCandidate("Source1", "1.0.0", "one")
	installStart := time.Now()
	installed, err := f.deploy.Install(f.e2eCtx(120*time.Second), first)
	installTook := time.Since(installStart)
	if err != nil {
		t.Fatalf("Install = %v", err)
	}
	t.Logf("Install: version=%s digest=%s took=%s",
		installed.Version, installed.BundleDigest[:12], installTook.Round(time.Millisecond))
	if installed.Path != f.app || installed.Version != "1.0.0" {
		t.Fatalf("Install = %+v, want %q at 1.0.0", installed, f.app)
	}

	activateStart := time.Now()
	activation, err := f.deploy.Activate(f.e2eCtx(180 * time.Second))
	activateTook := time.Since(activateStart)
	if err != nil {
		t.Fatalf("Activate = %v\nevents:%s", err, f.events())
	}
	t.Logf("Activate: build=%s generation=%d took=%s",
		activation.Readiness.Build()[:12], activation.Readiness.Generation(), activateTook.Round(time.Millisecond))
	if activation.Generation.Version != "1.0.0" {
		t.Errorf("Activate generation = %s, want 1.0.0", activation.Generation.Version)
	}

	// launchd's own view of the agent deploy converged it to.
	print1, code, err := e2eLaunchctl(f.e2eCtx(20*time.Second), "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), e2eAgentLabel))
	if err != nil || code != 0 {
		t.Fatalf("launchctl print = code %d err %v", code, err)
	}
	t.Logf("launchd holds the activated agent: state=%q program=%q",
		e2eField(print1, "state"), e2eField(print1, "program"))

	// The real inventory must see the launchd-spawned daemon on the
	// deployment's own executables, which is the gate Supersede has to clear.
	executables, err := f.deploy.executables()
	if err != nil {
		t.Fatal(err)
	}
	survivors, err := Inventory(executables...)
	if err != nil {
		t.Fatalf("Inventory = %v", err)
	}
	t.Logf("Inventory before supersede: %d live, %d unnameable (executables=%d)",
		len(survivors.Live), len(survivors.Unnameable), len(executables))
	if len(survivors.Live) == 0 {
		t.Errorf("the real process table shows no process on %v while the daemon is serving", executables)
	}
	if err := f.deploy.requireEmpty(); !errors.Is(err, ErrLive) {
		t.Errorf("requireEmpty() = %v, want ErrLive while the daemon is up", err)
	}

	// Supersede: quiesce the live daemon, converge launchd empty, swap, and
	// re-activate onto the new generation.
	second := f.e2eCandidate("Source2", "2.0.0", "two")
	supersedeStart := time.Now()
	replaced, err := f.deploy.Supersede(f.e2eCtx(180*time.Second), second)
	supersedeTook := time.Since(supersedeStart)
	if err != nil {
		t.Fatalf("Supersede = %v\nevents:%s", err, f.events())
	}
	t.Logf("Supersede: version=%s took=%s", replaced.Version, supersedeTook.Round(time.Millisecond))
	if replaced.Version != "2.0.0" {
		t.Errorf("Supersede generation = %s, want 2.0.0", replaced.Version)
	}
	if _, code, _ := e2eLaunchctl(f.e2eCtx(20*time.Second), "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), e2eAgentLabel)); code == 0 {
		t.Errorf("launchd still holds %s after Supersede converged it away", e2eAgentLabel)
	}

	reactivateStart := time.Now()
	reactivation, err := f.deploy.Activate(f.e2eCtx(180 * time.Second))
	reactivateTook := time.Since(reactivateStart)
	if err != nil {
		t.Fatalf("Activate after Supersede = %v\nevents:%s", err, f.events())
	}
	t.Logf("Activate #2: build=%s generation=%d took=%s",
		reactivation.Readiness.Build()[:12], reactivation.Readiness.Generation(), reactivateTook.Round(time.Millisecond))
	if reactivation.Readiness.Build() == activation.Readiness.Build() {
		t.Errorf("Activate #2 proved the same build %s; the superseded generation is a different binary",
			activation.Readiness.Build()[:12])
	}

	// Uninstall gates on the quiesce proof and the real inventory.
	uninstallStart := time.Now()
	removal, err := f.deploy.Uninstall(f.e2eCtx(180 * time.Second))
	uninstallTook := time.Since(uninstallStart)
	if err != nil {
		t.Fatalf("Uninstall = %v\nevents:%s", err, f.events())
	}
	t.Logf("Uninstall: version=%s absent=%v took=%s",
		removal.Generation.Version, removal.Runtime.Absent(), uninstallTook.Round(time.Millisecond))
	if !removal.Runtime.Absent() {
		t.Errorf("Uninstall Runtime.Absent() = false, want a proven-absent daemon")
	}
	if fileExists(f.app) {
		t.Errorf("canonical app %s survived Uninstall", f.app)
	}
	t.Logf("events:%s", f.events())
	t.Logf("TIMINGS deploy: install=%s activate=%s supersede=%s reactivate=%s uninstall=%s",
		installTook.Round(time.Millisecond), activateTook.Round(time.Millisecond),
		supersedeTook.Round(time.Millisecond), reactivateTook.Round(time.Millisecond),
		uninstallTook.Round(time.Millisecond))
}

// TestE2EDeployResetReturnsTheMachineToClean is item 6: Reset is the
// teardown-of-last-resort, and this drives it against a live launchd-spawned
// daemon and then verifies independently — launchd's own view, the metadata
// tree, and the process table.
func TestE2EDeployResetReturnsTheMachineToClean(t *testing.T) {
	f := newE2EFixture(t)

	candidate := f.e2eCandidate("Source1", "1.0.0", "one")
	if _, err := f.deploy.Install(f.e2eCtx(120*time.Second), candidate); err != nil {
		t.Fatalf("Install = %v", err)
	}
	activation, err := f.deploy.Activate(f.e2eCtx(180 * time.Second))
	if err != nil {
		t.Fatalf("Activate = %v\nevents:%s", err, f.events())
	}
	t.Logf("activated generation %d before Reset", activation.Readiness.Generation())
	if !fileExists(f.deploy.layout.activation) {
		t.Fatal("Activate sealed no activation record")
	}

	resetStart := time.Now()
	if err := f.deploy.Reset(f.e2eCtx(180 * time.Second)); err != nil {
		t.Fatalf("Reset = %v\nevents:%s", err, f.events())
	}
	resetTook := time.Since(resetStart)
	t.Logf("TIMINGS deploy: reset=%s", resetTook.Round(time.Millisecond))

	for name, path := range map[string]string{
		"activation": f.deploy.layout.activation,
		"removal":    f.deploy.layout.removal,
		"swap":       f.deploy.layout.swap,
		"services":   f.deploy.layout.services,
		"prior":      f.deploy.layout.prior,
		"removed":    f.deploy.layout.removed,
		"candidate":  f.deploy.layout.candidate,
	} {
		if fileExists(path) {
			t.Errorf("Reset left the %s record at %s", name, path)
		}
	}
	if !fileExists(f.app) {
		t.Errorf("Reset destroyed the installed bytes at %s; it never may", f.app)
	}
	if _, code, _ := e2eLaunchctl(f.e2eCtx(20*time.Second), "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), e2eAgentLabel)); code == 0 {
		t.Errorf("launchd still holds %s after Reset converged empty", e2eAgentLabel)
	}
	executables, err := f.deploy.executables()
	if err != nil {
		t.Fatal(err)
	}
	survivors, err := Inventory(executables...)
	if err != nil {
		t.Fatal(err)
	}
	// Unnameable is machine-wide: a same-user process the kernel will not name
	// is evidence about no particular executable, and Inventory's own contract
	// says to ignore it where nothing was recorded. Live is the answer, and
	// requireEmpty is the gate deploy itself would run.
	if len(survivors.Live) != 0 {
		t.Errorf("Reset left %d live processes on the deployment's executables: %v",
			len(survivors.Live), survivors.Live)
	}
	if err := f.deploy.requireEmpty(); err != nil {
		t.Errorf("requireEmpty() after Reset = %v, want nil", err)
	}
	t.Logf("Inventory after Reset: %d live, %d unnameable (machine-wide, uncorrelated)",
		len(survivors.Live), len(survivors.Unnameable))
	t.Logf("after Reset: launchd does not know %s, the process table is empty on %d executables, "+
		"and the installed bytes are intact", e2eAgentLabel, len(executables))
}

func e2eField(out, key string) string {
	for _, line := range strings.Split(out, "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " = ")
		if ok && name == key {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
