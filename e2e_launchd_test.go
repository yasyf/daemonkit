//go:build darwin

// This file is daemonkit's only real-machine coverage: it bootstraps genuine
// user LaunchAgents into gui/<uid>, lets launchd exec and supervise a real
// daemon, and drives Ensure, Control.Drain, and the shutdown ladder against
// them. Every other suite replaces /bin/launchctl with a recorder, so nothing
// else in the tree has ever observed launchd's own behaviour: the exit-code
// classifier, Verify's idempotence claim, the abandoned-stage park, and the
// ExitTimeOut SIGKILL backstop are all covered here and nowhere else.
//
// It mutates the machine, so it is opt-in: set DAEMONKIT_E2E_LAUNCHD=1 and it
// runs, otherwise every test skips. Everything it installs carries the
// e2eLabelPrefix, every label is fixed rather than minted per run (launchctl
// enable writes a row into the root-owned disabled.<uid>.plist that no verb
// can remove, so a per-run label would leak one row per run forever), and
// sweepLabel boots out and unlinks each one on every exit path.
package daemonkit_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

const (
	// e2eEnv opts a run in. Without it every test here skips, so a routine
	// scripts/test.sh ./... never bootstraps a LaunchAgent.
	e2eEnv = "DAEMONKIT_E2E_LAUNCHD"
	// e2eLabelPrefix is what every label this file installs begins with, so a
	// stranded job is attributable on sight.
	e2eLabelPrefix = "com.yasyf.daemonkit.e2e."
	// helperRel is where the helper daemon sits inside the fake bundle.
	helperRel = "Contents/MacOS/dkhelper"
)

// e2eLabels is every label this file may ever install, and sweepLabel refuses
// anything outside it. The set is closed and fixed rather than minted per run
// on purpose: launchctl enable writes a row into the root-owned
// /var/db/com.apple.xpc.launchd/disabled.<uid>.plist, Remove only boots out and
// unlinks, and no launchctl verb deletes a row — so a per-run label would leak
// one permanent row per run, where a fixed set leaks len(e2eLabels) forever.
var e2eLabels = []string{
	e2eLabelPrefix + "ladder",
	e2eLabelPrefix + "wedgekill",
	e2eLabelPrefix + "wedgepark",
	e2eLabelPrefix + "wedgeclose",
	e2eLabelPrefix + "recover",
}

// lane is one scenario: a sandbox home under /private/tmp, a fake bundle
// holding the helper daemon, and the Daemon the launcher converges on.
type lane struct {
	t      *testing.T
	home   string
	label  string
	app    string
	daemon daemonkit.Daemon
}

// newLane stands the scenario up. The home lives under /private/tmp rather
// than t.TempDir because launchd.validateProgramTree refuses a symlinked
// component anywhere above the program and both /tmp and /var are symlinks,
// and because /var/folders + a reverse-DNS label overruns darwin's 104-byte
// sun_path.
func newLane(t *testing.T, label string, shutdown time.Duration, restart daemonkit.Restart) *lane {
	t.Helper()
	requireE2E(t)
	sweepLabel(t, label)

	home, err := os.MkdirTemp("/private/tmp", "dke-")
	if err != nil {
		t.Fatalf("sandbox home: %v", err)
	}
	t.Setenv(realhome.EnvOverride, home)
	app := filepath.Join(home, "Helper.app")
	if err := os.MkdirAll(filepath.Join(app, "Contents", "MacOS"), 0o755); err != nil {
		t.Fatalf("bundle skeleton: %v", err)
	}
	// launchd.install creates ~/Library/LaunchAgents with durable.Mkdir, not
	// MkdirAll, so the sandbox has to supply the ~/Library a real home always has.
	if err := os.MkdirAll(filepath.Join(home, "Library"), 0o700); err != nil {
		t.Fatalf("sandbox Library: %v", err)
	}
	l := &lane{t: t, home: home, label: label, app: app}
	l.buildHelper("a")
	program, err := daemonkit.InBundle(app, helperRel)
	if err != nil {
		t.Fatalf("InBundle: %v", err)
	}
	l.daemon = daemonkit.Daemon{
		Label:    daemonkit.Label(label),
		Program:  program,
		Args:     []string{home, label},
		Schemas:  []daemonkit.Schema{"daemonkit.e2e.v1"},
		Shutdown: daemonkit.Grace(shutdown),
		Restart:  restart,
	}
	socket, err := paths.Socket(label)
	if err != nil {
		t.Fatalf("socket path: %v", err)
	}
	t.Logf("lane home=%s label=%s socket=%s (%d bytes)", home, label, socket, len(socket))
	t.Cleanup(func() { l.teardown() })
	return l
}

// buildHelper compiles internal/e2ehelper into the bundle, stamping variant so
// two builds of the same source differ in every byte a digest reads.
func (l *lane) buildHelper(variant string) string {
	l.t.Helper()
	target := filepath.Join(l.app, helperRel)
	build := exec.Command(
		"go", "build",
		"-ldflags", "-X main.variant="+variant,
		"-o", target,
		"github.com/yasyf/daemonkit/internal/e2ehelper",
	)
	build.Env = append(os.Environ(), "GOFLAGS=")
	if out, err := build.CombinedOutput(); err != nil {
		l.t.Fatalf("build helper %q: %v\n%s", variant, err, out)
	}
	return target
}

func (l *lane) behavior(t *testing.T, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(l.home, "behavior.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write behavior: %v", err)
	}
}

func (l *lane) client() *daemonkit.Client { return daemonkit.Open(l.daemon) }

func (l *lane) ctx(budget time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	l.t.Cleanup(cancel)
	return ctx
}

func (l *lane) plistPath() string {
	return filepath.Join(l.home, "Library", "LaunchAgents", l.label+".plist")
}

func (l *lane) socket() string {
	socket, err := paths.Socket(l.label)
	if err != nil {
		l.t.Fatalf("socket path: %v", err)
	}
	return socket
}

// events reads the helper's own lifecycle log: one nanosecond timestamp and
// one edge per line, flushed as it is written, so the record left by a process
// SIGKILLed mid-ladder is exactly the edges it reached.
func (l *lane) events() []event {
	data, err := os.ReadFile(filepath.Join(l.home, "events.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		l.t.Fatalf("read events: %v", err)
	}
	var out []event
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		stamp, rest, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		nanos, err := parseInt(stamp)
		if err != nil {
			continue
		}
		out = append(out, event{At: time.Unix(0, nanos), Text: rest})
	}
	return out
}

func (l *lane) findEvent(prefix string) (event, bool) {
	for _, e := range l.events() {
		if strings.HasPrefix(e.Text, prefix) {
			return e, true
		}
	}
	return event{}, false
}

func (l *lane) hasEvent(prefix string) bool {
	_, ok := l.findEvent(prefix)
	return ok
}

func (l *lane) awaitEvent(prefix string, within time.Duration) (event, bool) {
	deadline := time.Now().Add(within)
	for {
		if e, ok := l.findEvent(prefix); ok {
			return e, true
		}
		if !time.Now().Before(deadline) {
			return event{}, false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (l *lane) lastEvent() (event, bool) {
	events := l.events()
	if len(events) == 0 {
		return event{}, false
	}
	return events[len(events)-1], true
}

func (l *lane) logEvents() {
	events := l.events()
	if len(events) == 0 {
		l.t.Logf("events: (none)")
		return
	}
	base := events[0].At
	var b strings.Builder
	for _, e := range events {
		fmt.Fprintf(&b, "\n  +%-10s %s", e.At.Sub(base).Round(time.Millisecond), e.Text)
	}
	l.t.Logf("events (t0=%s):%s", base.Format(time.RFC3339Nano), b.String())
}

// teardown runs on every exit path, success or failure: boot the job out of
// launchd, unlink the plist, kill anything still serving, and drop the home.
func (l *lane) teardown() {
	if pid := l.recordedPID(); pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	sweepLabel(l.t, l.label)
	_ = os.Remove(l.plistPath())
	_ = os.RemoveAll(l.home)
}

// recordedPID reads the pid out of the helper's own event log rather than the
// process table, so teardown can reach a daemon whose socket is already gone.
func (l *lane) recordedPID() int {
	last := 0
	for _, e := range l.events() {
		if _, rest, ok := strings.Cut(e.Text, "pid="); ok {
			field, _, _ := strings.Cut(rest, " ")
			if pid, err := parseInt(field); err == nil {
				last = int(pid)
			}
		}
	}
	return last
}

type event struct {
	At   time.Time
	Text string
}

func parseInt(s string) (int64, error) {
	var n int64
	if s == "" {
		return 0, errors.New("empty")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a number: %q", s)
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv(e2eEnv) != "1" {
		t.Skipf("set %s=1 to run the real-launchd end-to-end suite", e2eEnv)
	}
	out, code, err := runLaunchctl(context.Background(), "print", "gui/"+itoa(os.Getuid()))
	if err != nil || code != 0 {
		t.Skipf("no bootstrappable gui/%d domain (code=%d err=%v): %s", os.Getuid(), code, err, firstLine(out))
	}
}

func runLaunchctl(ctx context.Context, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, "/bin/launchctl", args...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}

func serviceTarget(label string) string { return "gui/" + itoa(os.Getuid()) + "/" + label }

func itoa(n int) string { return fmt.Sprintf("%d", n) }

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}

// sweepLabel boots one label out of launchd unconditionally. It runs before a
// lane installs anything and again in teardown, because launchd.Remove proves
// ownership from the plist at daemonkit's own path and a run whose sandbox home
// is already gone would leave the bootstrapped job behind.
func sweepLabel(t *testing.T, label string) {
	t.Helper()
	if !strings.HasPrefix(label, e2eLabelPrefix) || !slices.Contains(e2eLabels, label) {
		t.Fatalf("refusing to sweep %q: not one of this suite's declared labels %v", label, e2eLabels)
	}
	_, _, _ = runLaunchctl(context.Background(), "bootout", serviceTarget(label))
}

// pidAlive is the departure probe. It is signal 0 against a pid, which cannot
// distinguish a reused pid from a live one — adequate over the seconds-long
// windows measured here, and never the proof daemonkit's own Reap is.
func pidAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// awaitDeparture polls until the pid leaves the process table and returns how
// long that took.
func awaitDeparture(pid int, within time.Duration) (time.Duration, bool) {
	start := time.Now()
	deadline := start.Add(within)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return time.Since(start), true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return time.Since(start), false
}

// TestE2ELaunchdEnsureLadder is items 1 and 3: a real LaunchAgent converges, a
// second pass is a proven no-op against launchd's own view, a rebuilt program
// upgrades, and Control.Drain against the live daemon is followed by the
// daemon's actual departure from the process table.
func TestE2ELaunchdEnsureLadder(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"ladder", 6*time.Second, daemonkit.RestartNever)
	client := l.client()

	started := time.Now()
	first, err := client.Ensure(l.ctx(120 * time.Second))
	firstTook := time.Since(started)
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure #1 = %v", err)
	}
	t.Logf("Ensure #1: did=%s pid=%d build=%s took=%s",
		first.Did, first.After.PID, short(first.After.Build), firstTook.Round(time.Millisecond))
	if first.Did != daemonkit.ActionStarted {
		t.Errorf("Ensure #1 Did = %s, want started", first.Did)
	}
	if first.After.Phase != daemonkit.PhaseReady {
		t.Errorf("Ensure #1 After.Phase = %d, want PhaseReady", first.After.Phase)
	}
	if first.After.PID == os.Getpid() || !pidAlive(first.After.PID) {
		t.Errorf("Ensure #1 After.PID = %d, want a live process that is not this test", first.After.PID)
	}

	// launchd's own view, asked directly rather than through the package.
	print1, code, err := runLaunchctl(l.ctx(20*time.Second), "print", serviceTarget(l.label))
	if err != nil || code != 0 {
		t.Fatalf("launchctl print = code %d err %v: %s", code, err, print1)
	}
	if got := launchctlField(print1, "pid"); got != itoa(first.After.PID) {
		t.Errorf("launchctl print pid = %q, want the daemon's %d", got, first.After.PID)
	}
	t.Logf("launchctl print: state=%q pid=%q program=%q",
		launchctlField(print1, "state"), launchctlField(print1, "pid"), launchctlField(print1, "program"))

	info, err := os.Stat(l.plistPath())
	if err != nil {
		t.Fatalf("plist: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("plist mode = %v, want 0600", info.Mode().Perm())
	}
	plist, err := os.ReadFile(l.plistPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plist), launchd.OwnerEnvKey) {
		t.Errorf("plist carries no %s ownership marker", launchd.OwnerEnvKey)
	}

	// Idempotence, against launchd's real `print` rather than a recorder.
	started = time.Now()
	second, err := client.Ensure(l.ctx(120 * time.Second))
	secondTook := time.Since(started)
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure #2 = %v", err)
	}
	t.Logf("Ensure #2: did=%s pid=%d took=%s", second.Did, second.After.PID, secondTook.Round(time.Millisecond))
	if second.Did != daemonkit.ActionNothing {
		t.Errorf("Ensure #2 Did = %s, want nothing", second.Did)
	}
	if second.After.PID != first.After.PID {
		t.Errorf("Ensure #2 replaced pid %d with %d; a converged system must not restart",
			first.After.PID, second.After.PID)
	}
	exact, err := launchd.Verify(l.ctx(20*time.Second), launchctlRunner, mustAgent(t, l))
	if err != nil {
		t.Fatalf("launchd.Verify = %v", err)
	}
	if !exact {
		t.Errorf("launchd.Verify = false against a job launchd itself reports bootstrapped")
	}

	// Upgrade: same path, different bytes.
	l.buildHelper("b")
	started = time.Now()
	third, err := client.Ensure(l.ctx(120 * time.Second))
	thirdTook := time.Since(started)
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure #3 (upgrade) = %v", err)
	}
	t.Logf("Ensure #3 (upgrade): did=%s pid=%d build=%s took=%s",
		third.Did, third.After.PID, short(third.After.Build), thirdTook.Round(time.Millisecond))
	if third.Did != daemonkit.ActionUpgraded {
		t.Errorf("Ensure #3 Did = %s, want upgraded", third.Did)
	}
	if third.After.Build == first.After.Build {
		t.Errorf("Ensure #3 Build = %s, want the rebuilt program's digest", short(third.After.Build))
	}
	if third.After.PID == first.After.PID {
		t.Errorf("Ensure #3 kept pid %d; an upgrade must replace the process", first.After.PID)
	}
	if _, gone := awaitDeparture(first.After.PID, 15*time.Second); !gone {
		t.Errorf("the upgraded-away daemon %d is still in the process table", first.After.PID)
	}

	// Control.Drain against the live daemon, then its real departure.
	ctx := l.ctx(60 * time.Second)
	control, err := client.Control(ctx)
	if err != nil {
		t.Fatalf("Control() = %v", err)
	}
	health, err := control.Health(ctx)
	if err != nil {
		t.Fatalf("Health() = %v", err)
	}
	drainStart := time.Now()
	stopped, err := control.Drain(ctx, daemonkit.Expect{Build: health.Build, Generation: health.Generation})
	drainTook := time.Since(drainStart)
	if err != nil {
		l.logEvents()
		t.Fatalf("Drain() = %v", err)
	}
	t.Logf("Control.Drain: reap=%d before.pid=%d took=%s", stopped.Reap, stopped.Before.PID, drainTook.Round(time.Millisecond))
	if stopped.Reap != daemonkit.ReapAbsent {
		t.Errorf("Drain Reap = %d, want ReapAbsent", stopped.Reap)
	}
	if pidAlive(third.After.PID) {
		t.Errorf("drained daemon %d is still in the process table", third.After.PID)
	}
	_ = control.Close(l.ctx(10 * time.Second))
	l.logEvents()

	t.Logf("TIMINGS ladder: start=%s noop=%s upgrade=%s drain=%s",
		firstTook.Round(time.Millisecond), secondTook.Round(time.Millisecond),
		thirdTook.Round(time.Millisecond), drainTook.Round(time.Millisecond))
}

// TestE2ELaunchdBootoutKillsTheParkedProcess is item 5's first half and the
// one path nothing in the tree has ever observed: a product whose Drain
// outlives its share leaves the ladder with an abandoned stage, Serve parks
// holding the flock, and launchd's ExitTimeOut SIGKILL is the only thing that
// can release it. ExitTimeOut is Daemon.Shutdown, and the ladder spends
// Daemon.Shutdown, so the park's whole lifetime is whatever the ladder
// under-spends — this measures it.
func TestE2ELaunchdBootoutKillsTheParkedProcess(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"wedgekill", 6*time.Second, daemonkit.RestartNever)
	l.behavior(t, `{"drain":"120s"}`)
	client := l.client()

	ensured, err := client.Ensure(l.ctx(120 * time.Second))
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure = %v", err)
	}
	pid := ensured.After.PID
	t.Logf("wedged daemon: pid=%d exitTimeOut=%s", pid, plistExitTimeOut(t, l))

	bootoutAt := time.Now()
	out, code, err := runLaunchctl(l.ctx(30*time.Second), "bootout", serviceTarget(l.label))
	t.Logf("launchctl bootout: code=%d err=%v out=%q returned after %s",
		code, err, strings.TrimSpace(out), time.Since(bootoutAt).Round(time.Millisecond))

	departure, gone := awaitDeparture(pid, 90*time.Second)
	if !gone {
		l.logEvents()
		t.Fatalf("daemon %d never left the process table after bootout", pid)
	}
	l.logEvents()

	drainEnter, sawDrain := l.findEvent("drain.enter")
	lastLadder, sawLadder := l.lastEvent()
	parked := !l.hasEvent("serve.return")
	t.Logf("TIMINGS wedge/kill: bootout→gone=%s drain.enter=+%s ladder-last-edge=+%s parked-for≈%s",
		departure.Round(time.Millisecond),
		durationOr(sawDrain, drainEnter.At.Sub(bootoutAt)),
		durationOr(sawLadder, lastLadder.At.Sub(bootoutAt)),
		durationOr(sawLadder, bootoutAt.Add(departure).Sub(lastLadder.At)))

	if !sawDrain {
		t.Errorf("the product's Drain never ran; the bootout SIGTERM did not reach the ladder")
	}
	if l.hasEvent("drain.exit") {
		t.Errorf("drain.exit was reached; the wedge did not wedge")
	}
	if !parked {
		t.Errorf("Serve returned rather than parking: the abandoned stage did not retain the flock")
	}
	// The whole point: the process left at the plist's ExitTimeOut, not at the
	// end of its own ladder, so what removed it was launchd's SIGKILL.
	exitTimeOut := time.Duration(l.daemon.Shutdown)
	if departure < exitTimeOut-time.Second || departure > exitTimeOut+3*time.Second {
		t.Errorf("bootout→gone = %s, want ≈ExitTimeOut %s: the SIGKILL backstop is what should have removed a parked process",
			departure.Round(time.Millisecond), exitTimeOut)
	}
	t.Logf("verdict: Serve never returned, so the process was parked holding the flock; "+
		"launchd removed it %s after bootout against an ExitTimeOut of %s — "+
		"the SIGKILL backstop fired, and it is the only thing that released the flock",
		departure.Round(time.Millisecond), exitTimeOut)
}

// TestE2ELaunchdLateStageWedgeStillDiesAtExitTimeOut wedges the LAST product
// stage rather than the middle one. Close takes Share(1.0) — everything the
// work window has left — so the ladder spends nearly the whole Shutdown budget
// before it abandons and parks, where a wedged Drain leaves the later stages
// time to finish early.
//
// What this can and cannot see is worth stating, because it bounds the claim:
// the park itself emits no edge, and with the last stage wedged nothing runs
// after it, so park entry is not directly observable in this shape — only the
// budget arithmetic predicts it (Close's share is what the work window has left
// after an instant Drain, ≈5.05s of 6s, leaving ≈0.9s of park). What IS
// observed is that the process still leaves at exactly ExitTimeOut and Serve
// still never returns: whichever stage wedges, the SIGKILL is what ends it.
func TestE2ELaunchdLateStageWedgeStillDiesAtExitTimeOut(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"wedgeclose", 6*time.Second, daemonkit.RestartNever)
	l.behavior(t, `{"close":"600s"}`)
	client := l.client()

	ensured, err := client.Ensure(l.ctx(120 * time.Second))
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure = %v", err)
	}
	pid := ensured.After.PID

	bootoutAt := time.Now()
	if _, code, err := runLaunchctl(l.ctx(30*time.Second), "bootout", serviceTarget(l.label)); err != nil {
		t.Fatalf("bootout = code %d err %v", code, err)
	}
	departure, gone := awaitDeparture(pid, 90*time.Second)
	if !gone {
		l.logEvents()
		t.Fatalf("daemon %d never left after bootout", pid)
	}
	l.logEvents()

	closeEnter, sawClose := l.findEvent("close.enter")
	exitTimeOut := time.Duration(l.daemon.Shutdown)
	t.Logf("TIMINGS wedge/close: bootout→gone=%s (ExitTimeOut=%s) close.enter=+%s; "+
		"no edge follows a wedged last stage, so park entry is unobservable here",
		departure.Round(time.Millisecond), exitTimeOut,
		durationOr(sawClose, closeEnter.At.Sub(bootoutAt)))

	if !sawClose {
		t.Errorf("the product's Close never ran")
	}
	if l.hasEvent("close.exit") {
		t.Errorf("close.exit was reached; the wedge did not wedge")
	}
	if l.hasEvent("serve.return") {
		t.Errorf("Serve returned rather than parking")
	}
	if departure < exitTimeOut-time.Second || departure > exitTimeOut+3*time.Second {
		t.Errorf("bootout→gone = %s, want ≈ExitTimeOut %s", departure.Round(time.Millisecond), exitTimeOut)
	}
	t.Logf("verdict: a wedge in the last product stage ends the same way as one in the middle — "+
		"Serve never returns and launchd's SIGKILL removes the process at ExitTimeOut (%s). "+
		"What differs is only how much park precedes it, which this shape cannot measure.",
		departure.Round(time.Millisecond))
}

// TestE2ELaunchdParkExitsOnASignal is item 5's second half, isolated from the
// ExitTimeOut race: the same wedge, but the plist's ExitTimeOut is far longer
// than the helper's own ladder, so the park survives to be signalled. It proves
// the park's own registration is live — that a parked process still answers
// SIGTERM after the drain-trigger registration was dropped.
func TestE2ELaunchdParkExitsOnASignal(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"wedgepark", 120*time.Second, daemonkit.RestartNever)
	l.behavior(t, `{"drain":"600s"}`)
	client := l.client()

	ensured, err := client.Ensure(l.ctx(180 * time.Second))
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure = %v", err)
	}
	pid := ensured.After.PID
	t.Logf("wedged daemon: pid=%d exitTimeOut=%s (helper ladder is 6s)", pid, plistExitTimeOut(t, l))

	bootoutAt := time.Now()
	if _, code, err := runLaunchctl(l.ctx(30*time.Second), "bootout", serviceTarget(l.label)); err != nil {
		t.Fatalf("bootout = code %d err %v", code, err)
	}
	if _, ok := l.awaitEvent("drain.enter", 30*time.Second); !ok {
		l.logEvents()
		t.Fatalf("the bootout SIGTERM never reached the ladder")
	}

	// The ladder spends the helper's own 6s Shutdown; give it room, then check
	// the process is still there, holding the flock over half-done work.
	time.Sleep(10 * time.Second)
	if !pidAlive(pid) {
		l.logEvents()
		t.Fatalf("daemon %d left on its own %s after bootout; the park did not hold",
			pid, time.Since(bootoutAt).Round(time.Millisecond))
	}
	t.Logf("park is holding %s after bootout, ExitTimeOut %s away", time.Since(bootoutAt).Round(time.Millisecond), plistExitTimeOut(t, l))

	signalAt := time.Now()
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	departure, gone := awaitDeparture(pid, 30*time.Second)
	l.logEvents()
	if !gone {
		t.Fatalf("parked daemon %d ignored SIGTERM", pid)
	}
	t.Logf("TIMINGS wedge/park: bootout→park-held=%s SIGTERM→gone=%s",
		signalAt.Sub(bootoutAt).Round(time.Millisecond), departure.Round(time.Millisecond))
	if !l.hasEvent("serve.return") {
		t.Errorf("Serve never returned; the park did not unblock on SIGTERM")
	}
}

// TestE2ELaunchdSuccessorRecoversAfterAKillMidDrain is item 4: the daemon is
// SIGKILLed while its product is inside Drain, which is the one moment it holds
// the record-store flock, the socket, and a half-run ladder at once. The next
// Ensure must take that state over rather than wedge on it.
func TestE2ELaunchdSuccessorRecoversAfterAKillMidDrain(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"recover", 6*time.Second, daemonkit.RestartNever)
	l.behavior(t, `{"drain":"600s"}`)
	client := l.client()

	ensured, err := client.Ensure(l.ctx(120 * time.Second))
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure #1 = %v", err)
	}
	victim := ensured.After.PID
	t.Logf("victim daemon: pid=%d generation=%d", victim, ensured.After.Generation)

	// Drive the drain through the wire verb, not a signal, so the process is
	// killed inside the product's own Drain rather than before the ladder.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		control, err := client.Control(ctx)
		if err != nil {
			return
		}
		defer func() { _ = control.Close(ctx) }()
		_, _ = control.Drain(ctx, daemonkit.Expect{})
	}()
	if _, ok := l.awaitEvent("drain.enter", 30*time.Second); !ok {
		l.logEvents()
		t.Fatalf("the drain verb never reached the product")
	}
	killAt := time.Now()
	if err := syscall.Kill(victim, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	departure, gone := awaitDeparture(victim, 30*time.Second)
	if !gone {
		t.Fatalf("SIGKILLed daemon %d is still in the process table", victim)
	}
	t.Logf("killed mid-drain: pid=%d gone in %s", victim, departure.Round(time.Millisecond))
	if l.hasEvent("serve.return") {
		t.Errorf("a SIGKILLed daemon reported a completed Serve")
	}
	if _, err := os.Stat(l.socket()); err != nil {
		t.Logf("socket after the kill: %v", err)
	} else {
		t.Logf("socket after the kill: still on disk (stale), as expected")
	}

	// The successor: a fresh Ensure over a stale socket, a released flock, and
	// an owner record naming a dead process.
	l.behavior(t, `{}`)
	recoverStart := time.Now()
	successor, err := client.Ensure(l.ctx(150 * time.Second))
	recoverTook := time.Since(recoverStart)
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure after the kill = %v (took %s)", err, recoverTook.Round(time.Millisecond))
	}
	l.logEvents()
	t.Logf("TIMINGS recover: kill→gone=%s successor Ensure=%s did=%s pid=%d",
		departure.Round(time.Millisecond), recoverTook.Round(time.Millisecond),
		successor.Did, successor.After.PID)
	if successor.After.Phase != daemonkit.PhaseReady {
		t.Errorf("successor Phase = %d, want PhaseReady", successor.After.Phase)
	}
	if successor.After.PID == victim {
		t.Errorf("successor PID = %d, the killed process", victim)
	}
	if successor.After.Generation == ensured.After.Generation {
		t.Errorf("successor reused generation %d", successor.After.Generation)
	}
	_ = killAt
}

// TestE2ELaunchdRemoveLeavesNothingBehind is item 6's independent verification:
// after launchd.Remove, launchd itself must not know the label, the plist must
// be gone, and the daemon must be out of the process table.
func TestE2ELaunchdRemoveLeavesNothingBehind(t *testing.T) {
	l := newLane(t, e2eLabelPrefix+"ladder", 6*time.Second, daemonkit.RestartNever)
	client := l.client()

	ensured, err := client.Ensure(l.ctx(120 * time.Second))
	if err != nil {
		l.logEvents()
		t.Fatalf("Ensure = %v", err)
	}
	pid := ensured.After.PID

	removeStart := time.Now()
	if err := launchd.Remove(l.ctx(60*time.Second), launchctlRunner, l.label); err != nil {
		t.Fatalf("launchd.Remove = %v", err)
	}
	removeTook := time.Since(removeStart)
	departure, gone := awaitDeparture(pid, 60*time.Second)
	l.logEvents()
	t.Logf("TIMINGS remove: launchd.Remove=%s bootout→gone=%s",
		removeTook.Round(time.Millisecond), departure.Round(time.Millisecond))
	if !gone {
		t.Fatalf("daemon %d survived launchd.Remove", pid)
	}
	if _, err := os.Stat(l.plistPath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plist still at %s: %v", l.plistPath(), err)
	}
	out, code, _ := runLaunchctl(l.ctx(20*time.Second), "print", serviceTarget(l.label))
	if code == 0 {
		t.Errorf("launchd still knows %s after Remove:\n%s", l.label, out)
	}
	t.Logf("launchctl print after Remove: code=%d %q", code, firstLine(out))

	// Remove is idempotent against a label launchd no longer knows.
	if err := launchd.Remove(l.ctx(30*time.Second), launchctlRunner, l.label); err != nil {
		t.Errorf("second launchd.Remove = %v, want nil", err)
	}
}

// launchctlRunner is the same real-process runner Ensure uses, exposed here so
// the exported launchd verbs can be driven directly.
func launchctlRunner(ctx context.Context, path string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, path, args...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}

func mustAgent(t *testing.T, l *lane) launchd.Agent {
	t.Helper()
	data, err := os.ReadFile(l.plistPath())
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	return launchd.Agent{
		Label:         l.label,
		Program:       filepath.Join(l.app, helperRel),
		Args:          l.daemon.Args,
		LogPath:       plistValue(string(data), "StandardOutPath"),
		RestartPolicy: launchd.NoRestart,
		ExitTimeOut:   time.Duration(l.daemon.Shutdown),
	}
}

func plistExitTimeOut(t *testing.T, l *lane) string {
	t.Helper()
	data, err := os.ReadFile(l.plistPath())
	if err != nil {
		return "?"
	}
	return plistValue(string(data), "ExitTimeOut") + "s"
}

// plistValue pulls the scalar that follows a key in daemonkit's own rendered
// plist. It reads that exact shape and nothing more general.
func plistValue(plist, key string) string {
	_, rest, ok := strings.Cut(plist, "<key>"+key+"</key>")
	if !ok {
		return ""
	}
	_, rest, ok = strings.Cut(rest, ">")
	if !ok {
		return ""
	}
	value, _, _ := strings.Cut(rest, "<")
	return strings.TrimSpace(value)
}

// launchctlField reads one "key = value" line out of launchctl print's output.
func launchctlField(out, key string) string {
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		name, value, ok := strings.Cut(trimmed, " = ")
		if ok && name == key {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func short(build string) string {
	if len(build) > 12 {
		return build[:12]
	}
	return build
}

func durationOr(ok bool, d time.Duration) string {
	if !ok {
		return "n/a"
	}
	return d.Round(time.Millisecond).String()
}
