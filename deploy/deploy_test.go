package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

const (
	daemonChildEnv   = "DAEMONKIT_DEPLOY_DAEMON_CHILD"
	daemonChildLabel = "DAEMONKIT_DEPLOY_DAEMON_CHILD_LABEL"
	daemonChildDelay = "DAEMONKIT_DEPLOY_DAEMON_CHILD_DELAY"
)

// TestMain branches on the child marker before m.Run so a spawned copy of this
// binary serves one daemon and exits instead of re-entering the suite — the
// re-exec fork bomb scripts/test.sh backstops.
func TestMain(m *testing.M) {
	if os.Getenv(daemonChildEnv) == "1" {
		serveDaemonChild()
	}
	os.Exit(m.Run())
}

// serveDaemonChild serves one daemon at the label its parent named and
// publishes readiness only after the named delay, so the parent's wait faces
// the daemon launchd has only just been asked to start. It never returns.
func serveDaemonChild() {
	delay, err := time.ParseDuration(os.Getenv(daemonChildDelay))
	if err != nil {
		fmt.Fprintf(os.Stderr, "deploy daemon child: %v\n", err)
		os.Exit(70)
	}
	_, err = daemonkit.Serve(
		context.Background(),
		daemonkit.Daemon{
			Label:    daemonkit.Label(os.Getenv(daemonChildLabel)),
			Schemas:  []daemonkit.Schema{"deploy.test.v1"},
			Shutdown: daemonkit.Grace(5 * time.Second),
		},
		func(daemonkit.Ctx) (daemonkit.Product, error) {
			time.Sleep(delay)
			return stubProduct{}, nil
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deploy daemon child: %v\n", err)
		os.Exit(71)
	}
	os.Exit(0)
}

type stubProduct struct{}

func (stubProduct) Handle(context.Context, daemonkit.Request) (daemonkit.Reply, error) {
	return daemonkit.Reply{}, errors.New("unused")
}

func (stubProduct) Drain(daemonkit.Budget) error { return nil }

func (stubProduct) Close(daemonkit.Budget) error { return nil }

// fakeVerifier attests a bundle by hashing its tree, so a test bundle carries
// a distinct CDHash per byte set without codesign ever running.
type fakeVerifier struct {
	fail error
}

func (v fakeVerifier) Verify(_ context.Context, appPath, _ string) (signatureAttestation, error) {
	if v.fail != nil {
		return signatureAttestation{}, v.fail
	}
	digest, err := bundleTreeDigest(appPath)
	if err != nil {
		return signatureAttestation{}, err
	}
	return signatureAttestation{CDHash: digest.String(), EntitlementsDigest: sha256.Sum256([]byte("entitlements"))}, nil
}

const (
	testTeamID  = "ABCDE12345"
	testSigning = "com.example.daemonkit.test"
)

// fixture is one deployment over a temporary install root, with codesign,
// launchctl, the LaunchAgents directory, and the process table all replaced by
// observable fakes.
type fixture struct {
	t          *testing.T
	root       string
	app        string
	agentsDir  string
	deploy     *Deployment
	agent      launchd.Agent
	launchctls [][]string
	live       []LiveProcess
	unnameable []LiveProcess
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agentsDir := filepath.Join(shortHome(t), "Library", "LaunchAgents")
	if err := os.MkdirAll(agentsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(root, "Example.app")
	agent := launchd.Agent{
		Label:         "com.example.daemonkit.test",
		Program:       filepath.Join(app, "Contents", "MacOS", "example"),
		LogPath:       filepath.Join(root, "daemon.log"),
		RestartPolicy: launchd.RestartOnFailure,
	}
	f := &fixture{t: t, root: root, app: app, agentsDir: agentsDir, agent: agent}
	deployment, err := Open(Config{
		App:         app,
		Requirement: daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning},
		Daemon:      daemonkit.Daemon{Label: daemonkit.Label("daemonkit-deploy-test-" + filepath.Base(root))},
		Agents:      []launchd.Agent{agent},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	deployment.verify = fakeVerifier{}
	deployment.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		return "", 0, nil
	}
	deployment.inventory = func(...string) (Survivors, error) {
		return Survivors{Live: f.live, Unnameable: f.unnameable}, nil
	}
	f.deploy = deployment
	return f
}

// recordOwner writes the durable owner record this deployment's daemon writes
// for itself before it binds, and returns the pin it names — the one identity
// the gate may hold an unnameable process against.
func (f *fixture) recordOwner(t *testing.T) proc.Identity {
	t.Helper()
	path := f.deploy.config.Daemon.RecordPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := proc.OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	owner, err := store.RecordOwner("test-build")
	if err != nil {
		t.Fatalf("RecordOwner: %v", err)
	}
	return owner.Identity()
}

// startDaemonChild spawns a real daemon at this deployment's label, publishing
// readiness delay after it binds. The daemon has to be another process: the
// control attach refuses to pin os.Getpid(), so a daemon served in the test
// process is one no client here can ever attach to.
func (f *fixture) startDaemonChild(delay time.Duration) *exec.Cmd {
	f.t.Helper()
	executable, err := os.Executable()
	if err != nil {
		f.t.Fatalf("os.Executable() = %v", err)
	}
	child := exec.Command(executable)
	child.Env = append(
		os.Environ(),
		daemonChildEnv+"=1",
		daemonChildLabel+"="+string(f.deploy.config.Daemon.Label),
		daemonChildDelay+"="+delay.String(),
	)
	child.Stderr = os.Stderr
	if err := child.Start(); err != nil {
		f.t.Fatalf("start daemon child: %v", err)
	}
	f.t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	return child
}

// squatSocket binds this deployment's socket path to a listener that accepts
// nothing, so every attach connects and then blocks out its whole clock in the
// handshake. It is the shape that turns a classifying attach into a raw
// transport timeout — a same-UID process holding the path, and no daemon
// behind it.
func (f *fixture) squatSocket() {
	f.t.Helper()
	socket, err := paths.Socket(string(f.deploy.config.Daemon.Label))
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		f.t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = listener.Close() })
}

// shortHome stands up the passwd home every LaunchAgent path and the daemon
// socket derive from. It lives under /tmp because t.TempDir routinely exceeds
// darwin's 104-byte sun_path.
func shortHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-deploy-%d-", os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv(realhome.EnvOverride, home)
	return home
}

func (f *fixture) ctx() context.Context {
	f.t.Helper()
	return f.within(20 * time.Second)
}

// within bounds the readiness wait: proving readiness now subscribes until the
// deadline rather than dialing once, so a test that means "nothing is serving"
// says how long it is willing to wait for that answer.
func (f *fixture) within(budget time.Duration) context.Context {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.t.Context(), budget)
	f.t.Cleanup(cancel)
	return ctx
}

// bundle writes a minimal .app whose Info.plist declares version and whose
// executable carries body, so two candidates differ in every digest.
func (f *fixture) bundle(name, version, body string) string {
	f.t.Helper()
	path := filepath.Join(f.root, name+".app")
	macOS := filepath.Join(path, "Contents", "MacOS")
	if err := os.MkdirAll(macOS, 0o755); err != nil {
		f.t.Fatal(err)
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleShortVersionString</key><string>%s</string>
</dict></plist>`, version)
	if err := os.WriteFile(filepath.Join(path, "Contents", "Info.plist"), []byte(plist), 0o644); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(macOS, "example"), []byte(body), 0o755); err != nil {
		f.t.Fatal(err)
	}
	return path
}

func (f *fixture) candidate(name, version, body string) Candidate {
	f.t.Helper()
	source := f.bundle(name, version, body)
	digest, err := bundleTreeDigest(source)
	if err != nil {
		f.t.Fatal(err)
	}
	return Candidate{Source: source, Version: version, Digest: digest}
}

func TestInstallLandsAndActivateRefusesWithoutADaemon(t *testing.T) {
	f := newFixture(t)
	candidate := f.candidate("Source", "1.0", "one")
	installed, err := f.deploy.Install(f.ctx(), candidate)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if installed.Path != f.app || installed.Version != "1.0" {
		t.Fatalf("Install = %+v, want %q at version 1.0", installed, f.app)
	}
	if installed.BundleDigest != candidate.Digest.String() {
		t.Fatalf("installed digest = %q, want %q", installed.BundleDigest, candidate.Digest)
	}
	if installed.TeamID != testTeamID || installed.SigningIdentifier != testSigning {
		t.Fatalf("installed identity = %q/%q", installed.TeamID, installed.SigningIdentifier)
	}
	if fileExists(f.deploy.layout.swap) || fileExists(f.deploy.layout.candidate) {
		t.Fatal("Install left the swap record or the candidate slot behind")
	}
	if _, err := f.deploy.Install(f.ctx(), candidate); !errors.Is(err, ErrConflict) {
		t.Fatalf("second Install err = %v, want ErrConflict", err)
	}
	// No daemon is listening, so readiness cannot be proved and no activation
	// may be sealed.
	if _, err := f.deploy.Activate(f.within(3 * time.Second)); !errors.Is(err, daemonkit.ErrAbsent) {
		t.Fatalf("Activate err = %v, want ErrAbsent", err)
	}
	if fileExists(f.deploy.layout.activation) {
		t.Fatal("Activate sealed a record without proving readiness")
	}
}

// TestProveAnswersAbsenceEveryTime pins the contract above against the race
// that made it a coin flip: the readiness wait derives its retry cadence from
// the very deadline it is racing, so its last attach reported either the
// absence it found or the timeout it ran into. One run proves nothing about
// that — a run of them does.
func TestProveAnswersAbsenceEveryTime(t *testing.T) {
	f := newFixture(t)
	for attempt := range 16 {
		_, err := f.deploy.prove(f.within(50 * time.Millisecond))
		if !errors.Is(err, daemonkit.ErrAbsent) {
			t.Fatalf("attempt %d: prove err = %v, want ErrAbsent", attempt, err)
		}
	}
}

// TestProveAnswersAbsenceWhenTheAttachRunsOutOfClock pins the same contract
// against the input that beats a dial: a same-UID process holding the socket
// path and never speaking. Every attach connects and then blocks out its clock
// in the handshake, so both the wait and the attach that classifies it answer
// with a transport timeout — and a caller branching on absence may not be
// handed a raw one.
func TestProveAnswersAbsenceWhenTheAttachRunsOutOfClock(t *testing.T) {
	f := newFixture(t)
	f.squatSocket()
	if _, err := f.deploy.prove(f.within(2 * time.Second)); !errors.Is(err, daemonkit.ErrAbsent) {
		t.Fatalf("prove err = %v, want ErrAbsent", err)
	}
}

// TestProveWaitsOutADaemonThatPublishesLate covers the arm every other
// readiness test leaves untouched by running against no daemon at all: the
// wait exists for a daemon launchd has only just been asked to start, and
// surrendering the end of the budget to the classifying attach may not cost it
// the readiness that arrives late.
func TestProveWaitsOutADaemonThatPublishesLate(t *testing.T) {
	f := newFixture(t)
	const delay = 750 * time.Millisecond
	f.startDaemonChild(delay)
	start := time.Now()
	readiness, err := f.deploy.prove(f.within(30 * time.Second))
	if err != nil {
		t.Fatalf("prove: %v", err)
	}
	if waited := time.Since(start); waited < delay {
		t.Fatalf("prove returned after %v, before the daemon could publish at %v", waited, delay)
	}
	if readiness.Build() == "" || readiness.Generation() == 0 || readiness.Digest() == (SHA256{}) {
		t.Fatalf("prove = %+v, want the late daemon's build, generation and digest", readiness)
	}
}

// TestAttachContextSurvivesTheDeadlineButNotTheCancel pins both halves of the
// classifying attach's clock. The caller's deadline is exactly what the grace
// is carved out of — a wait that overruns it by a scheduler tick may not eat
// the answer — while a caller that gave up is not asking for one more attach.
func TestAttachContextSurvivesTheDeadlineButNotTheCancel(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		attach, release := attachContext(ctx, time.Minute)
		defer release()
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		if err := attach.Err(); err != nil {
			t.Fatalf("attach ctx err = %v after the caller's deadline, want a clock of its own", err)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		attach, release := attachContext(ctx, time.Minute)
		defer release()
		cancel()
		select {
		case <-attach.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("attach ctx outlived the caller's cancellation")
		}
		if !errors.Is(context.Cause(attach), context.Canceled) {
			t.Fatalf("attach ctx cause = %v, want context.Canceled", context.Cause(attach))
		}
	})
}

func TestInstallRejectsMismatchedCandidate(t *testing.T) {
	f := newFixture(t)
	good := f.candidate("Source", "1.0", "one")
	tests := []struct {
		name      string
		candidate Candidate
		want      error
	}{
		{"wrong version", Candidate{Source: good.Source, Version: "2.0", Digest: good.Digest}, ErrVersion},
		{"wrong digest", Candidate{Source: good.Source, Version: "1.0", Digest: SHA256{1}}, ErrConflict},
		{"no digest", Candidate{Source: good.Source, Version: "1.0"}, ErrConfig},
		{"relative source", Candidate{Source: "Example.app", Version: "1.0", Digest: good.Digest}, ErrConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := f.deploy.Install(f.ctx(), tt.candidate); !errors.Is(err, tt.want) {
				t.Fatalf("Install err = %v, want %v", err, tt.want)
			}
			if fileExists(f.app) {
				t.Fatal("a rejected candidate reached the canonical path")
			}
		})
	}
}

func TestSupersedeReplacesTheInstalledGeneration(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	next := f.candidate("Second", "2.0", "two")
	landed, err := f.deploy.Supersede(f.ctx(), next)
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if landed.Version != "2.0" || landed.BundleDigest != next.Digest.String() {
		t.Fatalf("Supersede = %+v, want version 2.0 at %q", landed, next.Digest)
	}
	if fileExists(f.deploy.layout.prior) || fileExists(f.deploy.layout.swap) {
		t.Fatal("Supersede left the prior tree or the swap record behind")
	}
	if fileExists(f.deploy.layout.activation) {
		t.Fatal("Supersede kept the departed generation's sealed activation")
	}
}

func TestSupersedeRefusesWhileTheDaemonIsLive(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	f.live = []LiveProcess{{PID: 4242, Executable: f.agent.Program}}
	_, err := f.deploy.Supersede(f.ctx(), f.candidate("Second", "2.0", "two"))
	if !errors.Is(err, ErrLive) {
		t.Fatalf("Supersede err = %v, want ErrLive", err)
	}
	if fileExists(f.deploy.layout.swap) || fileExists(f.deploy.layout.candidate) {
		t.Fatal("Supersede staged a candidate despite a live process")
	}
	installed, err := f.deploy.inspect(f.ctx(), f.app)
	if err != nil || installed.Version != "1.0" {
		t.Fatalf("installed = %+v, %v; want the untouched 1.0 generation", installed, err)
	}
}

func TestSupersedeRequiresAnInstalledGeneration(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Supersede(f.ctx(), f.candidate("Only", "1.0", "one")); !errors.Is(err, ErrConflict) {
		t.Fatalf("Supersede err = %v, want ErrConflict", err)
	}
}

func TestUninstallGatesOnTheInventoryAndSealsATombstone(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	f.live = []LiveProcess{{PID: 99, Executable: f.agent.Program}}
	if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Uninstall err = %v, want ErrLive", err)
	}
	if !fileExists(f.app) {
		t.Fatal("Uninstall removed the app despite a surviving process")
	}
	if fileExists(f.deploy.layout.removal) {
		t.Fatal("Uninstall sealed a tombstone despite a surviving process")
	}

	f.live = nil
	removal, err := f.deploy.Uninstall(f.ctx())
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if !removal.Runtime.Absent() || removal.Runtime.Digest() == (SHA256{}) {
		t.Fatalf("Removal runtime = %+v, want an absent proof with a digest", removal.Runtime)
	}
	if removal.Generation.Version != "1.0" {
		t.Fatalf("Removal generation = %+v, want version 1.0", removal.Generation)
	}
	if fileExists(f.app) || fileExists(f.deploy.layout.removed) {
		t.Fatal("Uninstall left the app or the removal slot behind")
	}
	replay, err := f.deploy.Uninstall(f.ctx())
	if err != nil {
		t.Fatalf("Uninstall replay: %v", err)
	}
	if replay != removal {
		t.Fatalf("Uninstall replay = %+v, want the tombstoned generation re-proved absent as %+v", replay, removal)
	}
}

// TestUninstallHandsBackThisMomentsAbsenceProof pins which half of a replay
// comes out of the tombstone and which half may not. The generation does: once
// the bytes are gone, nothing but the record can name what left. The absence
// proof does not — it is what the caller acts on, and a proof minted at an
// earlier moment is evidence about that moment alone.
func TestUninstallHandsBackThisMomentsAbsenceProof(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	generation, err := f.deploy.inspect(f.ctx(), f.app)
	if err != nil {
		t.Fatal(err)
	}
	sealed := runtimeProof(daemonkit.Stopped{
		Before: daemonkit.Health{PID: 4242, Build: "departed", Generation: 7},
		Reap:   daemonkit.ReapCrossBoot,
	})
	if err := writeRecord(f.deploy.layout.removal, removalRecord{
		Identity: removalIdentity, Schema: recordSchema,
		Generation: generation, Runtime: sealed.stored(),
	}); err != nil {
		t.Fatal(err)
	}
	removal, err := f.deploy.Uninstall(f.ctx())
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if removal.Generation != generation {
		t.Fatalf("Removal generation = %+v, want the tombstoned %+v", removal.Generation, generation)
	}
	if want := runtimeProof(daemonkit.Stopped{Reap: daemonkit.ReapAbsent}); removal.Runtime != want {
		t.Fatalf("Removal runtime = %+v, want this moment's proof %+v", removal.Runtime, want)
	}
}

// TestUninstallRegatesAResumedRemoval covers the app being restored under a
// sealed tombstone: the proof was minted at an earlier moment and carries no
// authority over this one, so the inventory gate runs again.
func TestUninstallRegatesAResumedRemoval(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	generation, err := f.deploy.inspect(f.ctx(), f.app)
	if err != nil {
		t.Fatal(err)
	}
	proof := runtimeProof(daemonkit.Stopped{Reap: daemonkit.ReapAbsent})
	if err := writeRecord(f.deploy.layout.removal, removalRecord{
		Identity: removalIdentity, Schema: recordSchema,
		Generation: generation, Runtime: proof.stored(),
	}); err != nil {
		t.Fatal(err)
	}
	f.live = []LiveProcess{{PID: 7, Executable: f.agent.Program}}
	if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Uninstall err = %v, want ErrLive", err)
	}
	if !fileExists(f.app) {
		t.Fatal("a resumed uninstall removed the app despite a surviving process")
	}
}

// TestUninstallReprovesBothGateHalvesOnAReplay pins the half the hoisted
// inventory left behind: a replay reads its reap out of a tombstone sealed at
// an earlier moment while still performing two irreversible steps. The
// services a re-registered agent holds are the observable half — nothing takes
// them away until this moment's daemon is proved gone.
func TestUninstallReprovesBothGateHalvesOnAReplay(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if err := writeRecord(f.deploy.layout.services, serviceRecord{
		Identity: serviceIdentity, Schema: recordSchema, Labels: []string{f.agent.Label},
	}); err != nil {
		t.Fatal(err)
	}
	scans := 0
	f.deploy.inventory = func(...string) (Survivors, error) {
		scans++
		return Survivors{}, nil
	}
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall replay: %v", err)
	}
	if scans < 2 {
		t.Errorf("inventory ran %d times, want the quiesce gate's scan and the pre-removal one", scans)
	}
	if fileExists(f.deploy.layout.services) {
		t.Error("a replayed Uninstall never converged the services away")
	}
}

// TestUninstallRegatesAReplayAfterTheAppIsGone pins the gate on the arm that
// touches nothing: a replay still hands the caller a sealed absence proof, and
// a proof minted at an earlier moment carries no authority over this one.
func TestUninstallRegatesAReplayAfterTheAppIsGone(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if fileExists(f.app) {
		t.Fatal("Uninstall left the app behind")
	}
	f.live = []LiveProcess{{PID: 13, Executable: f.agent.Program}}
	if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Uninstall replay err = %v, want ErrLive", err)
	}
}

// TestInstallRetiresTheTombstone pins the record land must clear: a completed
// uninstall's proof names the departed generation, so a surviving tombstone
// wedges the next uninstall against bytes it was never minted for.
func TestInstallRetiresTheTombstone(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	departed, err := f.deploy.Uninstall(f.ctx())
	if err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Second", "2.0", "two")); err != nil {
		t.Fatalf("reinstall: %v", err)
	}
	if fileExists(f.deploy.layout.removal) {
		t.Fatal("Install kept the departed generation's tombstone")
	}
	removal, err := f.deploy.Uninstall(f.ctx())
	if err != nil {
		t.Fatalf("Uninstall after reinstall: %v", err)
	}
	if removal.Generation.Version != "2.0" {
		t.Fatalf("Removal generation = %+v, want the reinstalled 2.0", removal.Generation)
	}
	if removal.Generation.CDHash == departed.Generation.CDHash {
		t.Fatal("Uninstall replayed the departed generation's tombstone")
	}
}

// TestSupersedeRegatesBeforeTheRename pins the second gate: an unbounded
// bundle-tree copy separates the quiesce from the rename that destroys the
// incumbent's bytes, and that is long enough for a process to come back.
func TestSupersedeRegatesBeforeTheRename(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	scans := 0
	f.deploy.inventory = func(...string) (Survivors, error) {
		scans++
		if scans == 1 {
			return Survivors{}, nil
		}
		return Survivors{Live: []LiveProcess{{PID: 5150, Start: 77, Boot: 9, Executable: f.agent.Program}}}, nil
	}
	if _, err := f.deploy.Supersede(f.ctx(), f.candidate("Second", "2.0", "two")); !errors.Is(err, ErrLive) {
		t.Fatalf("Supersede err = %v, want ErrLive", err)
	}
	if scans < 2 {
		t.Fatalf("inventory ran %d times, want a second scan before the rename", scans)
	}
	if fileExists(f.deploy.layout.swap) {
		t.Fatal("a refused Supersede left a swap record for the next verb to resume past the gate")
	}
	installed, err := f.deploy.inspect(f.ctx(), f.app)
	if err != nil || installed.Version != "1.0" {
		t.Fatalf("installed = %+v, %v; want the untouched 1.0 generation", installed, err)
	}
}

// TestSupersedeConsumesAByteIdenticalCandidate pins the swap's completion
// probe against the one input that defeats a bytes-only answer: a candidate
// identical to the prior it replaces is already at the canonical path before
// either rename has happened.
func TestSupersedeConsumesAByteIdenticalCandidate(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	same := f.candidate("Same", "1.0", "one")
	landed, err := f.deploy.Supersede(f.ctx(), same)
	if err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if landed.BundleDigest != same.Digest.String() {
		t.Fatalf("Supersede = %+v, want digest %q", landed, same.Digest)
	}
	if fileExists(f.deploy.layout.candidate) {
		t.Fatal("Supersede stranded the staged candidate in its slot")
	}
	if fileExists(f.deploy.layout.prior) || fileExists(f.deploy.layout.swap) {
		t.Fatal("Supersede left the prior tree or the swap record behind")
	}
	if _, err := f.deploy.Supersede(f.ctx(), f.candidate("Next", "2.0", "two")); err != nil {
		t.Fatalf("Supersede after an identical candidate: %v", err)
	}
}

func TestResetSettlesAndPurges(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	generation, err := f.deploy.inspect(f.ctx(), f.app)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRecord(f.deploy.layout.activation, activationRecord{
		Identity: activationIdentity, Schema: recordSchema, Generation: generation,
		Readiness: storedProof{Build: "stale", Generation: 7, Digest: hex.EncodeToString(make([]byte, 32))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.deploy.layout.prior, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for name, path := range map[string]string{
		"activation": f.deploy.layout.activation,
		"removal":    f.deploy.layout.removal,
		"swap":       f.deploy.layout.swap,
		"services":   f.deploy.layout.services,
		"prior":      f.deploy.layout.prior,
		"candidate":  f.deploy.layout.candidate,
		"removed":    f.deploy.layout.removed,
	} {
		if fileExists(path) {
			t.Errorf("Reset left %s behind at %q", name, path)
		}
	}
	if !fileExists(f.app) {
		t.Fatal("Reset destroyed the installed bytes")
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset replay: %v", err)
	}
}

// TestResetDiscardsALeakedStagingTree pins the whole of what Reset claims to
// discard. stage names its copy through MkdirTemp and publishes it only by
// rename, so a crash in between strands a full bundle copy under the metadata
// directory that no record names and no later verb ever revisits: Reset is the
// only thing that can reclaim it.
func TestResetDiscardsALeakedStagingTree(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	leaked, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leaked, "Info.plist"), []byte("stranded"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if fileExists(leaked) {
		t.Fatalf("Reset left the leaked staging tree at %q", leaked)
	}
	if !fileExists(f.app) {
		t.Fatal("Reset destroyed the installed bytes")
	}
}

func TestResetRefusesWhileTheDaemonIsLive(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	f.live = []LiveProcess{{PID: 11, Executable: f.agent.Program}}
	if err := f.deploy.Reset(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Reset err = %v, want ErrLive", err)
	}
}

// TestActivateGatesTheServicesItRetires pins the gate Activate's own converge
// went around. A label the durable services record names and the config no
// longer does is booted out of launchd, and launchd booting a job out is how a
// live daemon dies — so it may only ever be taken away from a runtime already
// proved absent, exactly as every other verb takes it away.
func TestActivateGatesTheServicesItRetires(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	const retiredLabel = "com.example.daemonkit.test.retired"
	retired := f.markedAgent(retiredLabel)
	if err := writeRecord(f.deploy.layout.services, serviceRecord{
		Identity: serviceIdentity, Schema: recordSchema,
		Labels: []string{f.agent.Label, retiredLabel},
	}); err != nil {
		t.Fatal(err)
	}
	f.live = []LiveProcess{{PID: 4242, Executable: f.agent.Program}}
	if _, err := f.deploy.Activate(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Activate err = %v, want ErrLive", err)
	}
	if !fileExists(retired) {
		t.Fatal("Activate booted a label out of launchd with no absence proof behind it")
	}
	for _, args := range f.launchctls {
		if slices.Contains(args, "bootout") {
			t.Fatalf("launchctl %q booted a job out before the daemon was proved gone", args)
		}
	}
}

// TestConvergeTouchesOnlyItsOwnLabels pins the ownership model: deploy applies
// and retires exactly the labels its own durable record names, so another
// product's marked agent sharing the LaunchAgents directory is never
// discovered, never named to launchctl, and never swept.
func TestConvergeTouchesOnlyItsOwnLabels(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	const foreignLabel = "com.other.product.agent"
	foreign := f.markedAgent(foreignLabel)
	if err := f.deploy.converge(f.ctx(), f.deploy.config.Agents); err != nil {
		t.Fatalf("converge: %v", err)
	}
	own := filepath.Join(f.agentsDir, f.agent.Label+".plist")
	if !fileExists(own) {
		t.Fatalf("converge did not install %q", own)
	}
	var record serviceRecord
	if err := readRecord(f.deploy.layout.services, &record); err != nil {
		t.Fatalf("read services record: %v", err)
	}
	if !slices.Equal(record.Labels, []string{f.agent.Label}) {
		t.Fatalf("services record = %q, want [%q]", record.Labels, f.agent.Label)
	}
	if err := f.deploy.converge(f.ctx(), nil); err != nil {
		t.Fatalf("converge away: %v", err)
	}
	if fileExists(own) {
		t.Fatalf("converge away kept %q", own)
	}
	if fileExists(f.deploy.layout.services) {
		t.Fatal("converge away kept the services record")
	}
	if !fileExists(foreign) {
		t.Fatal("converge swept another product's marked agent")
	}
	for _, args := range f.launchctls {
		if slices.ContainsFunc(args, func(arg string) bool { return strings.Contains(arg, foreignLabel) }) {
			t.Fatalf("launchctl %q named another product's label", args)
		}
	}
}

// markedAgent writes a daemonkit-marked plist at label into the shared
// LaunchAgents directory and returns its path — another product's when the
// label is one this deployment never recorded, this deployment's own when it
// is one the services record names.
func (f *fixture) markedAgent(label string) string {
	f.t.Helper()
	plist, err := launchd.Agent{
		Label:         label,
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(f.root, label+".log"),
		RestartPolicy: launchd.RestartOnFailure,
	}.Plist()
	if err != nil {
		f.t.Fatal(err)
	}
	path := filepath.Join(f.agentsDir, label+".plist")
	if err := os.WriteFile(path, plist, 0o600); err != nil {
		f.t.Fatal(err)
	}
	return path
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	valid := Config{
		App:         "/opt/Example.app",
		Requirement: daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning},
		Daemon:      daemonkit.Daemon{Label: "example"},
		Agents:      []launchd.Agent{{Label: "l", Program: "/opt/Example.app/Contents/MacOS/x"}},
	}
	mutate := func(f func(*Config)) Config {
		config := valid
		config.Agents = append([]launchd.Agent(nil), valid.Agents...)
		f(&config)
		return config
	}
	tests := []struct {
		name   string
		config Config
	}{
		{"relative app", mutate(func(c *Config) { c.App = "Example.app" })},
		{"unclean app", mutate(func(c *Config) { c.App = "/opt/./Example.app" })},
		{"not an app", mutate(func(c *Config) { c.App = "/opt/Example" })},
		{"no team", mutate(func(c *Config) { c.Requirement.TeamID = "" })},
		{"no signing identifier", mutate(func(c *Config) { c.Requirement.SigningIdentifier = "" })},
		{"no label", mutate(func(c *Config) { c.Daemon.Label = "" })},
		{"no agents", mutate(func(c *Config) { c.Agents = nil })},
		{"program outside app", mutate(func(c *Config) { c.Agents[0].Program = "/usr/bin/true" })},
		{"relative program", mutate(func(c *Config) { c.Agents[0].Program = "Contents/MacOS/x" })},
		{"unclean program", mutate(func(c *Config) { c.Agents[0].Program = "/opt/Example.app/./Contents/MacOS/x" })},
		{"relative executable", mutate(func(c *Config) { c.Executables = []string{"hookd"} })},
		{"unclean executable", mutate(func(c *Config) { c.Executables = []string{"/usr/bin/../bin/true"} })},
		{"absent executable", mutate(func(c *Config) { c.Executables = []string{"/usr/bin/daemonkit-absent"} })},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Open(tt.config); !errors.Is(err, ErrConfig) {
				t.Fatalf("Open err = %v, want ErrConfig", err)
			}
		})
	}
	if _, err := Open(valid); err != nil {
		t.Fatalf("Open(valid): %v", err)
	}
}

// TestOpenResolvesDeclaredExecutables pins the fail-open the gate cannot have:
// the inventory compares a query against the literal form and the symlink-free
// one, and a process reports neither when the declared path reaches its
// executable through a symlinked directory — so an unresolved declaration would
// survive Open and then match nothing at all.
func TestOpenResolvesDeclaredExecutables(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	actual := filepath.Join(root, "hookd")
	if err := os.WriteFile(actual, []byte("host"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "hookd-link")
	if err := os.Symlink(actual, link); err != nil {
		t.Fatal(err)
	}
	app := filepath.Join(root, "Example.app")
	deployment, err := Open(Config{
		App:         app,
		Requirement: daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning},
		Daemon:      daemonkit.Daemon{Label: "example"},
		Agents:      []launchd.Agent{{Label: "l", Program: filepath.Join(app, "Contents", "MacOS", "x")}},
		Executables: []string{link},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := deployment.config.Executables; len(got) != 1 || got[0] != actual {
		t.Fatalf("Open resolved executables to %q, want [%q]", got, actual)
	}
}
