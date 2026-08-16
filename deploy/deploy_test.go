package deploy

import (
	"context"
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
	huskChildEnv     = "DAEMONKIT_DEPLOY_HUSK_CHILD"
	huskChildRecord  = "DAEMONKIT_DEPLOY_HUSK_CHILD_RECORD"
)

// TestMain branches on the child markers before m.Run so a spawned copy of
// this binary serves one daemon, or stands in for one whose bytes were
// unlinked, and exits instead of re-entering the suite — the re-exec fork bomb
// scripts/test.sh backstops.
func TestMain(m *testing.M) {
	if os.Getenv(daemonChildEnv) == "1" {
		serveDaemonChild()
	}
	if os.Getenv(huskChildEnv) == "1" {
		serveHuskChild()
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

// serveHuskChild records this process in the owner store its parent named,
// reports the pin it was given on stdout, and then blocks forever. Its parent
// unlinks the executable underneath it, which is the only way to mint the one
// process state no record can forge and no test can declare: live, same-user,
// and unnameable by the kernel. A parent that named no store records nothing,
// which is the stranger's husk. It never returns.
func serveHuskChild() {
	if record := os.Getenv(huskChildRecord); record != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		store, err := proc.OpenStore(ctx, record)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "deploy husk child: %v\n", err)
			os.Exit(72)
		}
		if _, err := store.RecordOwner("husk-child"); err != nil {
			fmt.Fprintf(os.Stderr, "deploy husk child: %v\n", err)
			os.Exit(73)
		}
		_ = store.Close()
	}
	fmt.Println(os.Getpid())
	select {}
}

type stubProduct struct{}

func (stubProduct) Handle(context.Context, daemonkit.Request) (daemonkit.Reply, error) {
	return daemonkit.Reply{}, errors.New("unused")
}

func (stubProduct) Drain(daemonkit.Budget) error { return nil }

func (stubProduct) Close(daemonkit.Budget) error { return nil }

const (
	testTeamID  = "ABCDE12345"
	testSigning = "com.example.daemonkit.test"

	// adhocRequirement is the designated requirement a test bundle can
	// actually satisfy, and the one clause of the configured requirement that
	// survives here. Config.Requirement renders to a Developer ID anchored DR
	// — apple generic anchor, the team's certificate leaf[subject.OU], the
	// Developer ID leaf and intermediate OIDs — which no ad-hoc signature
	// carries and no identity on a developer machine or a CI runner can mint
	// (`security find-identity -v -p codesigning` reports none). Every flow
	// test therefore runs the real codesign verifier against a really
	// ad-hoc-signed bundle under the identifier clause alone;
	// TestVerifyRefusesABundleThatMissesTheDesignatedRequirement is what
	// covers the certificate clauses relaxed here.
	adhocRequirement = `identifier "` + testSigning + `"`

	// bundleProgram is the Mach-O every test bundle carries. It sleeps, so a
	// test can start a real process on a deployment's executable and the real
	// inventory can read it back out of the kernel's own process table, and it
	// is a system binary, so no test needs a compiler to get one.
	bundleProgram = "/bin/sleep"

	// bundleBodyRel is the sealed resource that distinguishes two candidates.
	// It rides in Resources rather than in the executable so the executable
	// stays a runnable Mach-O; codesign seals it, so two bodies mean two
	// CDHashes as well as two tree digests.
	bundleBodyRel = "Contents/Resources/body"

	// entitlementsPlist is the entitlement set every test bundle is signed
	// with. It has to be present: codesign reports no plist at all for a
	// bundle signed without one, and the production verifier refuses that
	// rather than digesting an absence.
	entitlementsPlist = `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict><key>com.apple.security.app-sandbox</key><false/></dict></plist>`
)

// signBundle ad-hoc signs an .app in place, so the production verifier has a
// real signature to check: a real resource seal for --strict --deep to walk, a
// real CDHash to parse, and a real entitlement plist to digest.
func signBundle(t *testing.T, path string) {
	t.Helper()
	entitlements := filepath.Join(t.TempDir(), "entitlements.plist")
	if err := os.WriteFile(entitlements, []byte(entitlementsPlist), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(
		"/usr/bin/codesign", "--sign", "-", "--identifier", testSigning,
		"--entitlements", entitlements, "--force", path,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("codesign %q: %v\n%s", path, err, out)
	}
}

// writeBundle lays down a minimal .app: an Info.plist declaring version and
// naming the executable codesign must seal as the bundle's main image, that
// executable, and the sealed resource carrying body.
func writeBundle(t *testing.T, path, version, body string) {
	t.Helper()
	macOS := filepath.Join(path, "Contents", "MacOS")
	if err := os.MkdirAll(macOS, 0o755); err != nil {
		t.Fatal(err)
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleShortVersionString</key><string>%s</string>
<key>CFBundleExecutable</key><string>example</string>
<key>CFBundleIdentifier</key><string>%s</string>
</dict></plist>`, version, testSigning)
	if err := os.WriteFile(filepath.Join(path, "Contents", "Info.plist"), []byte(plist), 0o644); err != nil {
		t.Fatal(err)
	}
	program, err := os.ReadFile(bundleProgram)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(macOS, "example"), program, 0o755); err != nil {
		t.Fatal(err)
	}
	sealed := filepath.Join(path, bundleBodyRel)
	if err := os.MkdirAll(filepath.Dir(sealed), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sealed, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixture is one deployment over a temporary install root. codesign, the
// kernel's process table, and the LaunchAgents directory are all real; the one
// stand-in is launchctl, which a unit suite may not bootstrap jobs into (the
// real thing runs out of the suite, via scripts/e2e-launchd.sh).
type fixture struct {
	t          *testing.T
	root       string
	app        string
	agentsDir  string
	deploy     *Deployment
	agent      launchd.Agent
	launchctls [][]string
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
		Daemon: daemonkit.Daemon{
			Label: daemonkit.Label("daemonkit-deploy-test-" + filepath.Base(root)),
			Trust: daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		},
		Agents: []launchd.Agent{agent},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	deployment.requirement = adhocRequirement
	deployment.run = func(_ context.Context, _ string, args ...string) (string, int, error) {
		f.launchctls = append(f.launchctls, args)
		return "", 0, nil
	}
	f.deploy = deployment
	return f
}

// live starts a real process from program and blocks until the kernel's own
// process table reports it there, so the gate that runs next faces a survivor
// no test declared.
func (f *fixture) live(program string) *exec.Cmd {
	f.t.Helper()
	child := exec.Command(program, "600")
	if err := child.Start(); err != nil {
		f.t.Fatalf("start %q: %v", program, err)
	}
	f.t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	f.awaitLive(program, true)
	return child
}

// hostExecutable plants the host binary a launcher runs from outside the
// bundle and declares it the way a consumer does, through Config.Executables.
// It outlives every generation slot, so it is the only executable of this
// deployment a process can still be running once the app itself is gone.
// It is ad-hoc signed on the way down because a bare copy of a system binary
// carries a platform-binary signature the kernel honours only on the system
// volume and SIGKILLs anywhere else; bundled copies get the same treatment
// from signBundle.
func (f *fixture) hostExecutable() string {
	f.t.Helper()
	path := filepath.Join(f.root, "hostd")
	program, err := os.ReadFile(bundleProgram)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, program, 0o755); err != nil {
		f.t.Fatal(err)
	}
	out, err := exec.Command("/usr/bin/codesign", "--sign", "-", "--force", path).CombinedOutput()
	if err != nil {
		f.t.Fatalf("codesign %q: %v\n%s", path, err, out)
	}
	f.deploy.config.Executables = append(f.deploy.config.Executables, path)
	return path
}

// liveSlot starts a real process from whichever generation slot currently
// carries this deployment's executable. A crash point leaves the bundle at the
// canonical path or moved aside into the prior slot, and the gate enumerates
// both, so which one holds the bytes is the crash point's business, not the
// assertion's.
func (f *fixture) liveSlot() *exec.Cmd {
	f.t.Helper()
	for _, slot := range []string{f.deploy.layout.canonical, f.deploy.layout.prior, f.deploy.layout.candidate} {
		program := filepath.Join(slot, "Contents", "MacOS", "example")
		if fileExists(program) {
			return f.live(program)
		}
	}
	f.t.Fatal("no generation slot carries an executable to run")
	return nil
}

// settle kills a process this fixture started and blocks until the real
// inventory stops reporting it, so the verb that runs next faces a table the
// kernel itself says is empty.
func (f *fixture) settle(child *exec.Cmd, program string) {
	f.t.Helper()
	_ = child.Process.Kill()
	_ = child.Wait()
	f.awaitLive(program, false)
}

func (f *fixture) awaitLive(program string, want bool) {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		found, err := Inventory(program)
		if err != nil {
			f.t.Fatalf("Inventory(%q): %v", program, err)
		}
		if (len(found.Live) > 0) == want {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("Inventory(%q) never reported live=%v", program, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// husk starts a copy of this test binary that records itself as this
// deployment's daemon owner, then unlinks the copy — leaving a live same-user
// process the kernel can no longer name, which is what a daemon whose bytes an
// upgrade deleted actually is. record says whether the child writes the owner
// record; a child that writes none is the stranger's husk every machine
// carries and no gate may count. It returns the husk's pid.
func (f *fixture) husk(record bool) int {
	f.t.Helper()
	copied := filepath.Join(f.t.TempDir(), "husk")
	executable, err := os.Executable()
	if err != nil {
		f.t.Fatalf("os.Executable() = %v", err)
	}
	bytes, err := os.ReadFile(executable)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(copied, bytes, 0o755); err != nil {
		f.t.Fatal(err)
	}
	child := exec.Command(copied)
	child.Env = append(os.Environ(), huskChildEnv+"=1", huskChildRecord+"=")
	if record {
		path := f.deploy.config.Daemon.RecordPath()
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			f.t.Fatal(err)
		}
		child.Env[len(child.Env)-1] = huskChildRecord + "=" + path
	}
	child.Stderr = os.Stderr
	out, err := child.StdoutPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		f.t.Fatalf("start husk child: %v", err)
	}
	f.t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	var pid int
	if _, err := fmt.Fscanln(out, &pid); err != nil {
		f.t.Fatalf("husk child never reported its pin: %v", err)
	}
	if err := os.Remove(copied); err != nil {
		f.t.Fatal(err)
	}
	f.awaitHusk(pid)
	return pid
}

func (f *fixture) awaitHusk(pid int) {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		found, err := Inventory(filepath.Join(f.root, "nothing-runs-from-here"))
		if err != nil {
			f.t.Fatalf("Inventory: %v", err)
		}
		if slices.ContainsFunc(found.Unnameable, func(p LiveProcess) bool { return p.PID == pid }) {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("the husk at pid %d never became unnameable", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runnableAt plants a real program at path, ad-hoc signed for the same reason
// hostExecutable is: a bare copy of a system binary carries a platform-binary
// signature the kernel honours only on the system volume. It is what makes a
// generation slot bytes a live process can actually be running, so a gate that
// never scanned the slot is provable by the verb it fails to refuse.
func (f *fixture) runnableAt(path string) string {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	program, err := os.ReadFile(bundleProgram)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, program, 0o755); err != nil {
		f.t.Fatal(err)
	}
	out, err := exec.Command("/usr/bin/codesign", "--sign", "-", "--force", path).CombinedOutput()
	if err != nil {
		f.t.Fatalf("codesign %q: %v\n%s", path, err, out)
	}
	return path
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
	store, err := proc.OpenStore(f.within(30*time.Second), path)
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
// sealed resource carries body, then ad-hoc signs it, so two candidates differ
// in every digest the real verifier and the tree walk read.
func (f *fixture) bundle(name, version, body string) string {
	f.t.Helper()
	path := filepath.Join(f.root, name+".app")
	writeBundle(f.t, path, version, body)
	signBundle(f.t, path)
	return path
}

func (f *fixture) candidate(name, version, body string) Candidate {
	f.t.Helper()
	source := f.bundle(name, version, body)
	digest, err := BundleDigest(source)
	if err != nil {
		f.t.Fatal(err)
	}
	return Candidate{Source: source, Version: version, Digest: digest}
}

// cdHashOf is what codesign itself says the bundle's CDHash is, so the
// verifier's answer is checked against the tool rather than against a second
// copy of the verifier's own parsing.
func cdHashOf(t *testing.T, app string) string {
	t.Helper()
	out, err := exec.Command("/usr/bin/codesign", "-d", "--verbose=4", app).CombinedOutput()
	if err != nil {
		t.Fatalf("codesign -d %q: %v\n%s", app, err, out)
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "CDHash="); ok {
			return strings.ToLower(value)
		}
	}
	t.Fatalf("codesign -d %q printed no CDHash:\n%s", app, out)
	return ""
}

// TestVerifyReadsARealSignature runs the production verifier against a real
// signature. The CDHash it reports is the one codesign prints for the same
// bundle, and the entitlements digest is the one the plist the bundle was
// signed with canonicalizes to — neither is a hash the test handed it.
func TestVerifyReadsARealSignature(t *testing.T) {
	f := newFixture(t)
	app := f.bundle("Signed", "1.0", "one")
	attestation, err := codesignVerifier{}.Verify(f.ctx(), app, adhocRequirement)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if want := cdHashOf(t, app); attestation.CDHash != want {
		t.Fatalf("Verify CDHash = %q, want %q", attestation.CDHash, want)
	}
	want, err := DigestEntitlements([]byte(entitlementsPlist))
	if err != nil {
		t.Fatal(err)
	}
	if attestation.EntitlementsDigest != want {
		t.Fatalf("Verify entitlements digest = %v, want %v", attestation.EntitlementsDigest, want)
	}
	other := f.bundle("Other", "1.0", "two")
	otherAttestation, err := codesignVerifier{}.Verify(f.ctx(), other, adhocRequirement)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if otherAttestation.CDHash == attestation.CDHash {
		t.Fatalf("two byte sets share the CDHash %q", attestation.CDHash)
	}
}

// TestVerifyRefusesABundleThatMissesTheDesignatedRequirement covers the clauses
// adhocRequirement relaxes for every other test here — the apple generic
// anchor, the team's certificate leaf[subject.OU], and the Developer ID OIDs.
// A bundle that cannot satisfy them is refused as untrusted, so the DR
// Config.Requirement renders is a gate rather than a string nothing checks.
func TestVerifyRefusesABundleThatMissesTheDesignatedRequirement(t *testing.T) {
	f := newFixture(t)
	app := f.bundle("Signed", "1.0", "one")
	requirement, err := designatedRequirement(f.deploy.config.Requirement)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (codesignVerifier{}).Verify(f.ctx(), app, requirement); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Verify err = %v, want ErrUntrusted", err)
	}
}

// TestVerifyRefusesATamperedBundle is what --strict --deep buys: a bundle
// nobody signed carries no seal, and one whose tree moved after the seal no
// longer matches it — the executable and the sealed resource alike.
func TestVerifyRefusesATamperedBundle(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(*testing.T, string)
	}{
		{"never signed", func(*testing.T, string) {}},
		{"a resource added after the seal", func(t *testing.T, app string) {
			write(t, app, "Contents/Resources/extra", "planted", 0o644)
		}},
		{"the sealed body rewritten", func(t *testing.T, app string) {
			write(t, app, bundleBodyRel, "rewritten", 0o644)
		}},
		{"the main executable rewritten", func(t *testing.T, app string) {
			write(t, app, "Contents/MacOS/example", "rewritten", 0o755)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			app := filepath.Join(f.root, "Tampered.app")
			writeBundle(t, app, "1.0", "one")
			if tt.name != "never signed" {
				signBundle(t, app)
			}
			tt.tamper(t, app)
			if _, err := (codesignVerifier{}).Verify(f.ctx(), app, adhocRequirement); !errors.Is(err, ErrUntrusted) {
				t.Fatalf("Verify err = %v, want ErrUntrusted", err)
			}
		})
	}
}

// TestInstallRefusesAnUnsignedCandidate is the same refusal reached through the
// verb, so the wiring between a candidate and the codesign that gates it is
// covered end to end and no unsigned tree can reach the canonical path.
func TestInstallRefusesAnUnsignedCandidate(t *testing.T) {
	f := newFixture(t)
	source := filepath.Join(f.root, "Unsigned.app")
	writeBundle(t, source, "1.0", "one")
	digest, err := BundleDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.deploy.Install(f.ctx(), Candidate{Source: source, Version: "1.0", Digest: digest})
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Install err = %v, want ErrUntrusted", err)
	}
	if fileExists(f.app) {
		t.Fatal("an unsigned candidate reached the canonical path")
	}
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
	f.live(f.agent.Program)
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
	child := f.live(f.agent.Program)
	if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Uninstall err = %v, want ErrLive", err)
	}
	if !fileExists(f.app) {
		t.Fatal("Uninstall removed the app despite a surviving process")
	}
	if fileExists(f.deploy.layout.removal) {
		t.Fatal("Uninstall sealed a tombstone despite a surviving process")
	}

	f.settle(child, f.agent.Program)
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
	f.live(f.agent.Program)
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
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall replay: %v", err)
	}
	if fileExists(f.deploy.layout.services) {
		t.Error("a replayed Uninstall never converged the services away")
	}
}

// TestUninstallRegatesAReplayAfterTheAppIsGone pins the gate on the arm that
// touches nothing: a replay still hands the caller a sealed absence proof, and
// a proof minted at an earlier moment carries no authority over this one. Once
// the bundle is gone the survivor has to be on the host binary outside it —
// nothing can be running a path that no longer exists — which is exactly the
// executable Config.Executables is for.
func TestUninstallRegatesAReplayAfterTheAppIsGone(t *testing.T) {
	f := newFixture(t)
	host := f.hostExecutable()
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	if fileExists(f.app) {
		t.Fatal("Uninstall left the app behind")
	}
	f.live(host)
	if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Uninstall replay err = %v, want ErrLive", err)
	}
}

// TestUninstallDiscardsEveryGenerationItScanned pins what "removes the
// installed application" has to mean. A leaked staging tree and a superseded
// prior are whole copies of the same signed application, so an uninstall that
// took away only the canonical path left the app it removed sitting on disk.
// Their targets come out of the enumeration the gate just scanned, exactly as
// Reset's do, which is why a live process on either one refuses the whole verb.
func TestUninstallDiscardsEveryGenerationItScanned(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	leaked, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix)
	if err != nil {
		t.Fatal(err)
	}
	slots := map[string]string{"prior": f.deploy.layout.prior, "stage": leaked}
	for name, slot := range slots {
		writeMachO(t, filepath.Join(slot, "Contents", "MacOS", name), 0o755)
	}
	if _, err := f.deploy.Uninstall(f.ctx()); err != nil {
		t.Fatalf("Uninstall: %v", err)
	}
	for name, slot := range slots {
		if fileExists(slot) {
			t.Errorf("Uninstall left a whole signed generation at the %s slot %q", name, slot)
		}
	}
	if fileExists(f.app) || fileExists(f.deploy.layout.removed) {
		t.Fatal("Uninstall left the app or the removal slot behind")
	}
}

// TestUninstallScansEveryGenerationItDiscards is the other half, proved the
// only way a gate can be: a live process on a slot's own executable refuses
// the verb, so a slot the scan never asked about is a slot Uninstall would
// have destroyed under it.
func TestUninstallScansEveryGenerationItDiscards(t *testing.T) {
	slots := []string{"prior", "stage"}
	for _, name := range slots {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
				t.Fatalf("Install: %v", err)
			}
			slot := f.deploy.layout.prior
			if name == "stage" {
				leaked, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix)
				if err != nil {
					t.Fatal(err)
				}
				slot = leaked
			}
			f.live(f.runnableAt(filepath.Join(slot, "Contents", "MacOS", name)))
			if _, err := f.deploy.Uninstall(f.ctx()); !errors.Is(err, ErrLive) {
				t.Fatalf("Uninstall err = %v, want ErrLive: the %s slot at %q is running", err, name, slot)
			}
			if !fileExists(slot) {
				t.Fatalf("Uninstall destroyed the %s slot at %q while a process was running its bytes", name, slot)
			}
		})
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

// TestResetDestroysNothingTheGateDidNotScan holds Reset to the enumeration the
// inventory gate scans. Every slot it destroys can be the bytes a live process
// is running, so each one has to carry its executables into the query set the
// gate proved empty — a slot the gate never asked about is destruction nothing
// authorized.
func TestResetDestroysNothingTheGateDidNotScan(t *testing.T) {
	for _, name := range []string{"prior", "candidate", "removed", "stage"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
				t.Fatalf("Install: %v", err)
			}
			slot := f.resetSlot(name)
			f.live(f.runnableAt(filepath.Join(slot, "Contents", "MacOS", name)))
			if err := f.deploy.Reset(f.ctx()); !errors.Is(err, ErrLive) {
				t.Fatalf("Reset err = %v, want ErrLive: the %s slot at %q is running", err, name, slot)
			}
			if !fileExists(slot) {
				t.Fatalf("Reset destroyed the %s slot at %q, which the gate never scanned", name, slot)
			}
		})
	}
}

// resetSlot is one of the generation slots Reset discards, materialized so a
// process can be running out of it.
func (f *fixture) resetSlot(name string) string {
	f.t.Helper()
	switch name {
	case "prior":
		return f.deploy.layout.prior
	case "candidate":
		return f.deploy.layout.candidate
	case "removed":
		return f.deploy.layout.removed
	case "stage":
		leaked, err := os.MkdirTemp(f.deploy.layout.metadata, stagePrefix+"*"+stageSuffix)
		if err != nil {
			f.t.Fatal(err)
		}
		return leaked
	}
	f.t.Fatalf("no reset slot named %q", name)
	return ""
}

// TestResetDiscardsEveryGeneration is the same enumeration from the other
// side: with nothing running, every slot Reset scanned is a slot it destroys.
func TestResetDiscardsEveryGeneration(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	slots := map[string]string{}
	for _, name := range []string{"prior", "candidate", "removed", "stage"} {
		slot := f.resetSlot(name)
		writeMachO(t, filepath.Join(slot, "Contents", "MacOS", name), 0o755)
		slots[name] = slot
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	for name, slot := range slots {
		if fileExists(slot) {
			t.Fatalf("Reset left the %s slot at %q", name, slot)
		}
	}
}

// TestResetClearsASlotThatCannotHoldABundle is the wedge's regression, and the
// reason it mattered: a plain file at a generation slot — a botched install, a
// same-uid plant — failed the inventory the gate every verb runs, and Reset is
// the way out of a state no other verb accepts, so there was no way out at all.
func TestResetClearsASlotThatCannotHoldABundle(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := os.WriteFile(f.deploy.layout.prior, []byte("not a bundle"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.requireEmpty(); err != nil {
		t.Fatalf("requireEmpty = %v, want a gate a non-directory slot cannot wedge", err)
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if fileExists(f.deploy.layout.prior) {
		t.Fatal("Reset left the slot that cannot hold a bundle behind")
	}
	if !fileExists(f.app) {
		t.Fatal("Reset destroyed the installed bytes")
	}
}

// occupiedPriorSlots are the ways the destination of a supersede's first
// rename is already taken. Reading either as a landed rename left the
// candidate no empty slot to land in: the swap record stayed outstanding, and
// every verb after it — Reset, the one way out of a state no other verb
// accepts, included — resumed into the same refusal forever.
var occupiedPriorSlots = []struct {
	name   string
	occupy func(*testing.T, string)
}{
	{"plain file", func(t *testing.T, slot string) {
		if err := os.WriteFile(slot, []byte("not a bundle"), 0o600); err != nil {
			t.Fatal(err)
		}
	}},
	{"stale directory", func(t *testing.T, slot string) {
		if err := os.MkdirAll(filepath.Join(slot, "Contents"), 0o700); err != nil {
			t.Fatal(err)
		}
	}},
}

func TestSupersedeClearsAnOccupiedPriorSlot(t *testing.T) {
	for _, tt := range occupiedPriorSlots {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.deploy.Install(f.ctx(), f.candidate("First", "1.0", "one")); err != nil {
				t.Fatalf("Install: %v", err)
			}
			tt.occupy(t, f.deploy.layout.prior)
			landed, err := f.deploy.Supersede(f.ctx(), f.candidate("Second", "2.0", "two"))
			if err != nil {
				t.Fatalf("Supersede: %v", err)
			}
			if landed.Version != "2.0" {
				t.Fatalf("Supersede = %+v, want the 2.0 generation", landed)
			}
			for name, path := range map[string]string{
				"swap":      f.deploy.layout.swap,
				"prior":     f.deploy.layout.prior,
				"candidate": f.deploy.layout.candidate,
			} {
				if fileExists(path) {
					t.Errorf("Supersede left %s behind at %q", name, path)
				}
			}
			if err := f.deploy.Reset(f.ctx()); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			installed, err := f.deploy.inspect(f.ctx(), f.app)
			if err != nil || installed.Version != "2.0" {
				t.Fatalf("installed = %+v, %v; want the superseding 2.0 generation", installed, err)
			}
		})
	}
}

// TestResetClearsAnOutstandingSwapWhosePriorSlotIsOccupied holds Reset to what
// its godoc promises: it is the escape hatch of last resort, so it clears a
// deployment whatever wrote the outstanding swap record and whatever occupies
// the slot that record's first rename has to move into.
func TestResetClearsAnOutstandingSwapWhosePriorSlotIsOccupied(t *testing.T) {
	for _, tt := range occupiedPriorSlots {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.crash("one", "two", func(t *testing.T, f *fixture) {
				tt.occupy(t, f.deploy.layout.prior)
			})
			if err := f.deploy.Reset(f.ctx()); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			f.wantCanonical("two")
			for name, path := range map[string]string{
				"swap":      f.deploy.layout.swap,
				"prior":     f.deploy.layout.prior,
				"candidate": f.deploy.layout.candidate,
			} {
				if fileExists(path) {
					t.Errorf("Reset left %s behind at %q", name, path)
				}
			}
		})
	}
}

// TestResetRefusesAnOccupiedPriorSlotWhileTheDaemonIsLive is the other half:
// clearing that slot destroys bytes a process can be running, so it is gated
// exactly as every other generation slot's destruction is.
func TestResetRefusesAnOccupiedPriorSlotWhileTheDaemonIsLive(t *testing.T) {
	f := newFixture(t)
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		writeMachO(t, filepath.Join(f.deploy.layout.prior, "Contents", "MacOS", "stale"), 0o755)
	})
	f.liveSlot()
	if err := f.deploy.Reset(f.ctx()); !errors.Is(err, ErrLive) {
		t.Fatalf("Reset err = %v, want ErrLive", err)
	}
	if !fileExists(filepath.Join(f.deploy.layout.prior, "Contents", "MacOS", "stale")) {
		t.Fatal("Reset cleared the prior slot with no absence proof behind it")
	}
	f.wantCanonical("one")
}

func TestResetRefusesWhileTheDaemonIsLive(t *testing.T) {
	f := newFixture(t)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	f.live(f.agent.Program)
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
	f.live(f.agent.Program)
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
		Daemon: daemonkit.Daemon{
			Label: "example",
			Trust: daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		},
		Agents: []launchd.Agent{{Label: "l", Program: "/opt/Example.app/Contents/MacOS/x"}},
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
		{"label traversing out of the state root", mutate(func(c *Config) { c.Daemon.Label = "../../evil" })},
		{"hidden label", mutate(func(c *Config) { c.Daemon.Label = ".hidden" })},
		{"label naming two path elements", mutate(func(c *Config) { c.Daemon.Label = "bin/daemon" })},
		{"absolute label", mutate(func(c *Config) { c.Daemon.Label = "/daemon" })},
		{"label with an embedded dot-dot", mutate(func(c *Config) { c.Daemon.Label = "com.example..daemon" })},
		{"label outside launchd's alphabet", mutate(func(c *Config) { c.Daemon.Label = "com example" })},
		{"no agents", mutate(func(c *Config) { c.Agents = nil })},
		{"program outside app", mutate(func(c *Config) { c.Agents[0].Program = "/usr/bin/true" })},
		{"relative program", mutate(func(c *Config) { c.Agents[0].Program = "Contents/MacOS/x" })},
		{"unclean program", mutate(func(c *Config) { c.Agents[0].Program = "/opt/Example.app/./Contents/MacOS/x" })},
		{"serving requirement that admits nobody", mutate(func(c *Config) {
			c.Daemon.Trust.Serving = daemonkit.ServingSigned(daemonkit.Requirement{TeamID: testTeamID})
		})},
		{"unstated serving posture", mutate(func(c *Config) { c.Daemon.Trust.Serving = daemonkit.Serving{} })},
		{"business set stated but empty", mutate(func(c *Config) {
			c.Daemon.Trust.Business = daemonkit.Requirements{}
		})},
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
	pinned := mutate(func(c *Config) {
		c.Daemon.Trust.Serving = daemonkit.ServingSigned(daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning})
	})
	if _, err := Open(pinned); err != nil {
		t.Fatalf("Open with a pinned serving requirement: %v", err)
	}
}

// TestOpenRefusesALabelWhoseRecordPathEscapesTheStateRoot is the traversal a
// "Label is required" check lets through. Daemon.RecordPath states the layout
// without running the Label rule, and the inventory gate every quiesce arm ends
// at reads that path — so a Label of "../../../evil" reaches Install,
// Supersede, Uninstall, Reset, and Quiesce as a file outside the state root
// entirely. Open is the boundary that refuses it, and the owner record planted
// at the escaped path is what proves the refusal is the only thing standing
// between a consumer and that read. The traversal is as deep as
// ~/.daemonkit/agents/<Label> is; a shallower one lands back inside the home
// directory and proves nothing.
func TestOpenRefusesALabelWhoseRecordPathEscapesTheStateRoot(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(root, "state", "home")
	if err := os.MkdirAll(filepath.Join(home, "Library", "LaunchAgents"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(realhome.EnvOverride, home)

	const label = daemonkit.Label("../../../evil")
	escaped := daemonkit.Daemon{Label: label}.RecordPath()
	if strings.HasPrefix(escaped, home+string(filepath.Separator)) {
		t.Fatalf("record path %q is inside the state root %q; this label no longer escapes", escaped, home)
	}
	if err := os.MkdirAll(filepath.Dir(escaped), 0o700); err != nil {
		t.Fatal(err)
	}
	openCtx, cancelOpen := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelOpen()
	store, err := proc.OpenStore(openCtx, escaped)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	planted, err := store.RecordOwner("planted-build")
	if err != nil {
		t.Fatalf("RecordOwner: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	app := filepath.Join(root, "Example.app")
	deployment, err := Open(Config{
		App:         app,
		Requirement: daemonkit.Requirement{TeamID: testTeamID, SigningIdentifier: testSigning},
		Daemon: daemonkit.Daemon{
			Label: label,
			Trust: daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		},
		Agents: []launchd.Agent{{Label: "l", Program: filepath.Join(app, "Contents", "MacOS", "x")}},
	})
	if err == nil {
		found, readErr := deployment.recordedIdentities()
		t.Fatalf(
			"Open accepted Label %q; the inventory gate read %v (err %v) out of %q, outside the state root %q — planted %v",
			label, found, readErr, escaped, home, planted.Identity(),
		)
	}
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("Open err = %v, want ErrConfig", err)
	}
}

// TestOpenResolvesDeclaredExecutables pins when a declared host binary is held
// to a real file. The scan-time matcher resolves the query itself, so the
// symlink-free form a process reports is compared either way; what Open adds is
// the moment. A declaration naming no file is ErrConfig here, rather than a
// query that narrows to its literal form mid-gate — the narrowing
// executableMatcher makes on purpose for an agent Program whose bundle was
// renamed aside, and which for a host binary is a gate that matches nothing.
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
		Daemon: daemonkit.Daemon{
			Label: "example",
			Trust: daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		},
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
