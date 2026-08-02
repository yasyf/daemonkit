//go:build mixedera

package mixedera

import (
	"bufio"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
)

const (
	readyLine      = "READY"
	readyWait      = 60 * time.Second
	peerWait       = 90 * time.Second
	conformWait    = 30 * time.Second
	drainWait      = 30 * time.Second
	aliveSettle    = 3 * time.Second
	preambleSettle = 15 * time.Second
	buildWait      = 5 * time.Minute

	daemonkitModule = "github.com/yasyf/daemonkit"

	// precutStartAttempts absorbs the released binary's trust-probe flake,
	// which burned all three of the previous attempts on 4 of 13 full-matrix
	// runs; each attempt costs that runtime's own 10s probe bound, and its root
	// cause is recorded in cc-notes note a40a3a1.
	precutStartAttempts = 8

	// precutProbeExit and precutProbeToken are the released peer's trust-probe
	// signature, and the harness retries only on both together: an exit status
	// alone is forgeable by any wrapper that happens to die the same way.
	precutProbeExit  = 69
	precutProbeToken = "mixedera-precut: trust-verifier-probe-deadline"

	failureProtocolMismatch = "protocol-mismatch"
	failureUntrusted        = "untrusted"

	// reapAbsent is how a peer reports daemonkit.ReapAbsent: the pinned identity
	// observed gone from the process table.
	reapAbsent = "absent"
)

type peer struct {
	era    string
	binary string
	module string
}

type healthReport struct {
	WireBuild string `json:"wire_build"`
	Protocol  int    `json:"protocol"`
	PID       int    `json:"pid"`
}

type report struct {
	Era          string       `json:"era"`
	Protocol     uint16       `json:"protocol"`
	PeerProtocol uint16       `json:"peer_protocol"`
	Session      bool         `json:"session"`
	Failure      string       `json:"failure"`
	Detail       string       `json:"detail"`
	SelfBuild    string       `json:"self_build"`
	PeerBuild    string       `json:"peer_build"`
	Health       healthReport `json:"health"`
	StopAcked    bool         `json:"stop_acked"`
	Reap         string       `json:"reap"`
	DrainedPID   int          `json:"drained_pid"`
	Socket       string       `json:"socket"`
	Self         string       `json:"self"`
}

type syncBuffer struct {
	mu    sync.Mutex
	bytes []byte
}

func (b *syncBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bytes = append(b.bytes, payload...)
	return len(payload), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.bytes)
}

type daemonProc struct {
	era     string
	home    string
	socket  string
	cmd     *exec.Cmd
	log     *syncBuffer
	exited  chan error
	settled chan struct{}
	once    sync.Once
	err     error

	signalled bool
}

// startFailure carries the peer's own exit and stderr so a caller can decide
// on the process's signature rather than on a formatted message.
type startFailure struct {
	era    string
	reason string
	stderr string
	err    error
}

func (f *startFailure) Error() string {
	return fmt.Sprintf("%s daemon %s: %v\nstderr:\n%s", f.era, f.reason, f.err, f.stderr)
}

func (f *startFailure) Unwrap() error { return f.err }

func startDaemon(t *testing.T, p peer, socket string, args ...string) *daemonProc {
	t.Helper()
	d, err := tryStartDaemon(t, p, socket, args...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func tryStartDaemon(t *testing.T, p peer, socket string, args ...string) (*daemonProc, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), p.binary, args...)
	cmd.WaitDelay = 5 * time.Second
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	log := &syncBuffer{}
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	d := &daemonProc{
		era: p.era, socket: socket, cmd: cmd, log: log,
		exited: make(chan error, 1), settled: make(chan struct{}),
	}
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == readyLine {
				close(ready)
				break
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
		d.exited <- cmd.Wait()
	}()
	t.Cleanup(func() {
		select {
		case <-d.settled:
			return
		default:
		}
		_ = cmd.Process.Kill()
		<-d.exited
	})
	select {
	case <-ready:
		return d, nil
	case err := <-d.exited:
		d.record(err)
		return nil, &startFailure{
			era: p.era, reason: "exited before " + readyLine, stderr: log.String(), err: err,
		}
	case <-time.After(readyWait):
		_ = cmd.Process.Kill()
		err := <-d.exited
		d.record(err)
		return nil, &startFailure{
			era:    p.era,
			reason: fmt.Sprintf("did not report %s within %s", readyLine, readyWait),
			stderr: log.String(), err: err,
		}
	}
}

func (d *daemonProc) record(err error) {
	d.once.Do(func() {
		d.err = err
		close(d.settled)
	})
}

func (d *daemonProc) exitWithin(t *testing.T, wait time.Duration) error {
	t.Helper()
	err := d.await(t, wait)
	if err == nil {
		d.witnessDrain(t)
	}
	return err
}

func (d *daemonProc) await(t *testing.T, wait time.Duration) error {
	t.Helper()
	select {
	case <-d.settled:
		return d.err
	case err := <-d.exited:
		d.record(err)
		return err
	case <-time.After(wait):
		t.Fatalf("%s daemon did not leave within %s\nstderr:\n%s", d.era, wait, d.log.String())
		return nil
	}
}

// witnessDrain attributes a clean exit to whichever repair channel the harness
// itself reached for, so the case never names the mechanism it is about to claim.
func (d *daemonProc) witnessDrain(t *testing.T) {
	t.Helper()
	if d.signalled {
		coverage.ObservedPresent(t, d.era, mechanismSigterm, coverage.FromProcessTable,
			fmt.Sprintf("the %s daemon at %s exited 0 after SIGTERM", d.era, d.socket))
	}
}

// leftWithin reports whether the OS reaped this daemon inside the settle. It is
// the one artifact under both directions of the preamble claim, and no case can
// write it.
func (d *daemonProc) leftWithin(t *testing.T, settle time.Duration) bool {
	t.Helper()
	select {
	case err := <-d.exited:
		d.record(err)
		return true
	case <-time.After(settle):
		return false
	}
}

func (d *daemonProc) aliveAfter(t *testing.T, settle time.Duration) {
	t.Helper()
	if d.leftWithin(t, settle) {
		t.Fatalf("%s daemon left while it was expected to hold the socket: %v\nstderr:\n%s",
			d.era, d.err, d.log.String())
	}
}

// witnessPreamble redeems the preamble against two artifacts no case writes: the
// bytes a relay copied to this daemon, and whether the OS reaped it afterwards.
// It files whichever direction those two show, so an ABSENT row costs what a
// PROVEN one costs — a preamble that never crossed the wire redeems nothing, and
// a daemon that did drain on one is filed as having drained rather than as
// having ignored it.
func (d *daemonProc) witnessPreamble(t *testing.T, front *relay, settle time.Duration) {
	t.Helper()
	preamble := frozen(t, preambleFixture)
	if !front.carried(preamble, drainWait) {
		t.Fatalf("no connection the relay at %s copied opened with exactly the frozen drain preamble %#x, so nothing put that preamble in front of the %s daemon at %s and there is no absence here to redeem",
			front.path, preamble, d.era, d.socket)
	}
	if d.leftWithin(t, settle) {
		coverage.ObservedPresent(t, d.era, mechanismPreamble, coverage.FromProcessTable, fmt.Sprintf(
			"the OS reaped the %s daemon at %s within %s of the relay carrying it the frozen drain preamble %#x",
			d.era, d.socket, settle, preamble,
		))
		return
	}
	coverage.ObservedAbsent(t, d.era, mechanismPreamble, coverage.FromProcessTable, fmt.Sprintf(
		"the %s daemon at %s still held its socket %s after the relay carried it the frozen drain preamble %#x",
		d.era, d.socket, settle, preamble,
	))
}

// witnessPreambleTrustGate redeems the preamble's trust gate against the same
// two artifacts witnessPreamble reads — the bytes a relay copied and whether
// the OS reaped the daemon they crossed to — for the one configuration that
// separates the gate from the drain: a daemon whose control lane names a
// requirement the preamble's writer cannot prove. It refuses a preamble the
// relay never carried, and files the direction the process table shows, so a
// strict daemon the preamble did drain is filed as drained rather than gated.
func (d *daemonProc) witnessPreambleTrustGate(t *testing.T, front *relay, settle time.Duration) {
	t.Helper()
	preamble := frozen(t, preambleFixture)
	if !front.carried(preamble, drainWait) {
		t.Fatalf("no connection the relay at %s copied opened with exactly the frozen drain preamble %#x, so nothing put that preamble in front of the strict %s daemon at %s and there is no refusal here to redeem",
			front.path, preamble, d.era, d.socket)
	}
	if d.leftWithin(t, settle) {
		coverage.ObservedAbsent(t, d.era, mechanismTrustGate, coverage.FromProcessTable, fmt.Sprintf(
			"the OS reaped the strict %s daemon at %s within %s of the relay carrying it the frozen drain preamble %#x",
			d.era, d.socket, settle, preamble,
		))
		return
	}
	coverage.ObservedPresent(t, d.era, mechanismTrustGate, coverage.FromProcessTable, fmt.Sprintf(
		"the strict %s daemon at %s still held its socket %s after the relay carried it the frozen drain preamble %#x",
		d.era, d.socket, settle, preamble,
	))
}

func (d *daemonProc) terminate(t *testing.T) {
	t.Helper()
	if err := d.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal %s daemon: %v", d.era, err)
	}
	d.signalled = true
}

func output(t *testing.T, p peer, wait time.Duration, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), p.binary, args...)
	cmd.WaitDelay = 5 * time.Second
	out, err := run(t, cmd, wait)
	if err != nil {
		t.Fatalf("%s %s: %v\nstdout:\n%s", p.era, strings.Join(args, " "), err, out)
	}
	return out
}

// declaredBy runs an era peer's own conformance verb, so what the manifest holds
// that era to is the peer process's account of itself rather than this
// harness's.
func declaredBy(t *testing.T, p peer) coverage.Declaration {
	t.Helper()
	return coverage.Declaration{Era: p.era, JSON: output(t, p, conformWait, "conformance")}
}

func runPeer(t *testing.T, p peer, wait time.Duration, args ...string) (report, time.Duration) {
	t.Helper()
	started := time.Now()
	out := output(t, p, wait, args...)
	elapsed := time.Since(started)
	var decoded report
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode %s report %q: %v", p.era, out, err)
	}
	witnessVerdict(t, p, decoded, elapsed)
	return decoded, elapsed
}

// witnessVerdict takes the built peer's own classification as evidence: the
// harness cannot write it, because it is produced by a separate process from
// what that process actually read off the socket. Each branch files only what
// that verdict carries — a session at one protocol on both sides, or a mismatch
// the peer typed inside the bound this gate calls prompt.
func witnessVerdict(t *testing.T, p peer, decoded report, elapsed time.Duration) {
	t.Helper()
	switch {
	case decoded.Session && decoded.Protocol == decoded.PeerProtocol:
		coverage.ObservedPresent(t, p.era, mechanismSession, coverage.FromPeerVerdict, fmt.Sprintf(
			"the %s peer completed a request and its response over one unix socket against a peer at its own protocol %d",
			p.era, decoded.PeerProtocol,
		))
	case decoded.Failure == failureProtocolMismatch:
		if elapsed > refuseBound {
			t.Fatalf("the %s peer typed its refusal as %s after %s, over the %s this gate calls prompt: a refusal that slow is the wedge, not the gate",
				p.era, decoded.Failure, elapsed, refuseBound)
		}
		coverage.ObservedPresent(t, p.era, mechanismGate, coverage.FromPeerVerdict, fmt.Sprintf(
			"the %s peer typed its own refusal as %s in %s: %s",
			p.era, decoded.Failure, elapsed.Round(time.Millisecond), decoded.Detail,
		))
	}
}

// witnessControlTrustGate names every half of the control lane's drain trust
// gate in one fact, so none redeems it alone: a gate that only ever refuses is
// a broken lane, and one that only ever admits is no gate. It refuses each half
// on the reports themselves rather than trusting its caller to have refused it,
// because a witness that only formats what it is handed leaves its row PROVEN
// with the calling case emptied. All three halves are the peer process's own
// verdicts on what it read back — the refusal, the session the refusing
// incumbent went on to complete, and the reap.
func witnessControlTrustGate(t *testing.T, refused, served, honoured report, strict, open *daemonProc) {
	t.Helper()
	switch {
	case refused.Failure != failureUntrusted:
		t.Fatalf("the cut daemon at %s, whose control lane names a requirement this peer cannot prove, answered that peer's drain with %+v, want %s",
			strict.socket, refused, failureUntrusted)
	case !served.Session || served.Protocol != served.PeerProtocol:
		t.Fatalf("the cut daemon that refused this peer's drain answered that same peer's business-lane session with %+v, want a session completed at one protocol: a control-lane refusal leaves the incumbent serving",
			served)
	case served.Socket != strict.socket:
		t.Fatalf("the session that outlived the refused drain ran against %q, want the socket the drain was refused from, %q",
			served.Socket, strict.socket)
	case honoured.Failure != "" || honoured.Reap != reapAbsent:
		t.Fatalf("the drain of a cut daemon naming no control requirement = %+v, want a delivered drain reaping %q",
			honoured, reapAbsent)
	case honoured.DrainedPID != open.cmd.Process.Pid:
		t.Fatalf("the honoured drain proves pid %d gone, want the daemon it stopped, %d",
			honoured.DrainedPID, open.cmd.Process.Pid)
	case refused.Self != served.Self || served.Self != honoured.Self:
		t.Fatalf("the refusal, the session, and the drain name themselves %q, %q, and %q: this gate is one peer identity meeting two daemons, not three peers meeting one each",
			refused.Self, served.Self, honoured.Self)
	}
	coverage.ObservedPresent(t, cutEra, mechanismControlTrustGate, coverage.FromPeerVerdict, fmt.Sprintf(
		"the cut peer %s, whose drain the cut daemon at %s typed %s because that daemon's control lane names a requirement the peer cannot prove, completed a session at protocol %d against that same socket, and drained the cut daemon at %s that names no control requirement, reaping %q for pid %d",
		refused.Self, strict.socket, refused.Failure, served.PeerProtocol, open.socket, honoured.Reap, honoured.DrainedPID,
	))
}

func run(t *testing.T, cmd *exec.Cmd, wait time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	var stdout, stderr syncBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return stdout.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return stdout.String(), nil
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-done
		return stdout.String(), fmt.Errorf("timed out after %s: %s", wait, strings.TrimSpace(stderr.String()))
	}
}

// eraModule is what an era's peer compiles against: the requirement its go.mod
// carries, and the daemonkit release the built binary must be provably linked
// against — empty for the era that is this working tree.
type eraModule struct {
	directive string
	release   string
}

func moduleOf(t *testing.T, era string) eraModule {
	t.Helper()
	if era == precutEra {
		return eraModule{
			directive: fmt.Sprintf("require %s %s", daemonkitModule, precutBoundary),
			release:   precutBoundary,
		}
	}
	t.Fatalf("no foreign module defines the %q era", era)
	return eraModule{}
}

// buildPeer compiles the era's conformance peer. The cut era's peer is in-tree,
// built against this working tree's internal/wire directly; a released era's
// peer is built in its own module against the pinned release and proven linked
// to it. The era's name and the module it was built from cannot be chosen
// separately: a released requirement repointed at this tree — the edit that
// makes a build error go away — leaves the binary carrying a replaced daemonkit
// and fails here instead of running as the boundary under the boundary's label.
func buildPeer(t *testing.T, era string) peer {
	t.Helper()
	if era == cutEra {
		return buildInTreePeer(t)
	}
	defines := moduleOf(t, era)
	dir := t.TempDir()
	sources, err := filepath.Glob(filepath.Join("testdata", era, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range sources {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(path)), source, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	manifest := fmt.Sprintf("module mixedera/peer/%s\n\ngo %s\n\n%s\n", era, goDirective(t), defines.directive)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, era)
	goRun(t, dir, "mod", "tidy")
	goRun(t, dir, "build", "-o", binary, ".")
	built := peer{era: era, binary: binary, module: dir}
	if defines.release != "" {
		assertLinksRelease(t, built, defines.release)
	}
	return built
}

// buildInTreePeer compiles the cut peer from ci/mixedera/cutpeer against this
// module, so it links this working tree's internal/wire — which Go's
// internal-package rule bars the foreign per-era module from importing. There is
// no release to link-check: the cut era is this tree.
func buildInTreePeer(t *testing.T) peer {
	t.Helper()
	root := repoRoot(t)
	binary := filepath.Join(t.TempDir(), cutEra)
	goRun(t, root, "build", "-tags", "mixedera", "-o", binary, "./ci/mixedera/cutpeer")
	return peer{era: cutEra, binary: binary, module: filepath.Join(root, "ci", "mixedera", "cutpeer")}
}

// assertLinksRelease reads the module versions the linker embedded in the built
// binary, which no label, log line, or summary heading can stand in for.
func assertLinksRelease(t *testing.T, p peer, release string) {
	t.Helper()
	info, err := buildinfo.ReadFile(p.binary)
	if err != nil {
		t.Fatalf("read the %s peer's build info: %v", p.era, err)
	}
	for _, dep := range info.Deps {
		if dep.Path != daemonkitModule {
			continue
		}
		switch {
		case dep.Replace != nil:
			t.Fatalf("the %s peer links %s %s replaced by %s %s, not the boundary release %s: this peer is that replacement wearing the %s label",
				p.era, daemonkitModule, dep.Version,
				dep.Replace.Path, dep.Replace.Version, release, p.era)
		case dep.Version != release:
			t.Fatalf("the %s peer links %s %s, want the boundary release %s",
				p.era, daemonkitModule, dep.Version, release)
		}
		t.Logf("the %s peer links %s %s", p.era, daemonkitModule, dep.Version)
		return
	}
	t.Fatalf("the %s peer links no %s at all, so no part of it is the boundary release %s",
		p.era, daemonkitModule, release)
}

func goRun(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off")
	out, err := run(t, cmd, buildWait)
	if err != nil {
		t.Fatalf("go %s in %s: %v\n%s", strings.Join(args, " "), dir, err, out)
	}
	return out
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func goDirective(t *testing.T) string {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(manifest)) {
		if directive, ok := strings.CutPrefix(strings.TrimSpace(line), "go "); ok {
			return strings.TrimSpace(directive)
		}
	}
	t.Fatal("go.mod declares no go directive")
	return ""
}

// socketDir mirrors wiretest.SocketDir: macOS caps sun_path at 104 bytes,
// which t.TempDir routinely exceeds.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-mixedera-%d-", os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
