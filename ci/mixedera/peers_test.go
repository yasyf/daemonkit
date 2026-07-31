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
)

const (
	readyLine      = "READY"
	readyWait      = 60 * time.Second
	peerWait       = 90 * time.Second
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
	socket  string
	cmd     *exec.Cmd
	log     *syncBuffer
	exited  chan error
	settled chan struct{}
	once    sync.Once
	err     error

	signalled bool
	preambled bool
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
	switch {
	case d.signalled:
		observedPresent(t, d.era, mechanismSigterm, fromProcessTable,
			fmt.Sprintf("the %s daemon at %s exited 0 after SIGTERM", d.era, d.socket))
	case d.preambled:
		observedPresent(t, d.era, mechanismPreamble, fromProcessTable,
			fmt.Sprintf("the %s daemon at %s exited 0 after the frozen drain preamble reached it, with no signal sent",
				d.era, d.socket))
	}
}

func (d *daemonProc) aliveAfter(t *testing.T, settle time.Duration) {
	t.Helper()
	select {
	case err := <-d.exited:
		d.record(err)
		t.Fatalf("%s daemon left while it was expected to hold the socket: %v\nstderr:\n%s",
			d.era, err, d.log.String())
	case <-time.After(settle):
	}
	if d.preambled {
		observedAbsent(t, d.era, mechanismPreamble, fromProcessTable,
			fmt.Sprintf("the %s daemon at %s still held its socket %s after the frozen drain preamble reached it",
				d.era, d.socket, settle))
	}
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

func runPeer(t *testing.T, p peer, wait time.Duration, args ...string) (report, time.Duration) {
	t.Helper()
	started := time.Now()
	out := output(t, p, wait, args...)
	elapsed := time.Since(started)
	var decoded report
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("decode %s report %q: %v", p.era, out, err)
	}
	witnessVerdict(t, p, decoded)
	return decoded, elapsed
}

// witnessVerdict takes the built peer's own classification as evidence: the
// harness cannot write it, because it is produced by a separate process from
// what that process actually read off the socket.
func witnessVerdict(t *testing.T, p peer, decoded report) {
	t.Helper()
	switch {
	case decoded.Session && decoded.PeerProtocol != 0:
		observedPresent(t, p.era, mechanismSession, fromPeerVerdict, fmt.Sprintf(
			"the %s peer completed a session at protocol %d against a peer at protocol %d",
			p.era, decoded.Protocol, decoded.PeerProtocol,
		))
	case decoded.Failure == failureProtocolMismatch:
		observedPresent(t, p.era, mechanismGate, fromPeerVerdict, fmt.Sprintf(
			"the %s peer typed its own refusal as %s: %s", p.era, decoded.Failure, decoded.Detail,
		))
	}
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
