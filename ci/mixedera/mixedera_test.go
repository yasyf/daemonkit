//go:build mixedera

// Package mixedera is daemonkit's release gate for the boundary between the
// pre-cut release line and the cut: it drives a daemon and a client of
// different eras at each other over a real unix socket and proves the repair
// channel outlives the compatibility gate.
//
// It exists because the swarm era gated the transport on an application
// identity: a launchd daemon holding one schema met an upgraded client holding
// another, the handshake failed, and the client could no longer reach the wire
// to tell the daemon to drain — the one action that repairs the condition.
// 18,999 handshake failures over five days, behind green CI in every repo.
// DESIGN §8.4 makes this gate non-waivable on every release.
//
// The verdict is not this package's to write. Every observation the matrix owes
// is frozen in testdata/frozen/observations.txt and every mechanism it names in
// testdata/frozen/mechanisms.txt, both read on every access rather than held
// parsed; and the state those two files govern — the coverage rows, the ledger
// of observations, the evidence journal, the seal over the frozen text — lives
// in ci/mixedera/coverage, which exports the redemption verbs and nothing this
// package can assign. What is left here is the harness: it builds the two era
// peers, drives them at each other, and observes. It cannot write a row
// redeemed, mark a claim observed, drop an era from the record, or rewrite the
// frozen text it reads — that state is unexported, so each is a compile error
// rather than a finding — and a fact filed past the witness mechanisms.txt
// reserves is refused at the journal.
//
// What no package boundary reaches is this harness misreporting what it saw:
// every witness is handed the artifact it judges — a parameter, or a field of
// the harness's own handle, as (*daemonProc).witnessDrain attributes a clean
// exit to SIGTERM on a field this package sets when it signals — and a case
// body can build that artifact itself. What remains possible, and for whom, is
// accounted for in testdata/frozen/mechanisms.txt.
package mixedera

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/ci/mixedera/coverage"
	"github.com/yasyf/daemonkit/version"
)

const (
	summaryEnv  = "GITHUB_STEP_SUMMARY"
	coverageEnv = "MIXED_ERA_COVERAGE"
)

func TestMain(m *testing.M) {
	into := coverage.Destinations{Record: os.Getenv(coverageEnv), Summary: os.Getenv(summaryEnv)}
	if err := coverage.Bind(into); err != nil {
		fmt.Fprintf(os.Stderr, "mixed-era: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	if err := coverage.Settle(code == 0); err != nil {
		fmt.Fprintf(os.Stderr, "mixed-era: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

const (
	precutBuildA = "mixedera.precut.a"
	precutBuildB = "mixedera.precut.b"

	precutBoundary    = "v0.20.10"
	buildSkewDemotion = "v0.21.0"

	refuseBound             = 10 * time.Second
	maxHandshakeConnections = 4

	// cutLabel is the daemonkit.Label ci/mixedera/cutpeer serves under, which
	// with the state root the harness hands that process is what every path the
	// cut daemon owns derives from.
	cutLabel = "mixedera"
)

type peers struct {
	precut  peer
	cut     peer
	covered *coverage.Manifest
}

type gateCase struct {
	name string
	run  func(t *testing.T, p *peers)
}

var gateCases = []gateCase{
	{
		name: "session/precut",
		run: func(t *testing.T, p *peers) {
			daemon := startPrecut(t, p.precut, precutBuildA)
			front := newRelay(t, daemon.socket)
			result, _ := runPeer(t, p.precut, peerWait,
				"dial", "-socket", front.path, "-build", precutBuildA)
			if !result.Session {
				t.Fatalf("same-era session failed: %+v", result)
			}
			if result.PeerBuild != precutBuildA || result.PeerProtocol != precutProtocol {
				t.Errorf("peer identity = build %q protocol %d, want %q and %d",
					result.PeerBuild, result.PeerProtocol, precutBuildA, precutProtocol)
			}
			if result.Health.WireBuild != precutBuildA || result.Health.Protocol != precutProtocol {
				t.Errorf("health = %+v, want build %q protocol %d",
					result.Health, precutBuildA, precutProtocol)
			}
			if !result.StopAcked {
				t.Error("the pre-cut daemon did not acknowledge stop")
			}
			assertBothSidesFramed(t, front.quiesce(t), precutEra, precutEra)
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("pre-cut daemon exited %v\nstderr:\n%s", err, daemon.log.String())
			}
			p.covered.Redeem(t, precutEra, mechanismSession, mechanismFrame)
		},
	},
	{
		name: "session/cut",
		run: func(t *testing.T, p *peers) {
			daemon := startCut(t, p.cut)
			front := newRelay(t, daemon.socket)
			result, _ := runPeer(t, p.cut, peerWait, "dial", "-socket", front.path)
			if !result.Session {
				t.Fatalf("same-era session failed: %+v", result)
			}
			if result.Protocol != cutProtocol || result.PeerProtocol != cutProtocol {
				t.Errorf("session protocol = self %d peer %d, want %d on both",
					result.Protocol, result.PeerProtocol, cutProtocol)
			}
			assertBothSidesFramed(t, front.quiesce(t), cutEra, cutEra)
			daemon.terminate(t)
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("cut daemon exited %v\nstderr:\n%s", err, daemon.log.String())
			}
			p.covered.Redeem(t, cutEra, mechanismSession, mechanismFrame)
		},
	},
	{
		name: "skew/build",
		run: func(t *testing.T, p *peers) {
			accepts := !version.Newer(buildSkewDemotion, precutBoundary)
			t.Logf("boundary %s against the demotion release %s: build skew is expected to %s",
				precutBoundary, buildSkewDemotion, refusedOrAccepted(accepts))

			daemon := startPrecut(t, p.precut, precutBuildA)
			result, _ := runPeer(t, p.precut, peerWait,
				"dial", "-socket", daemon.socket, "-build", precutBuildB)
			switch {
			case accepts && !result.Session:
				t.Errorf("%s accepts build skew and this session failed: %+v", precutBoundary, result)
			case !accepts && result.Failure != "refused":
				t.Errorf("%s gates on the exact build, so a skewed client is refused; got %+v",
					precutBoundary, result)
			}
			if !result.Session {
				daemon.terminate(t)
			}
			_ = daemon.exitWithin(t, drainWait)
		},
	},
	{
		name: "refuse/precut-client-cut-daemon",
		run: func(t *testing.T, p *peers) {
			daemon := startCut(t, p.cut)
			front := newRelay(t, daemon.socket)
			result, elapsed := runPeer(t, p.precut, peerWait,
				"dial", "-socket", front.path, "-build", precutBuildA)
			assertCrispRefusal(t, result, elapsed, front, precutEra, cutEra)
			daemon.aliveAfter(t, aliveSettle)
			p.covered.Redeem(t, precutEra, mechanismGate)
			p.covered.Redeem(t, cutEra, mechanismGate)
		},
	},
	{
		name: "refuse/cut-client-precut-daemon",
		run: func(t *testing.T, p *peers) {
			daemon := startPrecut(t, p.precut, precutBuildA)
			front := newRelay(t, daemon.socket)
			result, elapsed := runPeer(t, p.cut, peerWait, "dial", "-socket", front.path)
			assertCrispRefusal(t, result, elapsed, front, cutEra, precutEra)
			if result.PeerProtocol != precutProtocol {
				t.Errorf("the cut client refused carrying peer protocol %d, want the pre-cut %d: a mismatch without the peer's version is not a typed one",
					result.PeerProtocol, precutProtocol)
			}
			daemon.aliveAfter(t, aliveSettle)
			p.covered.Redeem(t, cutEra, mechanismGate)
			p.covered.Redeem(t, precutEra, mechanismGate)
			daemon.terminate(t)
			_ = daemon.exitWithin(t, drainWait)
		},
	},
	{
		name: "classify/cut",
		run: func(t *testing.T, p *peers) {
			t.Log(goRun(t, p.cut.module, "test", "-count=1", "-tags", "mixedera", "./..."))
		},
	},
	{
		name: "drain/sigterm-precut",
		run: func(t *testing.T, p *peers) {
			daemon := startPrecut(t, p.precut, precutBuildA)
			daemon.terminate(t)
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("SIGTERM ends the pre-cut daemon cleanly; got %v\nstderr:\n%s",
					err, daemon.log.String())
			}
			p.covered.Redeem(t, precutEra, mechanismSigterm)
		},
	},
	{
		name: "drain/sigterm-cut",
		run: func(t *testing.T, p *peers) {
			daemon := startCut(t, p.cut)
			daemon.terminate(t)
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("the cut daemon arms signals first and drains, so SIGTERM ends it cleanly; got %v\nstderr:\n%s",
					err, daemon.log.String())
			}
			p.covered.Redeem(t, cutEra, mechanismSigterm)
		},
	},
	{
		name: "drain/preamble-emitted",
		run: func(t *testing.T, p *peers) {
			daemon := startCut(t, p.cut)
			front := newRelay(t, daemon.socket)
			held := parkHandshake(t, front)
			daemon.terminate(t)
			awaitIntakeClosed(t, daemon.socket, drainWait)
			assertPreambleAnswered(t, held.answer(t), front.quiesce(t))
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("the cut daemon that answered the parked handshake with the frozen preamble exited %v\nstderr:\n%s",
					err, daemon.log.String())
			}
			p.covered.Redeem(t, cutEra, mechanismPreambleEmitted)
		},
	},
	{
		name: "drain/control-trust-gate",
		run: func(t *testing.T, p *peers) {
			strict := startCut(t, p.cut, "-strict")
			refused, _ := runPeer(t, p.cut, peerWait, "drain", "-home", strict.home)
			strict.aliveAfter(t, aliveSettle)
			served, _ := runPeer(t, p.cut, peerWait, "dial", "-socket", strict.socket)

			open := startCut(t, p.cut)
			honoured, _ := runPeer(t, p.cut, peerWait, "drain", "-home", open.home)
			if err := open.exitWithin(t, drainWait); err != nil {
				t.Errorf("the drained cut daemon exited %v\nstderr:\n%s", err, open.log.String())
			}
			witnessControlTrustGate(t, refused, served, honoured, strict, open)
			strict.terminate(t)
			_ = strict.exitWithin(t, drainWait)
			p.covered.Redeem(t, cutEra, mechanismControlTrustGate)
		},
	},
	{
		name: "drain/preamble-absent-precut",
		run: func(t *testing.T, p *peers) {
			daemon := startPrecut(t, p.precut, precutBuildA)
			front := newRelay(t, daemon.socket)
			conn := writePreamble(t, front)
			daemon.witnessPreamble(t, front, preambleSettle)
			awaitPeerClose(t, conn, aliveSettle)
			p.covered.Redeem(t, precutEra, mechanismPreamble, mechanismTrustGate)
			daemon.terminate(t)
			_ = daemon.exitWithin(t, drainWait)
		},
	},
	{
		name: "drain/preamble-cut",
		run: func(t *testing.T, p *peers) {
			strict := startCut(t, p.cut, "-strict")
			strictFront := newRelay(t, strict.socket)
			held := writePreamble(t, strictFront)
			strict.witnessPreambleTrustGate(t, strictFront, aliveSettle)
			awaitPeerClose(t, held, aliveSettle)
			strict.terminate(t)
			_ = strict.exitWithin(t, drainWait)

			open := startCut(t, p.cut)
			front := newRelay(t, open.socket)
			conn := writePreamble(t, front)
			open.witnessPreamble(t, front, preambleSettle)
			awaitPeerClose(t, conn, aliveSettle)
			p.covered.Redeem(t, cutEra, mechanismPreamble, mechanismTrustGate)
		},
	},
	{
		name: "wedge/18999",
		run: func(t *testing.T, p *peers) {
			daemon := startCut(t, p.cut)
			front := newRelay(t, daemon.socket)

			wedged, elapsed := runPeer(t, p.precut, peerWait,
				"dial", "-socket", front.path, "-build", precutBuildA)
			assertCrispRefusal(t, wedged, elapsed, front, precutEra, cutEra)
			daemon.aliveAfter(t, aliveSettle)

			repaired, _ := runPeer(t, p.cut, peerWait, "drain", "-home", daemon.home)
			if repaired.Failure != "" || repaired.Reap != reapAbsent {
				t.Fatalf("the incumbent that refused the handshake answered its own repair channel with %+v, want a delivered drain reaping %q",
					repaired, reapAbsent)
			}
			if err := daemon.exitWithin(t, drainWait); err != nil {
				t.Errorf("the incumbent that refused the handshake did not drain: %v\nstderr:\n%s",
					err, daemon.log.String())
			}
			p.covered.Redeem(t, cutEra, mechanismGate)
			p.covered.Redeem(t, precutEra, mechanismGate)
		},
	},
}

func TestProbeDeadlineNeedsBothTheStatusAndTheToken(t *testing.T) {
	tests := []struct {
		name    string
		failure error
		retry   bool
		saw     string
	}{
		{
			"the released peer's own signature",
			probeFailure(t, precutProbeExit, "serve: begin: trust verifier probe\n"+precutProbeToken+"\n"),
			true, "exited 69 with that line",
		},
		{
			"an unrelated wrapper leaving by the same status",
			probeFailure(t, precutProbeExit, "wrapper: upstream unavailable\n"),
			false, "exited 69 without that line",
		},
		{
			"the token without the status",
			probeFailure(t, 1, precutProbeToken+"\n"),
			false, "exited 1 with that line",
		},
		{
			"the token embedded in a longer line",
			probeFailure(t, precutProbeExit, "note: "+precutProbeToken+" was expected\n"),
			false, "exited 69 without that line",
		},
		{"a start that never reached the peer", errors.New("fork/exec: permission denied"), false, "never reached the peer's own exit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retry, saw := probeDeadline(tt.failure)
			if retry != tt.retry {
				t.Errorf("retry = %t, want %t", retry, tt.retry)
			}
			if saw != tt.saw {
				t.Errorf("saw = %q, want %q", saw, tt.saw)
			}
		})
	}
	coverage.Observe(t)
}

func probeFailure(t *testing.T, code int, stderr string) error {
	t.Helper()
	exit := exec.CommandContext(t.Context(), "sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	if exit == nil {
		t.Fatalf("sh -c 'exit %d' left no exit error", code)
	}
	return &startFailure{era: precutEra, reason: "exited before " + readyLine, stderr: stderr, err: exit}
}

func TestPrecutBoundaryPredatesTheCut(t *testing.T) {
	if !version.Newer(buildSkewDemotion, precutBoundary) {
		t.Fatalf("boundary %s is not older than the cut's first release %s: the pre-cut peer would compile against the rewrite",
			precutBoundary, buildSkewDemotion)
	}
	coverage.Observe(t)
}

func TestMixedEra(t *testing.T) {
	t.Logf("mixed-era boundary: the working tree against %s", precutBoundary)

	p := &peers{
		precut: buildPeer(t, precutEra),
		cut:    buildPeer(t, cutEra),
	}
	p.covered = coverage.NewManifest(t, precutBoundary, declaredBy(t, p.precut), declaredBy(t, p.cut))
	t.Cleanup(func() { p.covered.Finish(t) })

	for _, gate := range gateCases {
		t.Run(gate.name, func(t *testing.T) {
			gate.run(t, p)
			coverage.Observe(t)
		})
	}
	coverage.Observe(t)
}

// assertCrispRefusal redeems the daemon era's gate off the wire: the relay saw
// the daemon answer with its own frozen frame identity, and a separate process
// read a typed protocol mismatch out of that answer.
func assertCrispRefusal(
	t *testing.T,
	result report,
	elapsed time.Duration,
	front *relay,
	clientEra, daemonEra string,
) {
	t.Helper()
	if result.Failure != failureProtocolMismatch {
		t.Errorf("failure = %q (%s), want %s", result.Failure, result.Detail, failureProtocolMismatch)
	}
	if elapsed > refuseBound {
		t.Errorf("the refusal took %s, want under %s", elapsed, refuseBound)
	}
	crossings := front.quiesce(t)
	if len(crossings) > maxHandshakeConnections {
		t.Errorf("the refused client opened %d connections, want at most %d",
			len(crossings), maxHandshakeConnections)
	}
	answered := assertBothSidesFramed(t, crossings, clientEra, daemonEra)
	if !answered || result.Failure != failureProtocolMismatch || elapsed > refuseBound {
		return
	}
	coverage.ObservedPresent(t, daemonEra, mechanismGate, coverage.FromWire, fmt.Sprintf(
		"the %s daemon answered each of the %d connections the refused %s client opened with its own frozen frame prefix, and that client read a typed %s out of the answer",
		daemonEra, len(crossings), clientEra, failureProtocolMismatch,
	))
}

func assertBothSidesFramed(t *testing.T, crossings []exchange, clientEra, daemonEra string) bool {
	t.Helper()
	if len(crossings) == 0 {
		t.Error("the relay copied no connection at all")
		return false
	}
	framed := true
	for i, crossing := range crossings {
		where := fmt.Sprintf("%d of %d", i+1, len(crossings))
		framed = assertFramePrefix(t, "client connection "+where, crossing.opened, clientEra) && framed
		framed = assertFramePrefix(t, "daemon connection "+where, crossing.answered, daemonEra) && framed
	}
	return framed
}

func assertFramePrefix(t *testing.T, side string, observed []byte, era string) bool {
	t.Helper()
	want := frozen(t, frameFixture(era))
	if !carriesFramePrefix(observed, want) {
		t.Errorf("the %s wrote %#x, which does not carry the frozen %s prefix %#x at offset %d",
			side, observed, era, want, framePrefixOffset)
		return false
	}
	return true
}

func refusedOrAccepted(accepts bool) string {
	if accepts {
		return "be accepted"
	}
	return "be refused"
}

// startPrecut retries the released binary's start on one adjudicated flake and
// nothing else: the pre-cut runtime bounds its trust-verifier self-probe at
// 10s, which fork/exec latency on a loaded runner loses, and no change to this
// tree can reach an already-released binary to widen it.
func startPrecut(t *testing.T, p peer, build string) *daemonProc {
	t.Helper()
	var probes []string
	for attempt := 1; attempt <= precutStartAttempts; attempt++ {
		dir := socketDir(t)
		socket := filepath.Join(dir, "d.sock")
		daemon, err := tryStartDaemon(t, p, socket,
			"serve", "-socket", socket, "-build", build, "-state", dir)
		if err == nil {
			return daemon
		}
		probe, saw := probeDeadline(err)
		if !probe {
			t.Fatalf("the pre-cut daemon failed to start, and the only failure this gate retries is its trust-verifier self-probe, which prints %q on stderr and then exits %d. This start %s:\n%v",
				precutProbeToken, precutProbeExit, saw, err)
		}
		probes = append(probes, fmt.Sprintf("attempt %d of %d: %s\n%v", attempt, precutStartAttempts, saw, err))
		t.Logf("the pre-cut trust-verifier self-probe missed its deadline, %s", probes[len(probes)-1])
	}
	t.Fatalf("the pre-cut daemon never cleared its trust-verifier self-probe in %d attempts. This is the known environmental flake, not a regression in this tree: the released runtime bounds its self-probe at 10s, and fork/exec latency for the same trivial spawn has been measured anywhere from 0.00s to 13.66s on a loaded machine (cc-notes note a40a3a1). Nothing in this tree can reach an already-released binary to widen that bound.\n%s",
		precutStartAttempts, strings.Join(probes, "\n"))
	return nil
}

// probeDeadline reports whether a failed pre-cut start carries the released
// peer's full trust-probe signature — the token it prints immediately before
// leaving AND the exit status — and describes what it saw either way.
func probeDeadline(err error) (bool, string) {
	var failure *startFailure
	if !errors.As(err, &failure) {
		return false, "never reached the peer's own exit"
	}
	tokened := slices.Contains(strings.Split(failure.stderr, "\n"), precutProbeToken)
	saw := "without that line"
	if tokened {
		saw = "with that line"
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false, fmt.Sprintf("left without an exit status, %s", saw)
	}
	return exit.ExitCode() == precutProbeExit && tokened,
		fmt.Sprintf("exited %d %s", exit.ExitCode(), saw)
}

func startCut(t *testing.T, p peer, args ...string) *daemonProc {
	t.Helper()
	home := socketDir(t)
	daemon := startDaemon(t, p, filepath.Join(home, cutLabel, "daemon.sock"),
		append([]string{"serve", "-home", home}, args...)...)
	daemon.home = home
	return daemon
}
