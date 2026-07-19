//go:build darwin && !daemonkit_unsigned

package trust

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

// These tests PIN query-time peer identity on the unix-socket transport: a peer
// that execs a different image after connecting authenticates as its CURRENT image.

const (
	peerSubSockEnv = "DAEMONKIT_PEERSUB_SOCK"
	peerSubExecEnv = "DAEMONKIT_PEERSUB_EXEC"
	peerSubArgsEnv = "DAEMONKIT_PEERSUB_ARGS"
)

// TestMain runs the substitution helper on a re-exec (peerSubSockEnv set), else the
// suite. The helper execs a foreign image, never os.Executable() — no fork-bomb risk.
func TestMain(m *testing.M) {
	if os.Getenv(peerSubSockEnv) != "" {
		runPeerSub()
		os.Exit(70) // exec failed; unreachable on success
	}
	os.Exit(m.Run())
}

// runPeerSub dials, waits for the go-ahead byte, then execs peerSubExecEnv on the
// SAME pid, holding a non-CLOEXEC dup open so last_pid still resolves this pid.
func runPeerSub() {
	conn, err := net.Dial("unix", os.Getenv(peerSubSockEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "peersub dial: %v\n", err)
		os.Exit(71)
	}
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		fmt.Fprintf(os.Stderr, "peersub read: %v\n", err)
		os.Exit(72)
	}
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "peersub syscallconn: %v\n", err)
		os.Exit(73)
	}
	var (
		heldFD int
		dupErr error
	)
	// dup(2) clears CLOEXEC on the new fd; it outlives conn and the exec, holding
	// the peer socket end open independently of any Go object.
	if err := raw.Control(func(fd uintptr) { heldFD, dupErr = syscall.Dup(int(fd)) }); err != nil || dupErr != nil {
		os.Exit(74)
	}
	_ = heldFD
	target := os.Getenv(peerSubExecEnv)
	argv := append([]string{target}, strings.Fields(os.Getenv(peerSubArgsEnv))...)
	_ = syscall.Exec(target, argv, os.Environ())
	os.Exit(75)
}

// spawnPeerSub re-execs this test binary as the connecting helper (which becomes
// execTarget on the same pid) and returns the accepted server-side connection.
func spawnPeerSub(t *testing.T, execTarget, execArgs string) *net.UnixConn {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-sub") // short path: unix socket names cap at ~104 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), peerSubSockEnv+"="+sock, peerSubExecEnv+"="+execTarget, peerSubArgsEnv+"="+execArgs)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()
	select {
	case a := <-ch:
		if a.err != nil {
			t.Fatalf("accept: %v", a.err)
		}
		t.Cleanup(func() { _ = a.conn.Close() })
		return a.conn.(*net.UnixConn)
	case <-time.After(5 * time.Second):
		t.Fatal("helper never connected")
		return nil
	}
}

// requireVerifier loads the Security.framework verifier or skips.
func requireVerifier(t *testing.T) {
	t.Helper()
	secOnce.Do(loadSecurity)
	if secErr != nil {
		t.Skipf("Security.framework verifier unavailable: %v", secErr)
	}
}

// resolvesTo reads the peer's CURRENT audit token (query-time) and reports whether
// the resolved SecCode satisfies req.
func resolvesTo(t *testing.T, conn *net.UnixConn, req string) bool {
	t.Helper()
	peer, err := wire.PeerFromConn(conn)
	if err != nil {
		t.Fatalf("PeerFromConn: %v", err)
	}
	guest, err := copyGuest(peer.Audit)
	if err != nil {
		return false
	}
	defer cfRelease(guest)
	return checkValidity(guest, req) == nil
}

func eventually(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPeerTokenIsQueryTimeLive_TF1 pins TF1 (fork/exec substitution): a peer that
// execs /bin/sleep after connecting authenticates as /bin/sleep — a query-time image.
func TestPeerTokenIsQueryTimeLive_TF1(t *testing.T) {
	// Residual platform limitation: LOCAL_PEERTOKEN is query-time-live, so the
	// identity observed at admission can differ from the original connector.
	requireVerifier(t)
	const sleepReq = `identifier "com.apple.sleep" and anchor apple`

	conn := spawnPeerSub(t, "/bin/sleep", "86400")

	// Before the substitution the peer is this (non-Apple) test binary.
	if resolvesTo(t, conn, sleepReq) {
		t.Fatal("baseline: peer resolves to /bin/sleep before the exec")
	}
	// Release the helper; it execs /bin/sleep on the same pid + connection.
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	// The query-time identity flips to /bin/sleep — the substitution succeeds.
	if !eventually(t, 5*time.Second, func() bool { return resolvesTo(t, conn, sleepReq) }) {
		t.Fatal("peer identity never became /bin/sleep after the exec — substitution not observed")
	}
}

// TestPolicyCheckAcceptsSubstitutedPeer_TF1 pins the security consequence: a peer
// that execs a Developer ID fixture after connecting is ACCEPTED by Policy.Check.
func TestPolicyCheckAcceptsSubstitutedPeer_TF1(t *testing.T) {
	// Residual platform limitation: LOCAL_PEERTOKEN identifies the process at
	// query time, so substitution by another process satisfying this exact policy
	// before admission remains accepted.
	requireE2E(t)
	requireVerifier(t)
	fixture := fixtureBin(t, "fixture-devid-a")
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	p := Policy{Requirement: &req}

	conn := spawnPeerSub(t, fixture, "")

	// Baseline: the connector is the untrusted test binary → rejected.
	peer, err := wire.PeerFromConn(conn)
	if err != nil {
		t.Fatalf("PeerFromConn: %v", err)
	}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("baseline Check = %v, want ErrUntrustedPeer (connector is not the fixture)", err)
	}
	// Release the helper; it execs the Developer ID fixture on the same pid.
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	// Policy.Check now ACCEPTS the substituted peer — the documented bypass.
	if !eventually(t, 5*time.Second, func() bool {
		peer, err := wire.PeerFromConn(conn)
		if err != nil {
			return false
		}
		return p.Check(peer) == nil
	}) {
		t.Fatal("Policy.Check never accepted the substituted fixture peer — substitution not observed")
	}
}

// TestPeerTokenPIDReuse_TF2 documents TF2 (PID reuse): a recycled pid lets a later
// LOCAL_PEERTOKEN read resolve an unrelated image. Not pinned — pid reuse is racy.
func TestPeerTokenPIDReuse_TF2(t *testing.T) {
	// Residual platform limitation: query-time lookup cannot bind a recycled pid
	// to the original connector.
	t.Skip("TF2 (PID reuse) is nondeterministic to pin in-tree; documented limitation — see db24393")
}

// TestPeerTokenFDDelegation_TF5 documents TF5 (fd delegation): SCM_RIGHTS fd passing
// or a setuid binary detaches identity from the connection's actor.
func TestPeerTokenFDDelegation_TF5(t *testing.T) {
	// Residual platform limitation: this needs a privileged multi-process harness
	// to pin, and connection admission cannot attribute later fd delegation.
	t.Skip("TF5 (fd delegation / SCM_RIGHTS / setuid) needs a privileged multi-process harness; documented limitation — see db24393")
}
