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

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
)

const (
	peerSubSockEnv = "DAEMONKIT_PEERSUB_SOCK"
	peerSubExecEnv = "DAEMONKIT_PEERSUB_EXEC"
	peerSubArgsEnv = "DAEMONKIT_PEERSUB_ARGS"
)

func TestMain(m *testing.M) {
	if os.Getenv(peerSubSockEnv) != "" {
		runPeerSub()
		os.Exit(70)
	}
	os.Exit(m.Run())
}

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
	if err := raw.Control(func(fd uintptr) { heldFD, dupErr = syscall.Dup(int(fd)) }); err != nil || dupErr != nil {
		os.Exit(74)
	}
	_ = heldFD
	target := os.Getenv(peerSubExecEnv)
	argv := append([]string{target}, strings.Fields(os.Getenv(peerSubArgsEnv))...)
	_ = syscall.Exec(target, argv, os.Environ())
	os.Exit(75)
}

func spawnPeerSub(t *testing.T, execTarget, execArgs string) *net.UnixConn {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-sub")
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

func requireVerifier(t *testing.T) {
	t.Helper()
	secOnce.Do(loadSecurity)
	if secErr != nil {
		t.Skipf("Security.framework verifier unavailable: %v", secErr)
	}
}

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

func TestPeerTokenIsQueryTimeLive_TF1(t *testing.T) {
	requireVerifier(t)
	const sleepReq = `identifier "com.apple.sleep" and anchor apple`

	conn := spawnPeerSub(t, "/bin/sleep", "86400")

	if resolvesTo(t, conn, sleepReq) {
		t.Fatal("baseline: peer resolves to /bin/sleep before the exec")
	}
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	if !eventually(t, 5*time.Second, func() bool { return resolvesTo(t, conn, sleepReq) }) {
		t.Fatal("peer identity never became /bin/sleep after the exec — substitution not observed")
	}
}

func TestPolicyCheckAcceptsSubstitutedPeer_TF1(t *testing.T) {
	requireE2E(t)
	requireVerifier(t)
	fixture := fixtureBin(t, "fixture-devid-a")
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	p := Policy{Requirement: &req}

	conn := spawnPeerSub(t, fixture, "")

	peer, err := wire.PeerFromConn(conn)
	if err != nil {
		t.Fatalf("PeerFromConn: %v", err)
	}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("baseline Check = %v, want ErrUntrustedPeer (connector is not the fixture)", err)
	}
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
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

func TestPeerTokenPIDReuse_TF2(t *testing.T) {
	conn := spawnPeerSub(t, "/bin/sleep", "86400")
	peer, err := wire.PeerFromConn(conn)
	if err != nil {
		t.Fatalf("PeerFromConn: %v", err)
	}
	token, err := proc.AuditTokenFromBytes(peer.Audit)
	if err != nil {
		t.Fatal(err)
	}
	recycled := token
	recycled[28] ^= 1
	if _, err := proc.BindAuditTokenIdentity(recycled, peer.PID); err == nil {
		t.Fatal("audit-token binding accepted a different pidversion for the live PID")
	}
}

func TestPeerTokenFDDelegation_TF5(t *testing.T) {
	t.Skip("TF5 (fd delegation / SCM_RIGHTS / setuid) needs a privileged multi-process harness; documented limitation — see db24393")
}
