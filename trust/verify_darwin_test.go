//go:build darwin && !daemonkit_unsigned

package trust

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"golang.org/x/sys/unix"
)

const fixtureTeam = "SXKCTF23Q2"

func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("DAEMONKIT_TRUST_E2E") != "1" {
		t.Skip("set DAEMONKIT_TRUST_E2E=1 (and build the .trust-fixtures via scripts/trust-fixtures.sh) to run the signed-peer trust E2E")
	}
}

func fixtureBin(t *testing.T, name string) string {
	t.Helper()
	p, err := filepath.Abs(filepath.Join("..", ".trust-fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err != nil {
		t.Skipf("fixture %s missing (run scripts/trust-fixtures.sh .trust-fixtures): %v", name, err)
	}
	return p
}

// peerOf spawns the signed fixture, dials back over a short-path unix socket, and
// returns its OS-read wire.Peer. The returned release unblocks and reaps the
// fixture; the peer stays alive until then so its SecCode resolves.
func peerOf(t *testing.T, bin string) (wire.Peer, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-tr")
	if err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cmd := exec.Command(bin, sock)
	if err := cmd.Start(); err != nil {
		ln.Close()
		os.RemoveAll(dir)
		t.Fatalf("start fixture: %v", err)
	}

	type accepted struct {
		conn net.Conn
		err  error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()

	var conn net.Conn
	select {
	case a := <-ch:
		if a.err != nil {
			t.Fatalf("accept: %v", a.err)
		}
		conn = a.conn
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("fixture never connected")
	}

	peer, err := wire.PeerFromConn(conn.(*net.UnixConn))
	if err != nil {
		conn.Close()
		_ = cmd.Process.Kill()
		t.Fatalf("PeerFromConn: %v", err)
	}
	if peer.PID != cmd.Process.Pid {
		t.Fatalf("peer PID %d != fixture PID %d (audit token resolved the wrong process)", peer.PID, cmd.Process.Pid)
	}
	release := func() {
		conn.Close()
		ln.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		os.RemoveAll(dir)
	}
	return peer, release
}

func TestTrustAcceptsMatchingDeveloperID(t *testing.T) {
	requireE2E(t)
	peer, release := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	defer release()
	p := Policy{Requirement: &Requirement{TeamID: fixtureTeam, Identifier: "com.yasyf.daemonkit.fixture-a"}}
	if err := p.Check(peer); err != nil {
		t.Errorf("Check(matching devid) = %v, want nil", err)
	}
}

func TestTrustRejectsWrongIdentifier(t *testing.T) {
	requireE2E(t)
	peer, release := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	defer release()
	p := Policy{Requirement: &Requirement{TeamID: fixtureTeam, Identifier: "com.yasyf.daemonkit.fixture-b"}}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(wrong identifier) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsAdHoc(t *testing.T) {
	requireE2E(t)
	peer, release := peerOf(t, fixtureBin(t, "fixture-adhoc"))
	defer release()
	p := Policy{Requirement: &Requirement{TeamID: fixtureTeam, Identifier: "com.yasyf.daemonkit.fixture-adhoc"}}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(ad-hoc) = %v, want ErrUntrustedPeer (no Developer ID anchor)", err)
	}
}

func TestTrustRejectsUnhardened(t *testing.T) {
	requireE2E(t)
	peer, release := peerOf(t, fixtureBin(t, "fixture-devid-unhardened"))
	defer release()
	req := Requirement{TeamID: fixtureTeam, Identifier: "com.yasyf.daemonkit.fixture-unhardened"}
	if err := (Policy{Requirement: &req}).Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(unhardened) = %v, want ErrUntrustedPeer (lacks CS_RUNTIME)", err)
	}
	req.AllowUnhardened = true
	if err := (Policy{Requirement: &req}).Check(peer); err != nil {
		t.Errorf("Check(unhardened, AllowUnhardened) = %v, want nil", err)
	}
}

// TestTrustVerificationIsLeakFree runs many validations against one live peer and
// asserts RSS stays bounded — the CFRelease discipline is load-bearing (a missing
// release leaks ~66 KB per call; 4000 calls would add ~250 MB).
func TestTrustVerificationIsLeakFree(t *testing.T) {
	requireE2E(t)
	peer, release := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	defer release()
	p := Policy{Requirement: &Requirement{TeamID: fixtureTeam, Identifier: "com.yasyf.daemonkit.fixture-a"}}

	for i := 0; i < 200; i++ { // warm up caches and one-time loads
		_ = p.Check(peer)
	}
	before := maxRSS(t)
	for i := 0; i < 4000; i++ {
		if err := p.Check(peer); err != nil {
			t.Fatalf("Check iteration %d: %v", i, err)
		}
	}
	grew := maxRSS(t) - before
	if grew > 40*1024*1024 {
		t.Errorf("RSS grew %d bytes over 4000 validations — a CFRelease leak", grew)
	}
}

func maxRSS(t *testing.T) int64 {
	t.Helper()
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		t.Fatalf("getrusage: %v", err)
	}
	return int64(ru.Maxrss) // darwin reports bytes
}
