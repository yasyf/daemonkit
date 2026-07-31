//go:build !daemonkit_unsigned

package trust

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const fixtureGroup = "group.com.yasyf.daemonkit.fixture"

func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("DAEMONKIT_TRUST_E2E") != "1" {
		t.Skip("set DAEMONKIT_TRUST_E2E=1 (and build the .trust-fixtures via scripts/trust-fixtures.sh) to run the signed-peer trust E2E")
	}
}

func fixtureBin(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", ".trust-fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("fixture %s missing (run scripts/trust-fixtures.sh .trust-fixtures): %v", name, err)
	}
	return path
}

func fixtureRequirement(identifier string) Requirement {
	return Requirement{TeamID: testTeam, SigningIdentifier: identifier, RequiredAppGroup: fixtureGroup}
}

func fixturePeer(t *testing.T, binary string) Peer {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-tr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	command := exec.Command(binary, sock)
	if err := command.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	type accepted struct {
		conn net.Conn
		err  error
	}
	results := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		results <- accepted{conn, err}
	}()
	var conn net.Conn
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("accept: %v", result.err)
		}
		conn = result.conn
	case <-time.After(5 * time.Second):
		t.Fatal("fixture never connected")
	}
	t.Cleanup(func() { _ = conn.Close() })

	peer, err := PeerCredentials(conn.(*net.UnixConn))
	if err != nil {
		t.Fatalf("PeerCredentials: %v", err)
	}
	if peer.Token.PID() != command.Process.Pid {
		t.Fatalf("peer PID %d != fixture PID %d (the audit token resolved the wrong process)",
			peer.Token.PID(), command.Process.Pid)
	}
	return peer
}

func TestTrustAcceptsMatchingDeveloperID(t *testing.T) {
	requireE2E(t)
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	if err := Verify(peer, &req); err != nil {
		t.Errorf("Verify(matching devid) = %v, want nil", err)
	}
}

func TestTrustAcceptsARelaxedJITPeerOnlyWhenAllowJITIsSet(t *testing.T) {
	requireE2E(t)
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-relaxed"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-relaxed")
	if err := Verify(peer, &req); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Verify(allow-jit peer, strict) = %v, want ErrUntrustedPeer", err)
	}
	req.AllowJIT = true
	if err := Verify(peer, &req); err != nil {
		t.Errorf("Verify(allow-jit peer, AllowJIT) = %v, want nil", err)
	}
}

func TestTrustAcceptsAnEntitlementFreeDeveloperIDPeer(t *testing.T) {
	requireE2E(t)
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-noents"))
	req := Requirement{TeamID: testTeam, SigningIdentifier: "com.yasyf.daemonkit.fixture-noents"}
	if err := Verify(peer, &req); err != nil {
		t.Errorf("Verify(entitlement-free devid) = %v, want nil", err)
	}
	req.RequiredAppGroup = fixtureGroup
	if err := Verify(peer, &req); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Verify(entitlement-free devid, app group required) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsSignedPeers(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		identifier string
		mutate     func(*Requirement)
	}{
		{"wrong identifier", "fixture-devid-a", "com.yasyf.daemonkit.fixture-b", nil},
		{"wrong team", "fixture-devid-a", "com.yasyf.daemonkit.fixture-a", func(r *Requirement) { r.TeamID = "ZZ0FAKE9TX" }},
		{"ad-hoc", "fixture-adhoc", "com.yasyf.daemonkit.fixture-adhoc", nil},
		{"wrong app group", "fixture-devid-wronggroup", "com.yasyf.daemonkit.fixture-wronggroup", nil},
		{"unhardened", "fixture-devid-unhardened", "com.yasyf.daemonkit.fixture-unhardened", nil},
		{"library validation disabled", "fixture-devid-nolv", "com.yasyf.daemonkit.fixture-nolv", nil},
		{"get-task-allow", "fixture-devid-gta", "com.yasyf.daemonkit.fixture-gta", nil},
		{"allow-jit", "fixture-devid-jit", "com.yasyf.daemonkit.fixture-jit", nil},
		{"allow-dyld-environment-variables", "fixture-devid-dyldenv", "com.yasyf.daemonkit.fixture-dyldenv", nil},
		{"allow-unsigned-executable-memory", "fixture-devid-unsignedmem", "com.yasyf.daemonkit.fixture-unsignedmem", nil},
		{"disable-executable-page-protection", "fixture-devid-nopageprot", "com.yasyf.daemonkit.fixture-nopageprot", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireE2E(t)
			peer := fixturePeer(t, fixtureBin(t, tt.fixture))
			req := fixtureRequirement(tt.identifier)
			if tt.mutate != nil {
				tt.mutate(&req)
			}
			if err := Verify(peer, &req); !errors.Is(err, ErrUntrustedPeer) {
				t.Errorf("Verify(%s) = %v, want ErrUntrustedPeer", tt.name, err)
			}
		})
	}
}

// The verifier holds no framework object and no cache, so repeated admission
// of the same peer must not grow the heap.
func TestTrustVerificationIsAllocationBounded(t *testing.T) {
	requireE2E(t)
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	for iteration := 0; iteration < 200; iteration++ {
		if err := Verify(peer, &req); err != nil {
			t.Fatalf("Verify iteration %d: %v", iteration, err)
		}
	}
}
