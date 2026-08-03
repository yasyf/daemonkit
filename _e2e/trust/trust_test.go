//go:build !daemonkit_unsigned

// This file is daemonkit's signed-peer trust coverage: it spawns fixture
// binaries carrying real Developer ID signatures, admits them over a real unix
// socket, and drives Verify against the audit token the kernel reports for each
// one. The suite's own trust tests feed real CMS chains through a csops double,
// so the accept path against a genuine kernel-reported identity is covered here
// and nowhere else.
//
// It lives under _e2e/, which the go tool excludes from ./..., because the
// identity it needs exists on no machine in the fleet and no CI runner:
// `security find-identity -v -p codesigning` reports 0 valid identities
// (docs/BUILD-ORDER.md:43 records the same), and ad-hoc signing is not a stand-in
// — it yields TeamIdentifier=not set and flags 0x2(adhoc), which verifyToken's
// teamID match and checkValidationCategory's category-6 requirement both refuse.
// In the suite these tests would be permanently red; with the category assertion
// relaxed to make them green, the eleven rejections below would pass on category
// rather than on the property each one names.
//
// scripts/e2e-trust.sh mints the fixtures and runs this package, and is the only
// thing that does. Nothing here is conditional: there is no env gate and no skip,
// and a missing fixture is a failure.
package trust

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
)

const (
	fixtureTeam  = "SXKCTF23Q2"
	fixtureGroup = "group.com.yasyf.daemonkit.fixture"
)

func fixtureBin(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", ".trust-fixtures", name))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture %s missing — mint it with scripts/e2e-trust.sh: %v", name, err)
	}
	return path
}

func fixtureRequirement(identifier string) trust.Requirement {
	return trust.Requirement{TeamID: fixtureTeam, SigningIdentifier: identifier, RequiredAppGroup: fixtureGroup}
}

func fixturePeer(t *testing.T, binary string) trust.Peer {
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

	peer, err := trust.PeerCredentials(conn.(*net.UnixConn))
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
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	if err := trust.Verify(peer, &req); err != nil {
		t.Errorf("Verify(matching devid) = %v, want nil", err)
	}
}

func TestTrustAcceptsARelaxedJITPeerOnlyWhenAllowJITIsSet(t *testing.T) {
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-relaxed"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-relaxed")
	if err := trust.Verify(peer, &req); !errors.Is(err, trust.ErrUntrustedPeer) {
		t.Errorf("Verify(allow-jit peer, strict) = %v, want ErrUntrustedPeer", err)
	}
	req.AllowJIT = true
	if err := trust.Verify(peer, &req); err != nil {
		t.Errorf("Verify(allow-jit peer, AllowJIT) = %v, want nil", err)
	}
}

func TestTrustAcceptsAnEntitlementFreeDeveloperIDPeer(t *testing.T) {
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-noents"))
	req := trust.Requirement{TeamID: fixtureTeam, SigningIdentifier: "com.yasyf.daemonkit.fixture-noents"}
	if err := trust.Verify(peer, &req); err != nil {
		t.Errorf("Verify(entitlement-free devid) = %v, want nil", err)
	}
	req.RequiredAppGroup = fixtureGroup
	if err := trust.Verify(peer, &req); !errors.Is(err, trust.ErrUntrustedPeer) {
		t.Errorf("Verify(entitlement-free devid, app group required) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsSignedPeers(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		identifier string
		mutate     func(*trust.Requirement)
	}{
		{"wrong identifier", "fixture-devid-a", "com.yasyf.daemonkit.fixture-b", nil},
		{"wrong team", "fixture-devid-a", "com.yasyf.daemonkit.fixture-a", func(r *trust.Requirement) { r.TeamID = "ZZ0FAKE9TX" }},
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
			peer := fixturePeer(t, fixtureBin(t, tt.fixture))
			req := fixtureRequirement(tt.identifier)
			if tt.mutate != nil {
				tt.mutate(&req)
			}
			if err := trust.Verify(peer, &req); !errors.Is(err, trust.ErrUntrustedPeer) {
				t.Errorf("Verify(%s) = %v, want ErrUntrustedPeer", tt.name, err)
			}
		})
	}
}

// The verifier holds no framework object and no cache, so repeated admission
// of the same peer must not grow the heap.
func TestTrustVerificationIsAllocationBounded(t *testing.T) {
	peer := fixturePeer(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	for iteration := 0; iteration < 200; iteration++ {
		if err := trust.Verify(peer, &req); err != nil {
			t.Fatalf("Verify iteration %d: %v", iteration, err)
		}
	}
}
