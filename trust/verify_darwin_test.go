//go:build darwin && !daemonkit_unsigned

package trust

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/csposture/csposturetest"
	"github.com/yasyf/daemonkit/peer"
	"golang.org/x/sys/unix"
)

const (
	fixtureTeam  = "SXKCTF23Q2"
	fixtureGroup = "group.com.yasyf.daemonkit.fixture"
)

func fixtureRequirement(identifier string) Requirement {
	return Requirement{
		TeamID: fixtureTeam, SigningIdentifier: identifier,
		RequiredAppGroup: fixtureGroup,
	}
}

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

func peerOf(t *testing.T, bin string) peer.Identity {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "dk-tr")
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
	cmd := exec.Command(bin, sock)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fixture: %v", err)
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

	var conn net.Conn
	select {
	case a := <-ch:
		if a.err != nil {
			t.Fatalf("accept: %v", a.err)
		}
		conn = a.conn
	case <-time.After(5 * time.Second):
		t.Fatal("fixture never connected")
	}
	t.Cleanup(func() { _ = conn.Close() })

	peer, err := peer.FromConn(conn.(*net.UnixConn))
	if err != nil {
		t.Fatalf("PeerFromConn: %v", err)
	}
	if peer.PID != cmd.Process.Pid {
		t.Fatalf("peer PID %d != fixture PID %d (audit token resolved the wrong process)", peer.PID, cmd.Process.Pid)
	}
	return peer
}

func TestTrustAcceptsMatchingDeveloperID(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	p := Policy{Requirement: &req}
	if err := p.Check(peer); err != nil {
		t.Errorf("Check(matching devid) = %v, want nil", err)
	}
}

func TestTrustRejectsWrongIdentifier(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-b")
	p := Policy{Requirement: &req}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(wrong identifier) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsWrongTeam(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	req.TeamID = "ZZ0FAKE9TX"
	p := Policy{Requirement: &req}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(wrong team) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsAdHoc(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-adhoc"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-adhoc")
	p := Policy{Requirement: &req}
	if err := p.Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(ad-hoc) = %v, want ErrUntrustedPeer (no Developer ID anchor)", err)
	}
}

func TestTrustRejectsWrongAppGroup(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-wronggroup"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-wronggroup")
	if err := (Policy{Requirement: &req}).Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(wrong app group) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsUnhardened(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-unhardened"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-unhardened")
	if err := (Policy{Requirement: &req}).Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(unhardened) = %v, want ErrUntrustedPeer (lacks CS_RUNTIME)", err)
	}
}

func TestTrustRejectsDisabledLibraryValidation(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-nolv"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-nolv")
	if err := (Policy{Requirement: &req}).Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(disable-library-validation) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustRejectsGetTaskAllow(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-gta"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-gta")
	if err := (Policy{Requirement: &req}).Check(peer); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Check(get-task-allow) = %v, want ErrUntrustedPeer", err)
	}
}

func TestTrustVerificationIsLeakFree(t *testing.T) {
	requireE2E(t)
	peer := peerOf(t, fixtureBin(t, "fixture-devid-a"))
	req := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	p := Policy{Requirement: &req}

	for i := 0; i < 200; i++ {
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
	return int64(ru.Maxrss)
}

func TestCodeStatusMatchesSharedPostureUnderEntitlementLibraryValidation(t *testing.T) {
	for _, tt := range csposturetest.Cases() {
		t.Run(tt.Name, func(t *testing.T) {
			err := checkCodeStatus(tt.Status)
			if !tt.EntitlementLVDenies {
				if err != nil {
					t.Fatalf("checkCodeStatus(0x%x) = %v, want nil", tt.Status, err)
				}
				return
			}
			if !errors.Is(err, ErrUntrustedPeer) {
				t.Fatalf("checkCodeStatus(0x%x) = %v, want ErrUntrustedPeer", tt.Status, err)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("status 0x%x", tt.Status)) {
				t.Errorf("checkCodeStatus(0x%x) message %q lacks the status word", tt.Status, err)
			}
		})
	}
}

func TestGuestLookupError(t *testing.T) {
	tests := []struct {
		name    string
		status  int32
		want    error
		notWant error
	}{
		{"success", errSecSuccess, nil, nil},
		{"departed peer", osStatusPeerGone, ErrPeerGone, ErrNoVerifier},
		{"posix non-esrch", 100002, ErrNoVerifier, ErrPeerGone},
		{"csreq failure", -67062, ErrNoVerifier, ErrPeerGone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := guestLookupError(tt.status)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("guestLookupError(%d) = %v, want nil", tt.status, err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("guestLookupError(%d) = %v, want %v", tt.status, err, tt.want)
			}
			if errors.Is(err, tt.notWant) {
				t.Errorf("guestLookupError(%d) = %v, must not match %v", tt.status, err, tt.notWant)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("OSStatus %d", tt.status)) {
				t.Errorf("guestLookupError(%d) message %q lacks the OSStatus", tt.status, err)
			}
		})
	}
}

func TestValidityAndSigningInfoErrorsClassifyDeadPeer(t *testing.T) {
	sites := []struct {
		name       string
		classify   func(int32) error
		failClosed error
	}{
		{"check validity", validityError, ErrUntrustedPeer},
		{"signing information", signingInfoError, ErrNoVerifier},
	}
	tests := []struct {
		name   string
		status int32
		want   func(failClosed error) error
	}{
		{"success", errSecSuccess, func(error) error { return nil }},
		{"departed peer", osStatusPeerGone, func(error) error { return ErrPeerGone }},
		{"no such guest", osStatusNoSuchCode, func(error) error { return ErrPeerGone }},
		{"posix non-esrch", 100002, func(failClosed error) error { return failClosed }},
		{"csreq failure", -67062, func(failClosed error) error { return failClosed }},
	}
	for _, site := range sites {
		for _, tt := range tests {
			t.Run(site.name+"/"+tt.name, func(t *testing.T) {
				err := site.classify(tt.status)
				want := tt.want(site.failClosed)
				if want == nil {
					if err != nil {
						t.Fatalf("classify(%d) = %v, want nil", tt.status, err)
					}
					return
				}
				if !errors.Is(err, want) {
					t.Errorf("classify(%d) = %v, want %v", tt.status, err, want)
				}
				for _, sentinel := range []error{ErrPeerGone, ErrNoVerifier, ErrUntrustedPeer} {
					if sentinel != want && errors.Is(err, sentinel) {
						t.Errorf("classify(%d) = %v, must not match %v", tt.status, err, sentinel)
					}
				}
				if !strings.Contains(err.Error(), fmt.Sprintf("OSStatus %d", tt.status)) {
					t.Errorf("classify(%d) message %q lacks the OSStatus", tt.status, err)
				}
			})
		}
	}
}

func TestRunVerifierChildReportsPeerGoneResult(t *testing.T) {
	secOnce.Do(loadSecurity)
	if secErr != nil {
		t.Fatalf("load Security framework: %v", secErr)
	}
	original := secCodeCopyGuestWithAttributes
	secCodeCopyGuestWithAttributes = func(_, _ uintptr, _ uint32, _ *uintptr) int32 {
		return osStatusPeerGone
	}
	t.Cleanup(func() { secCodeCopyGuestWithAttributes = original })

	requirement := fixtureRequirement("com.yasyf.daemonkit.fixture-a")
	payload, err := json.Marshal(verifierRequest{
		Protocol:    verifierProtocol,
		Peer:        peer.Identity{UID: os.Geteuid(), Audit: make([]byte, 32)},
		Requirement: &requirement,
	})
	if err != nil {
		t.Fatalf("encode verifier request: %v", err)
	}
	var stdout bytes.Buffer
	recognized, err := RunVerifierChild(
		[]string{verifierChildMode, base64.RawURLEncoding.EncodeToString(payload)},
		&stdout,
	)
	if !recognized || err != nil {
		t.Fatalf("RunVerifierChild = %t, %v, want recognized with no error", recognized, err)
	}
	var response verifierResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		t.Fatalf("decode verifier child response %q: %v", stdout.String(), err)
	}
	if response.Protocol != verifierProtocol {
		t.Errorf("verifier child protocol = %d, want %d", response.Protocol, verifierProtocol)
	}
	if response.Result != verifierResultPeerGone {
		t.Errorf("verifier child result = %q, want %q", response.Result, verifierResultPeerGone)
	}
	if !strings.Contains(response.Error, fmt.Sprintf("OSStatus %d", osStatusPeerGone)) {
		t.Errorf("verifier child error %q lacks the OSStatus", response.Error)
	}
}
