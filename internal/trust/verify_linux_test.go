package trust

import (
	"errors"
	"net"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/internal/proc"
)

// TestARequirementIsDeniedAndNeverDowngraded is the platform's whole trust
// contract: a same-user peer that would clear the floor is still denied any
// configured requirement, on every entry point that judges one, and the denial
// is the no-verifier class rather than a policy mismatch a caller might retry.
func TestARequirementIsDeniedAndNeverDowngraded(t *testing.T) {
	if CodeIdentity {
		t.Fatal("CodeIdentity = true, want linux to declare it has none")
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	self := Peer{UID: os.Geteuid(), Token: proc.PeerToken(os.Getpid())}
	if err := Verify(self, nil); err != nil {
		t.Fatalf("Verify(same uid, no requirement) = %v, want the floor alone to admit", err)
	}
	denials := map[string]error{
		"Verify":                  Verify(self, &req),
		"VerifyAny":               VerifyAny(self, []Requirement{req}),
		"VerifyAny over two":      VerifyAny(self, []Requirement{req, {TeamID: testTeam, SigningIdentifier: "com.example.other"}}),
		"VerifyProcess":           VerifyProcess(os.Getpid(), req),
		"verifyRequirement":       verifyRequirement(self.Token, req),
		"Verify of a bare floor":  Verify(Peer{UID: os.Geteuid()}, &req),
		"VerifyProcess of pid 1":  VerifyProcess(1, req),
		"verifyRequirement blank": verifyRequirement(proc.AuditToken{}, Requirement{}),
	}
	for name, err := range denials {
		if !errors.Is(err, ErrNoVerifier) {
			t.Errorf("%s = %v, want ErrNoVerifier", name, err)
		}
		if errors.Is(err, ErrUntrustedPeer) {
			t.Errorf("%s = %v, want it never to read as a policy mismatch", name, err)
		}
	}
	if err := Verify(Peer{UID: os.Geteuid() + 1}, &req); !errors.Is(err, ErrUntrustedPeer) {
		t.Errorf("Verify(another uid, requirement) = %v, want the floor to deny first", err)
	}
}

func TestVerifyProcessRejectsAnInvalidRequirementAsAConfigurationError(t *testing.T) {
	err := VerifyProcess(os.Getpid(), Requirement{TeamID: testTeam})
	if err == nil {
		t.Fatal("VerifyProcess with an invalid Requirement = nil, want an error")
	}
	if errors.Is(err, ErrNoVerifier) {
		t.Fatalf("VerifyProcess = %v, want a configuration error rather than the no-verifier denial", err)
	}
}

// TestPeerCredentialsReadTheConnectedProcess reads SO_PEERCRED off a real
// connected pair: both ends of a socket this process connected to itself name
// this process's pid and effective uid, in a token no verifier will accept.
func TestPeerCredentialsReadTheConnectedProcess(t *testing.T) {
	listener, err := net.Listen("unix", t.TempDir()+"/peer.sock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	dialed, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialed.Close() }()
	served, ok := <-accepted
	if !ok {
		t.Fatal("the listener never accepted")
	}
	defer func() { _ = served.Close() }()
	for name, conn := range map[string]net.Conn{"the dialing end": dialed, "the accepting end": served} {
		peer, err := PeerCredentials(conn.(*net.UnixConn))
		if err != nil {
			t.Fatalf("PeerCredentials(%s) = %v", name, err)
		}
		if peer.UID != os.Geteuid() || peer.Token.PID() != os.Getpid() {
			t.Errorf("%s reads uid %d pid %d, want uid %d pid %d", name, peer.UID, peer.Token.PID(), os.Geteuid(), os.Getpid())
		}
		if peer.Token.Valid() {
			t.Errorf("%s token is Valid, want a pid-only token no verifier judges", name)
		}
	}
}

func TestProcessTokenNamesThePIDAlone(t *testing.T) {
	token, err := ProcessToken(4242)
	if err != nil {
		t.Fatalf("ProcessToken() = %v", err)
	}
	if token.PID() != 4242 || token.PIDVersion() != 0 || token.Valid() {
		t.Fatalf("token = pid %d version %d valid %v, want pid 4242 with no execution version", token.PID(), token.PIDVersion(), token.Valid())
	}
}
