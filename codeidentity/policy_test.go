package codeidentity

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestCodePolicyRejectsForeignUIDBeforeCodeVerification(t *testing.T) {
	policy := CodePolicy{Identity: CodeIdentity{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit",
	}}
	err := policy.Check(wire.Peer{UID: os.Geteuid() + 1})
	if !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("Check() = %v, want ErrUntrustedPeer", err)
	}
}

func TestCodePolicyRejectsInvalidIdentityBeforeCodeVerification(t *testing.T) {
	for _, identity := range []CodeIdentity{
		{SigningIdentifier: "com.example.daemonkit"},
		{TeamID: "ABCDE12345"},
		{TeamID: `BAD"TEAM`, SigningIdentifier: "com.example.daemonkit"},
	} {
		err := (CodePolicy{Identity: identity}).Check(wire.Peer{UID: os.Geteuid()})
		if err == nil || errors.Is(err, ErrNoVerifier) {
			t.Fatalf("Check(%+v) = %v, want identity validation error", identity, err)
		}
	}
}

func TestCodePolicyFailsClosedWithoutUsableAuditToken(t *testing.T) {
	policy := CodePolicy{Identity: CodeIdentity{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit",
	}}
	err := policy.Check(wire.Peer{UID: os.Geteuid()})
	if !errors.Is(err, ErrNoVerifier) {
		t.Fatalf("Check() = %v, want ErrNoVerifier", err)
	}
}
