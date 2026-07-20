package trust

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

type identityAcceptorFunc func(context.Context, wire.Peer) (AcceptedIdentity, error)

func (f identityAcceptorFunc) Accept(ctx context.Context, peer wire.Peer) (AcceptedIdentity, error) {
	return f(ctx, peer)
}

func TestSessionClassifierDeniesOrdinaryAndAllowsSameOrNewerDaemon(t *testing.T) {
	classifier := SessionClassifier{Executable: "/opt/example/bin/daemon"}
	ordinary, err := classifier.Classify(t.Context(), wire.Peer{
		PID: 42, UID: os.Geteuid(), StartTime: "start", Boot: "boot",
		Executable: "/opt/example/bin/client",
	})
	if err != nil || ordinary {
		t.Fatalf("ordinary classification = %t, %v", ordinary, err)
	}
	protected, err := classifier.Classify(t.Context(), wire.Peer{
		PID: 42, UID: os.Geteuid(), StartTime: "start", Boot: "boot",
		Executable: "/opt/example/bin/daemon",
	})
	if err != nil || !protected {
		t.Fatalf("daemon classification = %t, %v", protected, err)
	}
	if !classifier.AuthorizeBuild("v1.2.0", "v1.2.0") ||
		!classifier.AuthorizeBuild("v1.2.0", "v1.3.0") ||
		classifier.AuthorizeBuild("v1.2.0", "v1.1.0") {
		t.Fatal("same/newer protected build relationship is not exact")
	}
}

func TestSignedSessionClassifierRejectsCodeDigestAndAuditSubstitution(t *testing.T) {
	peer := wire.Peer{
		PID: 42, UID: os.Geteuid(), StartTime: "start", Boot: "boot",
		Executable: "/Applications/Example.app/Contents/MacOS/Example",
		Audit:      []byte("accepted-audit"),
	}
	code := CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.product"}
	digest := [32]byte{1}
	accepted := AcceptedIdentity{
		peer: peer, code: code, entitlementPolicyDigest: digest,
	}
	classifier := SessionClassifier{
		Executable: peer.Executable, CodeIdentity: code,
		EntitlementPolicyDigest: digest,
		Acceptor: identityAcceptorFunc(func(context.Context, wire.Peer) (AcceptedIdentity, error) {
			return accepted, nil
		}),
	}
	protected, err := classifier.Classify(t.Context(), peer)
	if err != nil || !protected {
		t.Fatalf("accepted signed peer = %t, %v", protected, err)
	}
	tests := []struct {
		name   string
		mutate func(*AcceptedIdentity)
	}{
		{"audit", func(identity *AcceptedIdentity) { identity.peer.Audit = []byte("substituted-audit") }},
		{"code", func(identity *AcceptedIdentity) { identity.code.TeamID = "FGHIJ67890" }},
		{"digest", func(identity *AcceptedIdentity) { identity.entitlementPolicyDigest[0]++ }},
		{"executable", func(identity *AcceptedIdentity) { identity.peer.Executable += ".other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := accepted
			candidate.peer.Audit = append([]byte(nil), accepted.peer.Audit...)
			test.mutate(&candidate)
			classifier.Acceptor = identityAcceptorFunc(func(context.Context, wire.Peer) (AcceptedIdentity, error) {
				return candidate, nil
			})
			protected, err := classifier.Classify(t.Context(), peer)
			if protected || !errors.Is(err, ErrUntrustedPeer) {
				t.Fatalf("substituted signed peer = %t, %v", protected, err)
			}
		})
	}
}
