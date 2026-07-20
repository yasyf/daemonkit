package codeidentity

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

type acceptedFixture struct {
	peer   wire.Peer
	code   CodeIdentity
	digest PolicyDigest
}

func (i acceptedFixture) Peer() wire.Peer            { return i.peer }
func (i acceptedFixture) CodeIdentity() CodeIdentity { return i.code }
func (i acceptedFixture) PolicyDigest() PolicyDigest { return i.digest }

type identityAcceptorFunc func(context.Context, wire.Peer) (AcceptedIdentity, error)

func (f identityAcceptorFunc) Accept(ctx context.Context, peer wire.Peer) (AcceptedIdentity, error) {
	return f(ctx, peer)
}

func TestFixedClassifierDeniesOrdinaryAndAllowsSameOrNewerDaemon(t *testing.T) {
	classifier := FixedClassifier{Executable: "/opt/example/bin/daemon"}
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
	if !classifier.AuthorizeLifecycleBuild("v1.2.0", "v1.2.0") ||
		!classifier.AuthorizeLifecycleBuild("v1.2.0", "v1.3.0") ||
		classifier.AuthorizeLifecycleBuild("v1.2.0", "v1.1.0") {
		t.Fatal("same/newer protected build relationship is not exact")
	}
}

func TestFixedClassifierRejectsCodeDigestAndAuditSubstitution(t *testing.T) {
	peer := wire.Peer{
		PID: 42, UID: os.Geteuid(), StartTime: "start", Boot: "boot",
		Executable: "/Applications/Example.app/Contents/MacOS/Example",
		Audit:      []byte("accepted-audit"),
	}
	code := CodeIdentity{TeamID: "ABCDE12345", SigningIdentifier: "com.example.product"}
	digest := PolicyDigest{1}
	accepted := acceptedFixture{peer: peer, code: code, digest: digest}
	classifier := FixedClassifier{
		Executable: peer.Executable, CodeIdentity: code, PolicyDigest: digest,
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
		mutate func(*acceptedFixture)
	}{
		{"audit", func(identity *acceptedFixture) { identity.peer.Audit = []byte("substituted-audit") }},
		{"code", func(identity *acceptedFixture) { identity.code.TeamID = "FGHIJ67890" }},
		{"digest", func(identity *acceptedFixture) { identity.digest[0]++ }},
		{"executable", func(identity *acceptedFixture) { identity.peer.Executable += ".other" }},
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
