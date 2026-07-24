package wire

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func roleTestPolicy(t *testing.T, allowUnprotected bool) trust.TrustPolicy {
	t.Helper()
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: allowUnprotected,
		Roles: map[trust.PeerRole]trust.Requirement{
			"stop":      {TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.stop"},
			"lifecycle": {TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.lifecycle"},
		},
		StopRoles: []trust.PeerRole{"stop"}, ReceiptRoles: []trust.PeerRole{"lifecycle"},
		ReadinessRoles: []trust.PeerRole{"lifecycle"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestClientConfigRequiresExplicitRole(t *testing.T) {
	_, err := validateClientConfig(ClientConfig{Dial: func(_ context.Context) (net.Conn, error) {
		return nil, errors.New("not reached")
	}, WireBuild: "suite.v1"})
	if err == nil || err.Error() != "wire: Role is required" {
		t.Fatalf("validateClientConfig = %v", err)
	}
}

func TestReadClientHelloCarriesExactRole(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	written := make(chan error, 1)
	go func() {
		payload, err := json.Marshal(handshakeIdentity{
			Protocol: ProtocolVersion, WireBuild: "suite.v1", Role: "readiness-controller.v1",
		})
		if err == nil {
			err = NewCodec(clientConn).WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload})
		}
		written <- err
	}()
	identity, err := (&Server{WireBuild: "suite.v1"}).readClientHello(NewCodec(serverConn))
	if err != nil {
		t.Fatal(err)
	}
	if identity.Role != "readiness-controller.v1" {
		t.Fatalf("role = %q", identity.Role)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestReadClientHelloRejectsMissingRole(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	go func() {
		payload := []byte(`{"protocol":1,"wire_build":"suite.v1"}`)
		_ = NewCodec(clientConn).WriteFrame(Frame{Kind: FrameHello, Flags: FlagEnd, Payload: payload})
	}()
	if _, err := (&Server{WireBuild: "suite.v1"}).readClientHello(NewCodec(serverConn)); err == nil {
		t.Fatal("hello without a role succeeded")
	}
}

func TestVerifyPeerAcceptsOnlyPolicySelectedRole(t *testing.T) {
	peer := Peer{UID: os.Geteuid()}
	allowed := &Server{trustPolicy: roleTestPolicy(t, true)}
	role, protected, err := allowed.verifyPeer(t.Context(), peer, trust.UnprotectedRole)
	if err != nil {
		t.Fatal(err)
	}
	if role != trust.UnprotectedRole || protected {
		t.Fatalf("selected role = %q protected=%t", role, protected)
	}
	if _, _, err := allowed.verifyPeer(t.Context(), peer, "consumer.unprotected"); err == nil {
		t.Fatal("consumer-invented unprotected role succeeded")
	}
	denied := &Server{trustPolicy: roleTestPolicy(t, false)}
	if _, _, err := denied.verifyPeer(t.Context(), peer, trust.UnprotectedRole); err == nil {
		t.Fatal("disabled unprotected role succeeded")
	}
}

func TestVerifyPeerAcceptsExplicitUnprotectedOnlyPolicy(t *testing.T) {
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(), AllowUnprotected: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{trustPolicy: policy}
	role, protected, err := server.verifyPeer(t.Context(), Peer{UID: os.Geteuid()}, trust.UnprotectedRole)
	if err != nil {
		t.Fatal(err)
	}
	if role != trust.UnprotectedRole || protected {
		t.Fatalf("selected role = %q protected=%t", role, protected)
	}
}
