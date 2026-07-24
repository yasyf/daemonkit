package wire

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
)

func TestBrokerHandoffCanonicalEnvelopeMatchesSwift(t *testing.T) {
	payload := []byte(`{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":1,"runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"}}`)
	envelope, err := decodeBrokerHandoff(payload)
	if err != nil {
		t.Fatalf("decode canonical envelope: %v", err)
	}
	if envelope.RuntimeIdentity.ProcessGeneration != (proc.OwnerGeneration{1}) ||
		envelope.RuntimeIdentity.RuntimeBuild != "app.v1" {
		t.Fatalf("runtime identity = %+v", envelope.RuntimeIdentity)
	}
	encoded, err := marshalBrokerHandoff(envelope)
	if err != nil {
		t.Fatalf("encode canonical envelope: %v", err)
	}
	if string(encoded) != string(payload) {
		t.Fatalf("encoded envelope = %s, want %s", encoded, payload)
	}
}

func TestBrokerHandoffRejectsNonCanonicalAndForeignEnvelopes(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"protocol":1,"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"}}`),
		[]byte(`{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":1,"runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"},"unknown":true}`),
		[]byte(`{"nonce":"not-base64","protocol":1,"runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"}}`),
		[]byte(`{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":2,"runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"}}`),
	}
	for _, payload := range tests {
		if _, err := decodeBrokerHandoff(payload); err == nil {
			t.Fatalf("decodeBrokerHandoff(%s) unexpectedly succeeded", payload)
		}
	}
}

func TestBrokerHandoffSessionLimitsAndReplay(t *testing.T) {
	sess := &session{}
	reservations := make([]*brokerHandoffReservation, 0, brokerHandoffMaximumPending)
	for i := range brokerHandoffMaximumPending {
		nonce := make([]byte, brokerHandoffNonceBytes)
		nonce[0] = byte(i + 1)
		reservation, err := sess.reserveBrokerHandoff(nonce)
		if err != nil {
			t.Fatalf("reserve pending %d: %v", i, err)
		}
		reservations = append(reservations, reservation)
	}
	overflow := make([]byte, brokerHandoffNonceBytes)
	overflow[0] = 10
	if _, err := sess.reserveBrokerHandoff(overflow); !errors.Is(err, ErrHandoffPendingCapacity) {
		t.Fatalf("pending overflow = %v, want ErrHandoffPendingCapacity", err)
	}
	reservations[0].finish()
	replay := make([]byte, brokerHandoffNonceBytes)
	replay[0] = 1
	if _, err := sess.reserveBrokerHandoff(replay); !errors.Is(err, ErrHandoffReplay) {
		t.Fatalf("nonce replay = %v, want ErrHandoffReplay", err)
	}
	for _, reservation := range reservations[1:] {
		reservation.finish()
	}
	sess.handoffMu.Lock()
	sess.handoffAttempts = brokerHandoffMaximumAttempts
	sess.handoffMu.Unlock()
	if _, err := sess.reserveBrokerHandoff(overflow); !errors.Is(err, ErrHandoffSessionExhausted) {
		t.Fatalf("session exhaustion = %v, want ErrHandoffSessionExhausted", err)
	}
}

func TestBrokerHandoffRequiresDedicatedRoleAndExactRuntimeIdentity(t *testing.T) {
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
		Roles: map[trust.PeerRole]trust.Requirement{
			"stop":      {TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.stop"},
			"lifecycle": {TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.lifecycle"},
			"handoff":   {TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.handoff"},
		},
		StopRoles:      []trust.PeerRole{"stop"},
		ReceiptRoles:   []trust.PeerRole{"lifecycle"},
		ReadinessRoles: []trust.PeerRole{"lifecycle"},
		HandoffRoles:   []trust.PeerRole{"handoff"},
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := proc.OwnerGeneration{1}
	server := &Server{
		trustPolicy: policy, runtimeBuild: "app.v1", processGeneration: generation,
	}
	payload := []byte(`{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":1,"runtime_identity":{"process_generation":"01000000000000000000000000000000","runtime_build":"app.v1"}}`)
	if _, err := server.authorizeBrokerHandoff("lifecycle", payload); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("lifecycle role authorization = %v, want ErrPermissionDenied", err)
	}
	if _, err := server.authorizeBrokerHandoff("handoff", payload); err != nil {
		t.Fatalf("handoff role authorization: %v", err)
	}
	server.processGeneration = proc.OwnerGeneration{2}
	if _, err := server.authorizeBrokerHandoff("handoff", payload); err == nil {
		t.Fatal("foreign runtime identity unexpectedly authorized")
	}
}

func TestBrokerHandoffCannotAuthorizeBeforeReadyPublication(t *testing.T) {
	server := &Server{}
	err := server.authorizePreReady(context.Background(), entry{route: routeHandoff}, Request{})
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("authorize handoff without publication = %v, want ErrNotReady", err)
	}
}
