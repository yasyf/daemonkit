package wire

import (
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/proc"
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
