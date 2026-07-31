package wire

import (
	"errors"
	"testing"
)

func TestReserveHandoffNonce(t *testing.T) {
	nonce := func(b byte) []byte {
		n := make([]byte, brokerHandoffNonceBytes)
		n[0] = b
		return n
	}
	tests := []struct {
		name    string
		prepare func(s *session)
		nonce   []byte
		wantErr error
	}{
		{
			name:    "fresh nonce reserves",
			prepare: func(*session) {},
			nonce:   nonce(1),
			wantErr: nil,
		},
		{
			name: "consumed nonce replays",
			prepare: func(s *session) {
				if err := s.reserveHandoffNonce(nonce(1)); err != nil {
					t.Fatalf("seed reserve: %v", err)
				}
			},
			nonce:   nonce(1),
			wantErr: ErrHandoffReplay,
		},
		{
			name: "attempts cap bounds the nonce map",
			prepare: func(s *session) {
				s.handoffMu.Lock()
				s.handoffAttempts = brokerHandoffMaximumAttempts
				s.handoffMu.Unlock()
			},
			nonce:   nonce(2),
			wantErr: ErrSessionCapacity,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := &session{}
			tt.prepare(sess)
			err := sess.reserveHandoffNonce(tt.nonce)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("reserveHandoffNonce() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestHandoffAttemptsCapCountsEveryReservation(t *testing.T) {
	sess := &session{}
	for i := range brokerHandoffMaximumAttempts {
		n := make([]byte, brokerHandoffNonceBytes)
		n[0] = byte(i)
		n[1] = byte(i >> 8)
		if err := sess.reserveHandoffNonce(n); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	over := make([]byte, brokerHandoffNonceBytes)
	over[2] = 1
	if err := sess.reserveHandoffNonce(over); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("reserve past the cap = %v, want ErrSessionCapacity", err)
	}
	if got := len(sess.handoffNonces); got != brokerHandoffMaximumAttempts {
		t.Fatalf("nonce map holds %d entries, want the cap %d", got, brokerHandoffMaximumAttempts)
	}
}

func TestDecodeBrokerHandoffRejects(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"empty payload", ""},
		{"short nonce", `{"nonce":"AAAA"}`},
		{"unknown field", `{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","protocol":1}`},
		{"trailing value", `{"nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeBrokerHandoff([]byte(tt.payload)); err == nil {
				t.Fatalf("decodeBrokerHandoff(%q) unexpectedly succeeded", tt.payload)
			}
		})
	}
}
