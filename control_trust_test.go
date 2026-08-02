package daemonkit

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/yasyf/daemonkit/internal/trust"
)

// TestAttachNeverCallsAFailedVerificationAbsent pins the one ordering
// constraint in classifyWire: every one of trust's denials can wrap ENOENT
// out of csops (internal/trust/verify.go denyErrno), and ErrAbsent promises a
// proven no-listener. A squatter holding the socket, a kernel that answered a
// shape this build cannot read, and a peer that exited mid-verification are
// each a live socket, and none may be reported as no daemon at all.
func TestAttachNeverCallsAFailedVerificationAbsent(t *testing.T) {
	tests := []struct {
		name      string
		deny      error
		untrusted bool
	}{
		{"policy mismatch", trust.ErrUntrustedPeer, true},
		{"no verifier", trust.ErrNoVerifier, false},
		{"peer gone", trust.ErrPeerGone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fromKernel := fmt.Errorf("%w: csops op 16: %w", tt.deny, syscall.ENOENT)
			err := classifyWire(fmt.Errorf("wire: authorize accepting peer: %w", classifyServingTrust(fromKernel)))
			if errors.Is(err, ErrAbsent) {
				t.Errorf("classifyWire() = %v, want no ErrAbsent for a peer that failed verification", err)
			}
			if got := errors.Is(err, ErrUntrusted); got != tt.untrusted {
				t.Errorf("errors.Is(err, ErrUntrusted) = %t, want %t (err = %v)", got, tt.untrusted, err)
			}
			if !errors.Is(err, tt.deny) {
				t.Errorf("classifyWire() = %v, want the %v cause preserved", err, tt.deny)
			}
		})
	}
}

func TestServingTrustDenialsStayDisjoint(t *testing.T) {
	tests := []struct {
		name      string
		deny      error
		untrusted bool
	}{
		{"policy mismatch", trust.ErrUntrustedPeer, true},
		{"peer gone", trust.ErrPeerGone, false},
		{"no verifier", trust.ErrNoVerifier, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyServingTrust(fmt.Errorf("verify: %w", tt.deny))
			if got := errors.Is(err, ErrUntrusted); got != tt.untrusted {
				t.Fatalf("errors.Is(err, ErrUntrusted) = %t, want %t (err = %v)", got, tt.untrusted, err)
			}
			if !errors.Is(err, tt.deny) {
				t.Fatalf("classifyServingTrust() = %v, want the %v cause preserved", err, tt.deny)
			}
		})
	}
}
