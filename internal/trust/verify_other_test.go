//go:build !darwin && !daemonkit_unsigned

package trust

import (
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/internal/proc"
)

func TestUnsupportedPlatformVerifierFailsClosed(t *testing.T) {
	err := verifyRequirement(proc.AuditToken{}, Requirement{})
	if !errors.Is(err, ErrNoVerifier) {
		t.Errorf("verifyRequirement() = %v, want ErrNoVerifier", err)
	}
	req := Requirement{TeamID: testTeam, SigningIdentifier: testIdentifier}
	if err := Verify(Peer{UID: os.Geteuid()}, &req); !errors.Is(err, ErrNoVerifier) {
		t.Errorf("Verify(configured requirement) = %v, want ErrNoVerifier (never a UID-only downgrade)", err)
	}
}
