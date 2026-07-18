//go:build daemonkit_unsigned

package trust

import (
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/wire"
)

func TestUnsignedVerifierFailsClosed(t *testing.T) {
	err := verifyRequirement(wire.Peer{}, Requirement{})
	if err == nil {
		t.Fatal("verifyRequirement() = nil, want an error")
	}
	if !errors.Is(err, ErrNoVerifier) {
		t.Errorf("verifyRequirement() = %v, want ErrNoVerifier", err)
	}
}
