//go:build !darwin && !daemonkit_unsigned

package trust

import (
	"fmt"

	peer "github.com/yasyf/daemonkit/peer"
)

// Fails closed: no verifier on this platform, so a configured Requirement is denied, never downgraded to UID-only.
func verifyRequirement(_ peer.Identity, _ Requirement) error {
	return fmt.Errorf("%w (this platform has no codesign verifier)", ErrNoVerifier)
}
