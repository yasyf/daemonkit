//go:build daemonkit_unsigned

package trust

import (
	"fmt"

	peer "github.com/yasyf/daemonkit/peer"
)

// Fails closed: the daemonkit_unsigned tag drops the verifier, so a
// configured Requirement is denied, never downgraded to UID-only.
func verifyRequirement(_ peer.Identity, _ Requirement) error {
	return fmt.Errorf("%w (built with daemonkit_unsigned: no codesign verifier)", ErrNoVerifier)
}
