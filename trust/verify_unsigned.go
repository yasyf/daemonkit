//go:build daemonkit_unsigned

package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

// Fails closed: the daemonkit_unsigned tag drops the verifier, so a
// configured Requirement is denied, never downgraded to UID-only.
func verifyRequirement(_ wire.Peer, _ Requirement) error {
	return fmt.Errorf("%w (built with daemonkit_unsigned: no codesign verifier)", ErrNoVerifier)
}
