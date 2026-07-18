//go:build !darwin && !daemonkit_unsigned

package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

// Fails closed: no verifier on this platform, so a configured Requirement is denied, never downgraded to UID-only.
func verifyRequirement(_ wire.Peer, _ Requirement) error {
	return fmt.Errorf("%w (this platform has no codesign verifier)", ErrNoVerifier)
}
