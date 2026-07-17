//go:build !darwin && !daemonkit_unsigned

package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

// verifyRequirement fails closed on any platform without a code-identity
// verifier: a configured Requirement can never be satisfied, so it is denied
// rather than downgraded to the UID floor.
func verifyRequirement(_ wire.Peer, _ Requirement) error {
	return fmt.Errorf("%w (this platform has no codesign verifier)", ErrNoVerifier)
}
