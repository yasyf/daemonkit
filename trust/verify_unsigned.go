//go:build daemonkit_unsigned

package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

// verifyRequirement fails closed: the daemonkit_unsigned tag drops the darwin
// Security.framework verifier (for building without the purego/codesign path),
// so a configured Requirement can never be satisfied and is denied rather than
// downgraded to the UID floor. UID-only trust is requested explicitly with a nil
// Requirement, never implied by a build tag.
func verifyRequirement(_ wire.Peer, _ Requirement) error {
	return fmt.Errorf("%w (built with daemonkit_unsigned: no codesign verifier)", ErrNoVerifier)
}
