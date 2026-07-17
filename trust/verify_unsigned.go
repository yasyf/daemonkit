//go:build daemonkit_unsigned

package trust

import "github.com/yasyf/daemonkit/wire"

// verifyRequirement accepts any peer that passed the same-UID floor. This build
// exists ONLY for local unsigned test runs (the daemonkit_unsigned tag); release
// CI must reject any distributed artifact built with it. A configured
// Requirement is intentionally not enforced here — the tag is the opt-in.
func verifyRequirement(_ wire.Peer, _ Requirement) error {
	return nil
}
