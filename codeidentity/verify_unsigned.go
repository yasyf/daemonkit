//go:build daemonkit_unsigned

package codeidentity

import (
	"fmt"

	peer "github.com/yasyf/daemonkit/peer"
)

func verifyCodeIdentity(_ peer.Identity, _ CodeIdentity) error {
	return fmt.Errorf("%w: unsigned build", ErrNoVerifier)
}
