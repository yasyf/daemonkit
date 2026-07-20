//go:build !darwin && !daemonkit_unsigned

package codeidentity

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

func verifyCodeIdentity(_ wire.Peer, _ CodeIdentity) error {
	return fmt.Errorf("%w: unsupported platform", ErrNoVerifier)
}
