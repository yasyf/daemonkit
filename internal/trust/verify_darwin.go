//go:build darwin && !daemonkit_unsigned

package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/proc"
)

func verifyRequirement(token proc.AuditToken, req Requirement) error {
	if !token.Valid() {
		return fmt.Errorf("%w (the peer carries no audit token)", ErrNoVerifier)
	}
	csopsOnce.Do(loadCsops)
	if csopsErr != nil {
		return fmt.Errorf("%w: %w", ErrNoVerifier, csopsErr)
	}
	return verifyToken(kernelReads(token), req)
}
