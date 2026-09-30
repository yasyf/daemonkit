package daemonkit

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
)

// suspendedToken reads the audit token of a child the spawn still holds
// suspended, where its PID provably cannot have been reaped.
func suspendedToken(pid int) (proc.AuditToken, error) {
	minted, err := trust.ProcessToken(pid)
	if err != nil {
		return proc.AuditToken{}, fmt.Errorf("daemonkit: read the suspended child's audit token: %w", err)
	}
	if !minted.Valid() || minted.PID() != pid {
		return proc.AuditToken{}, fmt.Errorf("daemonkit: suspended child audit token names pid %d, want %d", minted.PID(), pid)
	}
	return minted, nil
}
