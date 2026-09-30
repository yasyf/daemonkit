package daemonkit

import "github.com/yasyf/daemonkit/internal/proc"

// suspendedToken is the transport token of a child the spawn still holds
// suspended. Linux mints no execution identity, so it names the pid alone.
func suspendedToken(pid int) (proc.AuditToken, error) {
	return proc.PeerToken(pid), nil
}
