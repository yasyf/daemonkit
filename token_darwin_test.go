package daemonkit

import "github.com/yasyf/daemonkit/internal/proc"

// pinsSpawned reports whether token is the execution identity of the child
// spawned at pid.
func pinsSpawned(token proc.AuditToken, pid int) bool {
	return token.Valid() && token.PID() == pid
}
