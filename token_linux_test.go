package daemonkit

import "github.com/yasyf/daemonkit/internal/proc"

// pinsSpawned reports whether token names the child spawned at pid. A linux
// token carries the pid alone and is never Valid.
func pinsSpawned(token proc.AuditToken, pid int) bool {
	return !token.Valid() && token.PID() == pid
}
