package daemonkit

import "github.com/yasyf/daemonkit/internal/proc"

// alive reports a process still running. kill(pid, 0) answers for a zombie
// too, and on linux an orphan stays one until init reaps it, so the process
// table decides.
func alive(pid int) bool {
	id, err := proc.ProbeIdentity(pid)
	if err != nil {
		return false
	}
	_, settled, err := proc.Observe(id)
	return err == nil && !settled
}
