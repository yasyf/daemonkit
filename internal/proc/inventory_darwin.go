//go:build darwin

package proc

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// processIDs enumerates the live processes this consumer owns, reading each
// owner out of the one table snapshot that already carries it. The euid belongs
// here rather than beside the executable read: proc_pidpath names another
// user's process as readily as one of this consumer's, so a floor applied
// downstream of it never runs on the path that succeeds.
func processIDs() ([]int, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("enumerate process table: %w", err)
	}
	euid := os.Geteuid()
	pids := make([]int, 0, len(procs))
	for _, kp := range procs {
		if int(kp.Eproc.Ucred.Uid) != euid {
			continue
		}
		if pid := int(kp.Proc.P_pid); pid > 1 && kp.Proc.P_stat != darwinZombieState {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
