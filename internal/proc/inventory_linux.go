package proc

import (
	"errors"
	"os"
)

// processIDs enumerates the live processes this consumer owns. The euid belongs
// here rather than beside the executable read, which is refused for another
// user's process and a non-dumpable one of this user's alike. Kernel threads
// run no executable and are never a consumer's process.
func processIDs() ([]int, error) {
	table, err := tablePIDs()
	if err != nil {
		return nil, err
	}
	euid := os.Geteuid()
	pids := make([]int, 0, len(table))
	for _, pid := range table {
		if pid <= 1 {
			continue
		}
		stat, err := readStat(pid)
		if errors.Is(err, errNoProc) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if stat.flags&taskKernelThread != 0 || stat.zombie() {
			continue
		}
		uid, err := effectiveUID(pid)
		if errors.Is(err, errNoProc) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if uid == euid {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
