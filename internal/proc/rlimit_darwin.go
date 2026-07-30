//go:build darwin

package proc

import (
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// Generous for a holder forking ~0 children, yet starves a runaway spawn loop before it exhausts the process table.
const spawnNprocHeadroom = 400

// No concurrent spawn may fork while this process's RLIMIT_NPROC is lowered.
var spawnRlimitMu sync.Mutex

// Lowers RLIMIT_NPROC across the fork so the child subtree inherits it; only ever LOWERS the limit.
func withChildNprocCap(spawn func() error) error {
	spawnRlimitMu.Lock()
	defer spawnRlimitMu.Unlock()

	procs, err := unix.SysctlKinfoProcSlice("kern.proc.uid", os.Getuid())
	if err != nil {
		return fmt.Errorf("count uid processes for spawn nproc cap: %w", err)
	}
	var orig unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &orig); err != nil {
		return fmt.Errorf("read RLIMIT_NPROC: %w", err)
	}
	capped := orig
	if want := uint64(len(procs) + spawnNprocHeadroom); want < capped.Cur {
		capped.Cur = want
	}
	if err := unix.Setrlimit(unix.RLIMIT_NPROC, &capped); err != nil {
		return fmt.Errorf("lower RLIMIT_NPROC for spawn: %w", err)
	}
	defer func() { _ = unix.Setrlimit(unix.RLIMIT_NPROC, &orig) }()

	return spawn()
}
