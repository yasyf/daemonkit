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
// The kernel charges a fork to the real uid, so that is the count the headroom sits on.
func withChildNprocCap(spawn func() error) error {
	spawnRlimitMu.Lock()
	defer spawnRlimitMu.Unlock()

	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ruid", os.Getuid())
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
	defer restoreNproc(orig)

	return spawn()
}

func restoreNproc(orig unix.Rlimit) {
	var applied unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &applied); err != nil {
		panic(fmt.Sprintf("proc: read RLIMIT_NPROC to restore it: %v", err))
	}
	restored := restoredNproc(orig, applied)
	if err := unix.Setrlimit(unix.RLIMIT_NPROC, &restored); err != nil {
		panic(fmt.Sprintf("proc: restore RLIMIT_NPROC to %+v: %v", restored, err))
	}
}

// restoredNproc raises the soft limit back under the hard limit the kernel
// applied: darwin clamps a non-root hard limit to kern.maxprocperuid on
// setrlimit, so the original hard limit reads as a raise and is refused.
func restoredNproc(orig, applied unix.Rlimit) unix.Rlimit {
	return unix.Rlimit{Cur: min(orig.Cur, applied.Max), Max: applied.Max}
}
