package proc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// deletedSuffix is what the kernel appends to the path of an executable whose
// inode has no link left.
const deletedSuffix = " (deleted)"

// ExecutablePath returns the absolute exec path the kernel holds for pid. A
// pid the kernel does not hold reports ErrNoProcess. A live process whose
// binary was replaced under it answers with the path it was executed from, in
// the resolved form the kernel reports, and one that stays unidentifiable is
// reported as unresolvable rather than as gone.
func ExecutablePath(pid int) (string, error) {
	link := "/proc/" + strconv.Itoa(pid) + "/exe"
	target, err := os.Readlink(link)
	if err != nil {
		return "", unnamedProcess(pid, err)
	}
	recorded, unlinked := strings.CutSuffix(target, deletedSuffix)
	if !unlinked {
		return target, nil
	}
	var image unix.Stat_t
	if err := unix.Stat(link, &image); err != nil {
		return "", unnamedProcess(pid, err)
	}
	if image.Nlink != 0 {
		return target, nil
	}
	resolved, err := filepath.EvalSymlinks(recorded)
	if err != nil {
		return "", errUnresolvedExecutable
	}
	return filepath.Clean(resolved), nil
}

// unnamedProcess decides what a refused read of pid's exe link proves. The
// link is ENOENT for a pid that left and for a task with no image alike, and
// EACCES for another user's process and a non-dumpable one of this user's, so
// the process table settles which. Any other errno names a read that failed
// rather than a process that left.
func unnamedProcess(pid int, cause error) error {
	stat, err := readStat(pid)
	if errors.Is(err, errNoProc) {
		return ErrNoProcess
	}
	if err != nil {
		return fmt.Errorf("identify pid %d after reading its executable failed (%w): %w", pid, cause, err)
	}
	if stat.zombie() {
		return ErrNoProcess
	}
	uid, err := effectiveUID(pid)
	if errors.Is(err, errNoProc) {
		return ErrNoProcess
	}
	if err != nil {
		return fmt.Errorf("identify pid %d after reading its executable failed (%w): %w", pid, cause, err)
	}
	if uid != os.Geteuid() {
		return errForeignProcess
	}
	if vanished(cause) || errors.Is(cause, unix.EACCES) || errors.Is(cause, unix.EPERM) {
		return errUnresolvedExecutable
	}
	return fmt.Errorf("read executable path for pid %d: %w", pid, cause)
}
