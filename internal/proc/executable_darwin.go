package proc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

const execPathBufferSize = 4096

var (
	pidPathOnce sync.Once
	pidPathSym  uintptr
	pidPathErr  error
)

func loadPidPath() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		pidPathErr = fmt.Errorf("proc: dlopen libSystem: %w", err)
		return
	}
	symbol, err := purego.Dlsym(lib, "proc_pidpath")
	if err != nil {
		pidPathErr = fmt.Errorf("proc: dlsym proc_pidpath: %w", err)
		return
	}
	pidPathSym = symbol
}

// ExecutablePath returns the absolute exec path captured by the kernel for
// pid. A pid the kernel does not hold reports ErrNoProcess; a live process the
// kernel cannot name falls back to the path it recorded at exec, and one that
// stays unidentifiable is reported as unresolvable rather than as gone.
func ExecutablePath(pid int) (string, error) {
	pidPathOnce.Do(loadPidPath)
	if pidPathErr != nil {
		return "", pidPathErr
	}
	buf := make([]byte, execPathBufferSize)
	size, errno := readPidPath(pid, buf)
	if size <= 0 {
		switch classifyPidPath(errno) {
		case pidPathGone:
			return "", ErrNoProcess
		case pidPathUnnamed:
			return execPathFromArgs(pid)
		}
		return "", fmt.Errorf("read executable path for pid %d: %w", pid, errno)
	}
	path := buf[:size]
	if len(path) > 0 && path[len(path)-1] == 0 {
		path = path[:len(path)-1]
	}
	if len(path) == 0 {
		return "", fmt.Errorf("read executable path for pid %d: empty path", pid)
	}
	return string(path), nil
}

// pidPathVerdict is what one failed proc_pidpath call proves about its pid.
type pidPathVerdict int

const (
	pidPathGone pidPathVerdict = iota
	pidPathUnnamed
	pidPathUndetermined
)

// classifyPidPath reads one proc_pidpath errno. ESRCH is the only one that
// proves the pid is gone: the kernel answers ENOENT for a live process whose
// text vnode it can no longer name — an upgrade that unlinked the binary out
// from under a running daemon — and EINVAL, EPERM and everything else name a
// call that failed rather than a process that left.
func classifyPidPath(errno unix.Errno) pidPathVerdict {
	switch {
	case errors.Is(errno, unix.ESRCH):
		return pidPathGone
	case errors.Is(errno, unix.ENOENT):
		return pidPathUnnamed
	}
	return pidPathUndetermined
}

// execPathFromArgs recovers pid's executable from the argument block the
// kernel captured at exec, the one record left when proc_pidpath cannot name
// the text vnode. KERN_PROCARGS2 answers only for the caller's own processes
// and refuses a dead pid and another user's with the same EINVAL, so the
// process table settles which. Its exec path is the unresolved execve
// argument — a homebrew shim rather than the Cellar file behind it — so it is
// resolved into the form the kernel reports before anything compares it, and a
// path that will not resolve leaves the process a survivor.
func execPathFromArgs(pid int) (string, error) {
	raw, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return "", unnamedProcess(pid, err)
	}
	path := parseExecPath(raw)
	if !filepath.IsAbs(path) {
		return "", errUnresolvedExecutable
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", errUnresolvedExecutable
	}
	return filepath.Clean(resolved), nil
}

// unnamedProcess decides what a KERN_PROCARGS2 refusal proves about a pid the
// kernel would not name.
func unnamedProcess(pid int, cause error) error {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return fmt.Errorf("identify pid %d after reading its arguments failed (%w): %w", pid, cause, err)
	}
	if len(procs) == 0 {
		return ErrNoProcess
	}
	if int(procs[0].Eproc.Ucred.Uid) != os.Geteuid() {
		return errForeignProcess
	}
	return errUnresolvedExecutable
}

// parseExecPath reads the exec path KERN_PROCARGS2 writes after its argc
// header, and returns "" for a block that carries none.
func parseExecPath(raw []byte) string {
	const argcSize = 4
	if len(raw) <= argcSize {
		return ""
	}
	rest := raw[argcSize:]
	end := bytes.IndexByte(rest, 0)
	if end <= 0 {
		return ""
	}
	return string(rest[:end])
}

// readPidPath binds one call to:
//
//	int proc_pidpath(int pid, void *buffer, uint32_t buffersize);
//
// and takes its errno from the same trampoline, which reads it on the thread
// that made the call. Reading errno through a separate __error() call cannot:
// errno is thread-local, the goroutine can be rescheduled onto another thread
// between the two calls, and the value it then reads belongs to that thread —
// which reported a live process as EINTR or ETIMEDOUT and failed the whole
// executable inventory closed.
func readPidPath(pid int, buf []byte) (int32, unix.Errno) {
	var pinner runtime.Pinner
	pinner.Pin(&buf[0])
	defer pinner.Unpin()
	result, _, errno := purego.SyscallN(
		pidPathSym,
		uintptr(pid),
		uintptr(unsafe.Pointer(&buf[0])), //nolint:gosec // FFI requires the pinned buffer pointer.
		uintptr(len(buf)),
	)
	size := int32(result) //nolint:gosec // proc_pidpath returns a C int in the low half of the register.
	if size > 0 {
		return size, 0
	}
	// purego's darwin trampoline loads the 4-byte C errno with an 8-byte move,
	// so the upper half is adjacent thread-local noise.
	return size, unix.Errno(uint32(errno)) //nolint:gosec // The errno is a C int; the upper half is noise.
}
