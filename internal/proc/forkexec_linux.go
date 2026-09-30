package proc

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// startChild forks and execs the target traced, then trades the exec trap for
// a plain job-control stop: linux has no suspended spawn, and PTRACE_TRACEME
// is the one way to halt a child after its image is established and before
// its first instruction. The child is detached with SIGSTOP injected in place
// of the trap, so it leaves the trace already stopped and no instruction of
// the target executes until releaseChild. Every ptrace request belongs to the
// thread that forked, which is why the whole exchange holds one OS thread.
//
// RLIMIT_NPROC is not lowered across the fork as it is on darwin. Linux counts
// every thread of the uid against it, and that total moves by more than any
// fixed headroom while a long-lived child is running, so an inherited cap
// would refuse the child's own forks on someone else's load.
func startChild(c Cmd, files spawnFiles) (int, error) {
	env := c.Env
	if env == nil {
		env = os.Environ()
	}
	descriptors := []uintptr{files.stdin.Fd(), files.stdout.Fd(), files.stderr.Fd()}
	if files.handoff != nil {
		descriptors = append(descriptors, files.handoff.Fd())
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	pid, err := syscall.ForkExec(c.Path, append([]string{c.Path}, c.Args...), &syscall.ProcAttr{
		Dir:   c.Dir,
		Env:   env,
		Files: descriptors,
		Sys:   &syscall.SysProcAttr{Setsid: c.Session, Ptrace: true},
	})
	if err != nil {
		return 0, fmt.Errorf("fork and exec %s: %w", c.Path, err)
	}
	reaped, err := suspendAtEntry(pid)
	if err == nil {
		return pid, nil
	}
	if reaped {
		return 0, err
	}
	killErr := syscall.Kill(pid, syscall.SIGKILL)
	if terminal := awaitExit(pid); terminal.err != nil {
		return 0, errors.Join(err, killErr, fmt.Errorf("%w: pid %d was not proven reaped: %w", ErrUnsettled, pid, terminal.err))
	}
	return 0, err
}

func releaseChild(pid int) error {
	return syscall.Kill(pid, syscall.SIGCONT)
}

// suspendAtEntry leaves the traced child stopped and untraced at its entry
// point. reaped reports that a wait already collected the child's exit, after
// which its PID is no longer this process's to signal.
func suspendAtEntry(pid int) (reaped bool, err error) {
	trap, reaped, err := awaitStop(pid, 0)
	if err != nil {
		return reaped, err
	}
	if trap != unix.SIGTRAP {
		return false, fmt.Errorf("proc: pid %d stopped on %v, want its exec trap", pid, trap)
	}
	if _, _, errno := unix.Syscall6(unix.SYS_PTRACE, unix.PTRACE_DETACH, uintptr(pid), 0, uintptr(unix.SIGSTOP), 0, 0); errno != 0 {
		return false, fmt.Errorf("proc: detach pid %d stopped: %w", pid, errno)
	}
	stop, reaped, err := awaitStop(pid, unix.WUNTRACED)
	if err != nil {
		return reaped, err
	}
	if stop != unix.SIGSTOP {
		return false, fmt.Errorf("proc: pid %d stopped on %v after its detach, want SIGSTOP", pid, stop)
	}
	return false, nil
}

func awaitStop(pid, options int) (stop unix.Signal, reaped bool, err error) {
	var wstatus unix.WaitStatus
	for {
		_, err := unix.Wait4(pid, &wstatus, options, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return 0, true, fmt.Errorf("proc: await the stop of pid %d: %w", pid, err)
		}
		break
	}
	if !wstatus.Stopped() {
		return 0, true, fmt.Errorf("proc: pid %d ended before it was suspended (wait status %#x)", pid, uint32(wstatus))
	}
	return wstatus.StopSignal(), false, nil
}
