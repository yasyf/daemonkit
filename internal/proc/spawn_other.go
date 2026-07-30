//go:build !darwin

package proc

import (
	"fmt"
	"os"
	"syscall"
)

// spawnSuspends is false off darwin: the child starts running before its
// record is durable — the microsecond record-before-run window is DESIGN's
// named non-structural residue (4), on a CI-only platform.
const spawnSuspends = false

func startChild(c Cmd, files spawnFiles) (int, error) {
	env := c.Env
	if env == nil {
		env = os.Environ()
	}
	fds := []uintptr{files.stdin.Fd(), files.stdout.Fd(), files.stderr.Fd()}
	if files.handoff != nil {
		fds = append(fds, files.handoff.Fd())
	}
	attr := &syscall.ProcAttr{
		Dir:   c.Dir,
		Env:   env,
		Files: fds,
		Sys:   &syscall.SysProcAttr{Setsid: c.Session},
	}
	var pid int
	err := withChildNprocCap(func() error {
		started, err := syscall.ForkExec(c.Path, append([]string{c.Path}, c.Args...), attr)
		if err != nil {
			return fmt.Errorf("proc: fork-exec %s: %w", c.Path, err)
		}
		pid = started
		return nil
	})
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func releaseChild(int) error { return nil }
