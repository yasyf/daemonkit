package proc

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/unix"
)

const spawnSuspends = true

const (
	posixSpawnStartSuspended = 0x0080
	posixSpawnSetSID         = 0x0400
	posixSpawnCloexecDefault = 0x4000
)

var (
	spawnOnce sync.Once
	spawnErr  error

	posixSpawn      func(pid *int32, path *byte, fileActions *uintptr, attr *uintptr, argv **byte, envp **byte) int32
	attrInit        func(attr *uintptr) int32
	attrDestroy     func(attr *uintptr) int32
	attrSetFlags    func(attr *uintptr, flags int16) int32
	actionsInit     func(fa *uintptr) int32
	actionsDestroy  func(fa *uintptr) int32
	actionsAddDup2  func(fa *uintptr, fd, newfd int32) int32
	actionsAddChdir func(fa *uintptr, path *byte) int32
)

func loadSpawnAPI() {
	lib, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		spawnErr = fmt.Errorf("proc: dlopen libSystem: %w", err)
		return
	}
	purego.RegisterLibFunc(&posixSpawn, lib, "posix_spawn")
	purego.RegisterLibFunc(&attrInit, lib, "posix_spawnattr_init")
	purego.RegisterLibFunc(&attrDestroy, lib, "posix_spawnattr_destroy")
	purego.RegisterLibFunc(&attrSetFlags, lib, "posix_spawnattr_setflags")
	purego.RegisterLibFunc(&actionsInit, lib, "posix_spawn_file_actions_init")
	purego.RegisterLibFunc(&actionsDestroy, lib, "posix_spawn_file_actions_destroy")
	purego.RegisterLibFunc(&actionsAddDup2, lib, "posix_spawn_file_actions_adddup2")
	purego.RegisterLibFunc(&actionsAddChdir, lib, "posix_spawn_file_actions_addchdir_np")
}

// startChild posix_spawns the target suspended in one step: no wrapper image
// runs, and no instruction of the target executes until releaseChild.
func startChild(c Cmd, files spawnFiles) (int, error) {
	spawnOnce.Do(loadSpawnAPI)
	if spawnErr != nil {
		return 0, spawnErr
	}
	path, err := cstring(c.Path)
	if err != nil {
		return 0, err
	}
	argv, argvHold, err := cstrings(append([]string{c.Path}, c.Args...))
	if err != nil {
		return 0, err
	}
	env := c.Env
	if env == nil {
		env = os.Environ()
	}
	envp, envpHold, err := cstrings(env)
	if err != nil {
		return 0, err
	}

	var attr uintptr
	if rc := attrInit(&attr); rc != 0 {
		return 0, fmt.Errorf("posix_spawnattr_init: %w", spawnErrno(rc))
	}
	defer attrDestroy(&attr)
	flags := int16(posixSpawnStartSuspended | posixSpawnCloexecDefault)
	if c.Session {
		flags |= posixSpawnSetSID
	}
	if rc := attrSetFlags(&attr, flags); rc != 0 {
		return 0, fmt.Errorf("posix_spawnattr_setflags: %w", spawnErrno(rc))
	}

	var fa uintptr
	if rc := actionsInit(&fa); rc != 0 {
		return 0, fmt.Errorf("posix_spawn_file_actions_init: %w", spawnErrno(rc))
	}
	defer actionsDestroy(&fa)
	stdio := []*os.File{files.stdin, files.stdout, files.stderr}
	if files.handoff != nil {
		stdio = append(stdio, files.handoff)
	}
	// A source fd inside the dup2 target range 0..len-1 would be clobbered by
	// an earlier ordered action (a daemon with stdio closed lands sources
	// there), so such a source is first moved above the range — os/exec's fd
	// shuffle. CLOEXEC_DEFAULT keeps the moved copies out of the child.
	sources := make([]int, len(stdio))
	var moved []int
	defer func() {
		for _, fd := range moved {
			_ = unix.Close(fd)
		}
	}()
	for i, f := range stdio {
		src := int(f.Fd())
		if src < len(stdio) {
			high, err := unix.FcntlInt(uintptr(src), unix.F_DUPFD_CLOEXEC, len(stdio))
			if err != nil {
				return 0, fmt.Errorf("proc: shuffle fd %d above the dup2 targets: %w", src, err)
			}
			moved = append(moved, high)
			src = high
		}
		sources[i] = src
	}
	for target, src := range sources {
		if rc := actionsAddDup2(&fa, int32(src), int32(target)); rc != 0 { //nolint:gosec // open descriptors and dup targets 0-3 fit int32
			return 0, fmt.Errorf("posix_spawn_file_actions_adddup2 %d: %w", target, spawnErrno(rc))
		}
	}
	if c.Dir != "" {
		dir, err := cstring(c.Dir)
		if err != nil {
			return 0, err
		}
		if rc := actionsAddChdir(&fa, dir); rc != 0 {
			return 0, fmt.Errorf("posix_spawn_file_actions_addchdir_np: %w", spawnErrno(rc))
		}
	}

	var pid int32
	err = withChildNprocCap(func() error {
		if rc := posixSpawn(&pid, path, &fa, &attr, &argv[0], &envp[0]); rc != 0 {
			return fmt.Errorf("posix_spawn %s: %w", c.Path, spawnErrno(rc))
		}
		return nil
	})
	runtime.KeepAlive(argvHold)
	runtime.KeepAlive(envpHold)
	if err != nil {
		return 0, err
	}
	return int(pid), nil
}

func releaseChild(pid int) error {
	return syscall.Kill(pid, syscall.SIGCONT)
}

func spawnErrno(rc int32) unix.Errno {
	return unix.Errno(uint32(rc)) //nolint:gosec // posix_spawn returns non-negative errno values
}

func cstring(s string) (*byte, error) {
	if strings.IndexByte(s, 0) >= 0 {
		return nil, fmt.Errorf("proc: string %q contains a NUL byte", s)
	}
	buf := make([]byte, len(s)+1)
	copy(buf, s)
	return &buf[0], nil
}

func cstrings(values []string) ([]*byte, [][]byte, error) {
	pointers := make([]*byte, 0, len(values)+1)
	hold := make([][]byte, 0, len(values))
	for _, value := range values {
		if strings.IndexByte(value, 0) >= 0 {
			return nil, nil, fmt.Errorf("proc: string %q contains a NUL byte", value)
		}
		buf := make([]byte, len(value)+1)
		copy(buf, value)
		hold = append(hold, buf)
		pointers = append(pointers, &buf[0])
	}
	pointers = append(pointers, nil)
	return pointers, hold, nil
}
