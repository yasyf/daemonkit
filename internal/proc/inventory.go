package proc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

// errForeignProcess means a live process is owned by another user. daemonkit's
// trust floor is the same effective uid, so such a process is never one of
// this consumer's own, and the inventory skips the one it cannot read rather
// than counting it.
var errForeignProcess = errors.New("proc: process is owned by another user")

// errUnresolvedExecutable means a live same-user process is running an
// executable neither the kernel nor the recorded execve path could resolve.
// The inventory reports it as unnameable: an absence proof may never rest on a
// process it failed to identify, and it may not attribute one either.
var errUnresolvedExecutable = errors.New("proc: executable path is unresolvable")

// Report is what one executable-scoped inventory proved about the live process
// table, in the two sets that carry different authority. Both hold same-euid
// processes alone: daemonkit's trust floor makes another user's process none of
// this consumer's, and counting one refuses the gate for good.
//
// Matched is attributed to the query: every process the kernel names as
// running that exact executable. Unnameable is attributed to nothing — no path
// can be read off those processes, so nothing about them says which executable
// they run — and each carries an empty Executable beside its full instance
// pin. A caller holds an unnameable process against itself only when it
// recognizes the pin as one it recorded; attributing every unnameable process
// to every query wedges the gate shut on the first stranger's husk.
type Report struct {
	Matched    []Identity
	Unnameable []Identity
}

// ExecutableIdentities reports every live same-user process whose executable
// is path, and every live same-user process nothing could name. It does not
// use names or shell process discovery, and it revalidates each matched PID
// around the identity snapshot: both the executable and the {start, boot}
// instance are re-read afterwards, so a PID that was reused mid-inventory by
// another process running the same executable is dropped rather than reported
// at its dead predecessor's pin.
//
// It never answers empty for a process it could not read. A live same-user
// process whose executable neither the kernel nor its recorded execve path
// resolves is reported in Unnameable, because an absence proof that drops what
// it cannot identify clears the irreversible step it guards. Any errno past
// the three that classify a process — gone, another user's, unnameable —
// fails the whole inventory rather than quietly shrinking it.
//
// The query resolves once, before the scan, and each process resolves as the
// kernel names it. Those are different instants, and the window between them
// does not close from user space: a running process's executable is a file
// while the query is a path, so a same-uid writer that repoints a component of
// the query between a daemon's exec and this resolution leaves the daemon
// running one file while the query names another, and the scan drops it. That
// residual is why an irreversible action gates on a recorded identity as well
// as on this scan.
func ExecutableIdentities(path string) (Report, error) {
	return executableIdentities(path, processIDs, ExecutablePath, ProbeIdentity)
}

// SameInstance reports whether two pins name one process instance. It is the
// module's only identity comparison reached from outside proc, so a caller
// correlating an unnameable process against an identity it recorded compares
// the whole pin the way everything else does — never the PID alone.
func SameInstance(a, b Identity) bool { return instance(a).matches(instance(b)) }

func executableIdentities(
	path string,
	list func() ([]int, error),
	executable func(int) (string, error),
	probe func(int) (Identity, error),
) (Report, error) {
	matches, err := executableMatcher(path)
	if err != nil {
		return Report{}, err
	}
	pids, err := list()
	if err != nil {
		return Report{}, err
	}
	report := Report{Matched: make([]Identity, 0), Unnameable: make([]Identity, 0)}
	for _, pid := range pids {
		before, err := readExecutable(executable, pid)
		if err != nil {
			return Report{}, fmt.Errorf("inspect executable for pid %d: %w", pid, err)
		}
		if before.state == execSkipped || (before.state == execResolved && !matches(before.path)) {
			continue
		}
		identity, err := probe(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return Report{}, fmt.Errorf("probe executable pid %d: %w", pid, err)
		}
		if before.state == execUnnamed {
			report.Unnameable = append(report.Unnameable, identity)
			continue
		}
		after, err := readExecutable(executable, pid)
		if err != nil {
			return Report{}, fmt.Errorf("revalidate executable for pid %d: %w", pid, err)
		}
		if after.state == execSkipped || (after.state == execResolved && after.path != before.path) {
			continue
		}
		repinned, err := probe(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return Report{}, fmt.Errorf("revalidate identity for pid %d: %w", pid, err)
		}
		if !instance(identity).matches(instance(repinned)) {
			continue
		}
		identity.Executable = before.path
		report.Matched = append(report.Matched, identity)
	}
	sort.Slice(report.Matched, func(i, j int) bool { return report.Matched[i].PID < report.Matched[j].PID })
	sort.Slice(report.Unnameable, func(i, j int) bool { return report.Unnameable[i].PID < report.Unnameable[j].PID })
	return report, nil
}

// execState is what one executable read settled about a PID.
type execState int

const (
	// execResolved carries the path the process is running.
	execResolved execState = iota
	// execSkipped is a PID this inventory can never be about: one the kernel
	// no longer holds, or one another user owns.
	execSkipped
	// execUnnamed is a live same-user process nothing could identify, which
	// the inventory reports rather than drops.
	execUnnamed
)

type execRead struct {
	path  string
	state execState
}

func readExecutable(executable func(int) (string, error), pid int) (execRead, error) {
	path, err := executable(pid)
	switch {
	case errors.Is(err, ErrNoProcess), errors.Is(err, errForeignProcess):
		return execRead{state: execSkipped}, nil
	case errors.Is(err, errUnresolvedExecutable):
		return execRead{state: execUnnamed}, nil
	case err != nil:
		return execRead{}, err
	}
	return execRead{path: path}, nil
}

// executableMatcher accepts the query in the exact form the caller wrote it
// and, when it resolves, in the symlink-free form a running process reports.
// The two sides must be compared in the same form or the gate silently misses:
// a caller's /var path never equals the kernel's /private/var, and a program
// named through a homebrew shim never equals the Cellar file behind it.
//
// A query naming no file resolves to nothing and only its literal form remains
// to compare — the state a bundle renamed aside leaves its agents' programs
// in. That answer is exact rather than degraded: nothing the kernel can name
// runs from a path that is not there, and a process running an unlinked
// executable is unnameable rather than matched. Every other resolution failure
// — a directory the caller may not traverse, a symlink loop — is returned
// instead: a query the scan could not put in the kernel's form matches
// nothing, and a gate that matches nothing is a gate that always passes.
func executableMatcher(path string) (func(string) bool, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if errors.Is(err, fs.ErrNotExist) {
		return func(candidate string) bool { return candidate == path }, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve inventory query %q: %w", path, err)
	}
	resolved = filepath.Clean(resolved)
	return func(candidate string) bool { return candidate == path || candidate == resolved }, nil
}

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
