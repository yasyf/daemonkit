package proc

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

// TestProcessIDsHoldsTheSameEUIDFloor pins the trust floor where the scan's
// population is decided. proc_pidpath answers for another user's process as
// readily as for one of this consumer's own, so a floor applied anywhere later
// than here is a floor the common path never reaches.
func TestProcessIDsHoldsTheSameEUIDFloor(t *testing.T) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		t.Fatal(err)
	}
	foreign := make(map[int]int)
	for _, kp := range procs {
		if uid := int(kp.Eproc.Ucred.Uid); uid != os.Geteuid() {
			foreign[int(kp.Proc.P_pid)] = uid
		}
	}
	if len(foreign) == 0 {
		t.Skip("no process owned by another user to read")
	}
	pids, err := processIDs()
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		if uid, ok := foreign[pid]; ok {
			t.Fatalf("processIDs() returned pid %d owned by uid %d, want the same-euid floor", pid, uid)
		}
	}
	if !slices.Contains(pids, os.Getpid()) {
		t.Fatalf("processIDs() returned %d pids, want this process among them", len(pids))
	}
}

// TestExecutableIdentitiesSkipsAnotherUsersProcess is the floor at the exported
// boundary. Root runs daemons from paths a consumer may well query, and one
// counted as a survivor refuses every irreversible action forever — the same
// permanently-unpassable gate the unnameable split exists to prevent, arriving
// from the other direction.
func TestExecutableIdentitiesSkipsAnotherUsersProcess(t *testing.T) {
	pid, path := namedForeignProcess(t)
	report, err := ExecutableIdentities(path)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", path, err)
	}
	names := func(id Identity) bool { return id.PID == pid }
	if slices.ContainsFunc(report.Matched, names) {
		t.Fatalf("Matched = %+v, want pid %d skipped: it is another user's", report.Matched, pid)
	}
	if slices.ContainsFunc(report.Unnameable, names) {
		t.Fatalf("Unnameable = %+v, want pid %d skipped: it is another user's", report.Unnameable, pid)
	}
}

// namedForeignProcess returns a live process owned by another user together
// with the executable the kernel names it by, in the fully resolved form the
// scan compares against.
func namedForeignProcess(t *testing.T) (int, string) {
	t.Helper()
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		t.Fatal(err)
	}
	for _, kp := range procs {
		pid := int(kp.Proc.P_pid)
		if pid <= 1 || int(kp.Eproc.Ucred.Uid) == os.Geteuid() {
			continue
		}
		path, err := ExecutablePath(pid)
		if err != nil {
			continue
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			continue
		}
		return pid, resolved
	}
	t.Skip("no nameable process owned by another user to read")
	return 0, ""
}
