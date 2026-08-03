package proc

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// suspendedChild posix_spawns path parked before its first instruction, so a
// scan sees a live same-user process the kernel names exactly and no
// instruction of the target ever runs.
func suspendedChild(t *testing.T, path string) int {
	t.Helper()
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := startChild(Cmd{Path: path}, spawnFiles{stdin: devNull, stdout: devNull, stderr: devNull})
	_ = devNull.Close()
	if err != nil {
		t.Fatalf("startChild(%q) = %v", path, err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		awaitExit(pid)
	})
	return pid
}

// TestExecutableIdentitiesFailsClosedOnAnUnresolvableQuery is the other
// fail-closed half: a query the scan cannot put in the kernel's form matches
// nothing, and a gate that matches nothing always passes. A query that merely
// names no file is answerable — nothing runs from a path that is not there —
// and answers empty.
func TestExecutableIdentitiesFailsClosedOnAnUnresolvableQuery(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	loop := filepath.Join(dir, "loop")
	if err := os.Symlink(loop, loop); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{name: "a path nothing is there to hold", query: filepath.Join(dir, "absent")},
		{name: "a query under a regular file", query: filepath.Join(file, "under"), wantErr: true},
		{name: "a symlink loop", query: loop, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report, err := ExecutableIdentities(test.query)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ExecutableIdentities(%q) = %+v, want an error", test.query, report)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExecutableIdentities(%q) = %v", test.query, err)
			}
			if len(report.Matched) != 0 {
				t.Fatalf("Matched = %+v, want none", report.Matched)
			}
		})
	}
}

func TestExecutableIdentitiesKeepsStableInstance(t *testing.T) {
	const program = "/bin/sleep"
	pid := suspendedChild(t, program)
	report, err := ExecutableIdentities(program)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", program, err)
	}
	at := slices.IndexFunc(report.Matched, func(id Identity) bool { return id.PID == pid })
	if at < 0 {
		t.Fatalf("Matched = %+v, want pid %d", report.Matched, pid)
	}
	got := report.Matched[at]
	if got.Executable != program || got.Start == 0 || got.Boot == 0 {
		t.Fatalf("Matched[%d] = %+v, want %q under a non-zero start and boot", at, got, program)
	}
}

// TestSameInstanceComparesTheWholePin is the correlation consumers gate on: an
// unnameable process is theirs only when the pin is the one they recorded, and
// a PID the kernel has since handed to a stranger is not that pin.
func TestSameInstanceComparesTheWholePin(t *testing.T) {
	recorded := Identity{PID: 10, Start: 77, Boot: 9}
	tests := []struct {
		name  string
		other Identity
		want  bool
	}{
		{name: "the recorded instance", other: Identity{PID: 10, Start: 77, Boot: 9}, want: true},
		{name: "the same pin under an executable", other: Identity{PID: 10, Start: 77, Boot: 9, Executable: "/x"}, want: true},
		{name: "a reused pid", other: Identity{PID: 10, Start: 78, Boot: 9}},
		{name: "another boot session", other: Identity{PID: 10, Start: 77, Boot: 10}},
		{name: "another pid", other: Identity{PID: 11, Start: 77, Boot: 9}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SameInstance(recorded, test.other); got != test.want {
				t.Fatalf("SameInstance(%+v, %+v) = %v, want %v", recorded, test.other, got, test.want)
			}
		})
	}
}

func TestIdentityNamesTheSurvivor(t *testing.T) {
	tests := []struct {
		name     string
		identity Identity
		want     string
	}{
		{
			name:     "a named process",
			identity: Identity{PID: 10, Start: 77, Boot: 9, Executable: "/Applications/Exact.app/Contents/MacOS/Exact"},
			want:     "pid 10 /Applications/Exact.app/Contents/MacOS/Exact",
		},
		{
			name:     "a process nothing could name still names its pin",
			identity: Identity{PID: 10, Start: 77, Boot: 9},
			want:     "pid 10 (unnameable, start 77, boot 9)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.identity.String(); got != test.want {
				t.Fatalf("String() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestExecutableIdentitiesMatchesTheQueryInBothForms pins both sides of the
// comparison in the same form: a consumer that names its program through a
// symlink must still match the symlink-free path a process runs under, or the
// gate reports a clear inventory for a daemon that is running.
func TestExecutableIdentitiesMatchesTheQueryInBothForms(t *testing.T) {
	const resolved = "/bin/sleep"
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	if err := os.Symlink(resolved, link); err != nil {
		t.Fatal(err)
	}
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	pid := suspendedChild(t, link)
	tests := []struct {
		name  string
		query string
	}{
		{name: "queried through the symlink", query: link},
		{name: "queried under a symlinked directory", query: filepath.Join(resolvedDir, "link")},
		{name: "queried in the resolved form", query: resolved},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report, err := ExecutableIdentities(test.query)
			if err != nil {
				t.Fatalf("ExecutableIdentities(%q) = %v", test.query, err)
			}
			want := Identity{PID: pid, Executable: resolved}
			at := slices.IndexFunc(report.Matched, func(id Identity) bool { return id.PID == pid })
			if at < 0 {
				t.Fatalf("Matched = %+v, want %+v", report.Matched, want)
			}
			if got := report.Matched[at]; got.Executable != resolved {
				t.Fatalf("Matched[%d] = %+v, want executable %q", at, got, resolved)
			}
		})
	}
}

// TestExecutablePathTellsGoneFromReadable pins the gate's fail-closed contract
// at its source: a live process reports its kernel-resolved executable, and
// only a pid the kernel does not hold reports ErrNoProcess.
func TestExecutablePathTellsGoneFromReadable(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		pid  int
		want string
		err  error
	}{
		{name: "this process", pid: os.Getpid(), want: resolved},
		{name: "a pid the kernel cannot hold", pid: 1 << 30, err: ErrNoProcess},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecutablePath(tt.pid)
			if !errors.Is(err, tt.err) {
				t.Fatalf("ExecutablePath(%d) error = %v, want %v", tt.pid, err, tt.err)
			}
			if got != tt.want {
				t.Fatalf("ExecutablePath(%d) = %q, want %q", tt.pid, got, tt.want)
			}
		})
	}
}

func TestExecutableIdentitiesFindsSelf(t *testing.T) {
	self, err := ExecutablePath(os.Getpid())
	if err != nil {
		t.Fatalf("ExecutablePath(self) = %v", err)
	}
	report, err := ExecutableIdentities(self)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", self, err)
	}
	if slices.ContainsFunc(report.Unnameable, func(id Identity) bool { return id.PID == os.Getpid() }) {
		t.Fatalf("Unnameable = %v, want this process named rather than unnameable", report.Unnameable)
	}
	for _, identity := range report.Matched {
		if identity.PID != os.Getpid() {
			continue
		}
		if identity.Start == 0 || identity.Boot == 0 {
			t.Fatalf("identity = %+v, want non-zero start and boot", identity)
		}
		if identity.Executable != self {
			t.Fatalf("Executable = %q, want %q", identity.Executable, self)
		}
		return
	}
	t.Fatalf("Matched = %v, want pid %d", report.Matched, os.Getpid())
}

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
		t.Fatal("kern.proc.all reported no process owned by another user; a live macOS always has some, and without one the euid floor is unprovable")
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
	t.Fatal("no nameable process owned by another user; a live macOS always runs some, and without one this scan proves nothing")
	return 0, ""
}
