package proc

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestExecutableIdentitiesExactAbsentPath(t *testing.T) {
	report, err := executableIdentities(
		"/daemonkit-test/definitely-not-an-executable",
		func() ([]int, error) { return []int{101, 202}, nil },
		func(int) (string, error) { return "/other/executable", nil },
		func(int) (Identity, error) {
			t.Fatal("probe called for a nonmatching executable")
			return Identity{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Matched) != 0 || len(report.Unnameable) != 0 {
		t.Fatalf("report = %+v, want none", report)
	}
}

func TestExecutableIdentitiesFailsClosedOnUnreadableProcess(t *testing.T) {
	permissionErr := errors.New("permission denied")
	_, err := executableIdentities(
		"/Applications/Exact.app/Contents/MacOS/Exact",
		func() ([]int, error) { return []int{10}, nil },
		func(int) (string, error) { return "", permissionErr },
		func(int) (Identity, error) { return Identity{}, nil },
	)
	if err == nil || !strings.Contains(err.Error(), "inspect executable for pid 10") {
		t.Fatalf("error = %v, want fail-closed inventory error", err)
	}
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
			report, err := executableIdentities(
				test.query,
				func() ([]int, error) { return []int{10}, nil },
				func(int) (string, error) { return "/other/executable", nil },
				func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
			)
			if test.wantErr {
				if err == nil {
					t.Fatalf("executableIdentities(%q) = %+v, want an error", test.query, report)
				}
				return
			}
			if err != nil {
				t.Fatalf("executableIdentities(%q) = %v", test.query, err)
			}
			if len(report.Matched) != 0 {
				t.Fatalf("Matched = %+v, want none", report.Matched)
			}
		})
	}
}

func TestExecutableIdentitiesDropsReusedPID(t *testing.T) {
	path := "/Applications/Exact.app/Contents/MacOS/Exact"
	probes := 0
	report, err := executableIdentities(
		path,
		func() ([]int, error) { return []int{10}, nil },
		func(int) (string, error) { return path, nil },
		func(pid int) (Identity, error) {
			probes++
			// The PID is reused by another instance of the same executable
			// between the bracketing executable reads.
			return Identity{PID: pid, Start: uint64(probes), Boot: 9}, nil
		},
	)
	if err != nil {
		t.Fatalf("executableIdentities() = %v", err)
	}
	if probes != 2 {
		t.Fatalf("probes = %d, want the identity re-probed around the executable bracket", probes)
	}
	if len(report.Matched) != 0 {
		t.Fatalf("Matched = %+v, want none: the pin named a dead instance", report.Matched)
	}
}

func TestExecutableIdentitiesKeepsStableInstance(t *testing.T) {
	path := "/Applications/Exact.app/Contents/MacOS/Exact"
	report, err := executableIdentities(
		path,
		func() ([]int, error) { return []int{10}, nil },
		func(int) (string, error) { return path, nil },
		func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
	)
	if err != nil {
		t.Fatalf("executableIdentities() = %v", err)
	}
	want := []Identity{{PID: 10, Start: 77, Boot: 9, Executable: path}}
	if !slices.Equal(report.Matched, want) {
		t.Fatalf("Matched = %+v, want %+v", report.Matched, want)
	}
}

// TestExecutableIdentitiesReportsAnUnidentifiableProcess is the fail-closed
// half of the gate, and the half that must not fail closed forever: a live
// same-user process whose executable nothing could resolve is reported, since
// dropping it reports an empty inventory for a daemon that may still be
// running — but it is reported apart from the query, since attributing it to
// every query lets one stranger's husk wedge every gate on the machine shut.
func TestExecutableIdentitiesReportsAnUnidentifiableProcess(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantMatched    []Identity
		wantUnnameable []Identity
	}{
		{
			name:           "a live same-user process nothing could name is unnameable, not matched",
			err:            errUnresolvedExecutable,
			wantMatched:    []Identity{},
			wantUnnameable: []Identity{{PID: 10, Start: 77, Boot: 9}},
		},
		{
			name:           "another user's process is never this daemon",
			err:            errForeignProcess,
			wantMatched:    []Identity{},
			wantUnnameable: []Identity{},
		},
		{
			name:           "a pid the kernel does not hold is gone",
			err:            ErrNoProcess,
			wantMatched:    []Identity{},
			wantUnnameable: []Identity{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report, err := executableIdentities(
				"/Applications/Exact.app/Contents/MacOS/Exact",
				func() ([]int, error) { return []int{10}, nil },
				func(int) (string, error) { return "", test.err },
				func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
			)
			if err != nil {
				t.Fatalf("executableIdentities() = %v", err)
			}
			if !slices.Equal(report.Matched, test.wantMatched) {
				t.Fatalf("Matched = %+v, want %+v", report.Matched, test.wantMatched)
			}
			if !slices.Equal(report.Unnameable, test.wantUnnameable) {
				t.Fatalf("Unnameable = %+v, want %+v", report.Unnameable, test.wantUnnameable)
			}
		})
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

// TestExecutableIdentitiesKeepsAProcessUnnamedMidInventory holds the
// revalidation bracket to the same rule: an executable that stops resolving
// between the bracketing reads leaves the process counted, and the identity
// repin stays the anti-PID-reuse authority.
func TestExecutableIdentitiesKeepsAProcessUnnamedMidInventory(t *testing.T) {
	path := "/Applications/Exact.app/Contents/MacOS/Exact"
	reads := 0
	report, err := executableIdentities(
		path,
		func() ([]int, error) { return []int{10}, nil },
		func(int) (string, error) {
			reads++
			if reads == 1 {
				return path, nil
			}
			return "", errUnresolvedExecutable
		},
		func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
	)
	if err != nil {
		t.Fatalf("executableIdentities() = %v", err)
	}
	want := []Identity{{PID: 10, Start: 77, Boot: 9, Executable: path}}
	if !slices.Equal(report.Matched, want) {
		t.Fatalf("Matched = %+v, want %+v", report.Matched, want)
	}
}

// TestExecutableIdentitiesMatchesTheQueryInBothForms pins both sides of the
// comparison in the same form: a consumer that names its program through a
// symlink must still match the symlink-free path a process runs under, or the
// gate reports a clear inventory for a daemon that is running.
func TestExecutableIdentitiesMatchesTheQueryInBothForms(t *testing.T) {
	dir := t.TempDir()
	program := filepath.Join(dir, "program")
	if err := os.WriteFile(program, []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(program, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(program)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		query string
	}{
		{name: "queried through the symlink", query: link},
		{name: "queried as the caller wrote it", query: program},
		{name: "queried in the resolved form", query: resolved},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report, err := executableIdentities(
				test.query,
				func() ([]int, error) { return []int{10}, nil },
				func(int) (string, error) { return resolved, nil },
				func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
			)
			if err != nil {
				t.Fatalf("executableIdentities() = %v", err)
			}
			want := []Identity{{PID: 10, Start: 77, Boot: 9, Executable: resolved}}
			if !slices.Equal(report.Matched, want) {
				t.Fatalf("Matched = %+v, want %+v", report.Matched, want)
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
