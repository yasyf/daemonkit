package proc

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestExecutableIdentitiesExactAbsentPath(t *testing.T) {
	identities, err := executableIdentities(
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
	if len(identities) != 0 {
		t.Fatalf("identities = %v, want none", identities)
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

func TestExecutableIdentitiesDropsReusedPID(t *testing.T) {
	path := "/Applications/Exact.app/Contents/MacOS/Exact"
	probes := 0
	identities, err := executableIdentities(
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
	if len(identities) != 0 {
		t.Fatalf("identities = %+v, want none: the pin named a dead instance", identities)
	}
}

func TestExecutableIdentitiesKeepsStableInstance(t *testing.T) {
	path := "/Applications/Exact.app/Contents/MacOS/Exact"
	identities, err := executableIdentities(
		path,
		func() ([]int, error) { return []int{10}, nil },
		func(int) (string, error) { return path, nil },
		func(pid int) (Identity, error) { return Identity{PID: pid, Start: 77, Boot: 9}, nil },
	)
	if err != nil {
		t.Fatalf("executableIdentities() = %v", err)
	}
	want := []Identity{{PID: 10, Start: 77, Boot: 9, Executable: path}}
	if len(identities) != 1 || identities[0] != want[0] {
		t.Fatalf("identities = %+v, want %+v", identities, want)
	}
}

func TestExecutableIdentitiesFindsSelf(t *testing.T) {
	self, err := ExecutablePath(os.Getpid())
	if err != nil {
		t.Fatalf("ExecutablePath(self) = %v", err)
	}
	identities, err := ExecutableIdentities(self)
	if err != nil {
		t.Fatalf("ExecutableIdentities(%q) = %v", self, err)
	}
	for _, identity := range identities {
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
	t.Fatalf("identities = %v, want pid %d", identities, os.Getpid())
}
