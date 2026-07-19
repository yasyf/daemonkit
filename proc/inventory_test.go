package proc

import (
	"errors"
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
