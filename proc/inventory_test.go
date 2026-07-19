package proc

import "testing"

func TestExecutableIdentitiesExactAbsentPath(t *testing.T) {
	identities, err := ExecutableIdentities("/daemonkit-test/definitely-not-an-executable")
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 0 {
		t.Fatalf("identities = %v, want none", identities)
	}
}
