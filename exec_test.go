package daemonkit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSpawnSameUserWaiverAdmitsAnUnsignedTarget is the other half: the named
// waiver is what a Python interpreter or a platform binary takes, and it runs
// the child that ServingSigned refused.
func TestSpawnSameUserWaiverAdmitsAnUnsignedTarget(t *testing.T) {
	owned := ownedScope(t)
	marker := filepath.Join(t.TempDir(), "ran")

	child, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "touch " + marker},
		Exec: ServingSameUser(),
	}, ChannelNone, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	exit := <-child.Done()
	if exit.Code != 0 {
		t.Fatalf("Exit = %+v, want a clean exit", exit)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the admitted child never ran: %v", statErr)
	}
}

// TestServingIsAClosedSumOfTwoConstructors upholds §4: the dangerous posture is
// not the zero value, because the zero value is not a posture at all.
func TestServingIsAClosedSumOfTwoConstructors(t *testing.T) {
	var unstated Serving
	if unstated.stated() {
		t.Fatal("the zero Serving reads as a stated posture")
	}
	if !ServingSameUser().stated() || !ServingSigned(Requirement{}).stated() {
		t.Fatal("a constructed Serving does not read as stated")
	}
	if ServingSameUser().policy.requirement() != nil {
		t.Fatal("ServingSameUser pins a requirement")
	}
	pinned := Requirement{TeamID: "T", SigningIdentifier: "id"}
	if got := ServingSigned(pinned).policy.requirement(); got == nil || got.Digest() != pinned.Digest() {
		t.Fatalf("ServingSigned(%+v).requirement() = %+v", pinned, got)
	}
}
