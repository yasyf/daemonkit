package daemonkit

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/trust"
)

// TestSpawnVerifiesTheSuspendedChildInPlace is F1b: the exec posture is read
// off the kernel-held code identity the exec established, against a child that
// has not executed an instruction, and a failed verify aborts the spawn
// through the record-failure path rather than killing from inside a void hook.
//
// The denial is a policy denial, not an infrastructure one: neither ErrPeerGone
// (the token could not be minted) nor ErrNoVerifier (the reads could not run)
// is what a platform binary lands on, which is the whole proof that the
// csops reads answered for a suspended, unreleased child.
func TestSpawnVerifiesTheSuspendedChildInPlace(t *testing.T) {
	owned := ownedScope(t)
	marker := filepath.Join(t.TempDir(), "ran")

	_, err := owned.Spawn(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "touch " + marker},
		Exec: ServingSigned(Requirement{
			TeamID:            "SXKCTF23Q2",
			SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
		}),
	}, ChannelNone, nil)
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Spawn() = %v, want ErrUntrusted for a child that cannot prove the pinned identity", err)
	}
	if errors.Is(err, trust.ErrPeerGone) {
		t.Fatalf("Spawn() = %v: the audit token could not be minted for the suspended child", err)
	}
	if errors.Is(err, trust.ErrNoVerifier) {
		t.Fatalf("Spawn() = %v: the code-identity reads could not run against the suspended child", err)
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("the refused child executed an instruction: %v", statErr)
	}
}

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

func TestRunCarriesTheSameExecGate(t *testing.T) {
	owned := ownedScope(t)
	_, err := owned.Run(bounded(t, 20*time.Second), Cmd{
		Path: "/bin/echo",
		Args: []string{"never"},
		Exec: ServingSigned(Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.not-this-binary"}),
	})
	if !errors.Is(err, ErrUntrusted) {
		t.Fatalf("Run() = %v, want ErrUntrusted", err)
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
