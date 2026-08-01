package trust

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

// TestProcessTokenAnswersForASuspendedChild is the F1b premise: the kernel
// establishes the CodeDirectory at exec, before the entry point, so the audit
// token — and every csops read judged against it — answers for a child that
// has not executed an instruction.
func TestProcessTokenAnswersForASuspendedChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := proc.OpenStore(ctx, filepath.Join(t.TempDir(), "records.dkstate"))
	if err != nil {
		t.Fatalf("OpenStore() = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var (
		token  proc.AuditToken
		gated  int
		verify error
	)
	refused := errors.New("gate closed after the read")
	_, err = store.Spawn(ctx, proc.Cmd{
		Path: "/bin/sleep",
		Args: []string{"600"},
		Verify: func(pid int) error {
			gated = pid
			token, verify = ProcessToken(pid)
			return refused
		},
	}, nil)
	if !errors.Is(err, refused) {
		t.Fatalf("Spawn() = %v, want the gate's refusal", err)
	}
	if verify != nil {
		t.Fatalf("ProcessToken(suspended pid %d) = %v", gated, verify)
	}
	if token.PID() != gated {
		t.Fatalf("token PID = %d, want the suspended child %d", token.PID(), gated)
	}
	if !token.Valid() {
		t.Fatalf("token %+v carries no usable execution identity", token)
	}
}

// TestVerifyProcessDeniesAPlatformBinaryCleanly upholds the ordering carried
// over from the socket lane: the status and category checks run before the
// op-answering reads, so a child that is signed but not Developer ID lands on
// a policy denial rather than on ErrNoVerifier.
func TestVerifyProcessDeniesAPlatformBinaryCleanly(t *testing.T) {
	err := VerifyProcess(os.Getpid(), Requirement{
		TeamID:            "SXKCTF23Q2",
		SigningIdentifier: "com.yasyf.daemonkit.not-this-binary",
	})
	if !errors.Is(err, ErrUntrustedPeer) {
		t.Fatalf("VerifyProcess() = %v, want ErrUntrustedPeer", err)
	}
}

func TestVerifyProcessReportsAGonePeerApart(t *testing.T) {
	child, err := os.StartProcess("/bin/sleep", []string{"/bin/sleep", "0"}, &os.ProcAttr{})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if _, err := child.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := ProcessToken(child.Pid)
		if errors.Is(err, ErrPeerGone) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ProcessToken(reaped pid %d) = %v, want ErrPeerGone", child.Pid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
