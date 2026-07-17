package proc

import (
	"errors"
	"os"
	"testing"
)

// TestProbeSelf: the running process resolves to a non-empty start stamp and
// comm under its own pid.
func TestProbeSelf(t *testing.T) {
	id, err := Probe(os.Getpid())
	if err != nil {
		t.Fatalf("Probe(self): %v", err)
	}
	if id.PID != os.Getpid() {
		t.Errorf("PID = %d, want %d", id.PID, os.Getpid())
	}
	if id.StartTime == "" {
		t.Error("StartTime empty, want a platform-native start stamp")
	}
	if id.Comm == "" {
		t.Error("Comm empty, want the process name")
	}
}

// TestProbeGone: a pid past the platform max resolves to ErrNoProcess, the
// definitive "gone" the SIGKILL-authority caller branches on.
func TestProbeGone(t *testing.T) {
	const absent = 2_000_000 // above darwin's default pid_max and any live linux pid
	if _, err := Probe(absent); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("Probe(%d) err = %v, want ErrNoProcess", absent, err)
	}
}
