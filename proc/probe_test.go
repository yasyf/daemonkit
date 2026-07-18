package proc

import (
	"errors"
	"os"
	"testing"
)

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

func TestProbeGone(t *testing.T) {
	const absent = 2_000_000
	if _, err := Probe(absent); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("Probe(%d) err = %v, want ErrNoProcess", absent, err)
	}
}
