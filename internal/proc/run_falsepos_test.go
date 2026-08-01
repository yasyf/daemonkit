package proc

import (
	"bytes"
	"errors"
	"syscall"
	"testing"
	"time"
)

// TestRunPartialStdinConsumerSucceeds is the Hunt-1 refutation witness: a
// well-behaved child that reads part of a large stdin and exits 0 (head -c)
// must NOT be reported as a failed run.
func TestRunPartialStdinConsumerSucceeds(t *testing.T) {
	s, _ := newTestStore(t)
	result, err := s.Run(runContext(t, 10*time.Second), Cmd{
		Path:  "/usr/bin/head",
		Args:  []string{"-c", "100"},
		Stdin: bytes.Repeat([]byte("x"), 1<<20),
	}, unheld)
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0 (head -c exits cleanly)", result.Exit.Code)
	}
	if err != nil {
		t.Fatalf("Run() = %v (EPIPE? %v); a well-behaved partial-stdin consumer was reported as failed", err, errors.Is(err, syscall.EPIPE))
	}
}
