package proc

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func runContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestRunDeliversStdinAndCollectsOutput(t *testing.T) {
	s, _ := newTestStore(t)
	result, err := s.Run(runContext(t, 10*time.Second), Cmd{
		Path:  "/bin/cat",
		Stdin: []byte("hello over the pipe"),
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if !bytes.Equal(result.Stdout, []byte("hello over the pipe")) {
		t.Fatalf("Stdout = %q", result.Stdout)
	}
	if result.Truncated {
		t.Fatal("Truncated = true for an unbounded stream")
	}
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	if result.Exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", result.Exit.Record)
	}
}

func TestRunTruncationIsDataNotError(t *testing.T) {
	s, _ := newTestStore(t)
	result, err := s.Run(runContext(t, 10*time.Second), Cmd{
		Path:      "/bin/sh",
		Args:      []string{"-c", "printf 0123456789abcdef"},
		MaxStdout: 8,
	})
	if err != nil {
		t.Fatalf("Run() = %v, want truncation as data", err)
	}
	if !result.Truncated {
		t.Fatal("Truncated = false for an overflowing stream")
	}
	if !bytes.Equal(result.Stdout, []byte("01234567")) {
		t.Fatalf("Stdout = %q, want the bounded prefix", result.Stdout)
	}
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d: truncation tore down the run", result.Exit.Code)
	}
}

func TestRunExpiredDeadlineTerminatesWithinReservedTail(t *testing.T) {
	s, _ := newTestStore(t)
	start := time.Now()
	result, err := s.Run(runContext(t, 2*time.Second), Cmd{
		Path: "/bin/sleep",
		Args: []string{"60"},
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("Run() took %v; settlement starved the deadline", elapsed)
	}
	if result.Exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", result.Exit.Reap)
	}
}

func TestRunRequiresDeadline(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Run(context.Background(), Cmd{Path: "/bin/true"}); err == nil {
		t.Fatal("Run() accepted a context without a deadline")
	}
}

func TestRunDeadlineBoundsDrainHeldByDescendant(t *testing.T) {
	s, _ := newTestStore(t)
	start := time.Now()
	result, err := s.Run(runContext(t, 1*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "sleep 3 & exit 0"},
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Run took %v with a 1s deadline: it blocked on stdout EOF held by an unowned 3s descendant", elapsed)
	}
	if !result.Truncated {
		t.Error("Truncated = false for a drain severed at the deadline")
	}
	if result.Exit.Code != 0 {
		t.Errorf("Exit.Code = %d, want 0", result.Exit.Code)
	}
}

func TestRunRejectsNegativeOutputLimit(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Run(runContext(t, time.Second), Cmd{Path: "/bin/true", MaxStdout: -1}); err == nil {
		t.Fatal("Run() accepted a negative MaxStdout")
	}
	if _, err := s.Run(runContext(t, time.Second), Cmd{Path: "/bin/true", MaxStderr: -1}); err == nil {
		t.Fatal("Run() accepted a negative MaxStderr")
	}
}

func TestRunStdinReaderClosedEarlyIsNotAFailure(t *testing.T) {
	s, _ := newTestStore(t)
	result, err := s.Run(runContext(t, 5*time.Second), Cmd{
		Path:  "/bin/sh",
		Args:  []string{"-c", "exec 0<&-; sleep 0.05"},
		Stdin: bytes.Repeat([]byte("x"), 1<<20),
	})
	if err != nil {
		t.Fatalf("Run() = %v; a child closing stdin early is the child's prerogative, not a delivery failure", err)
	}
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	result, err = s.Run(runContext(t, 5*time.Second), Cmd{Path: "/bin/cat", Stdin: []byte("ok")})
	if err != nil {
		t.Fatalf("second Run() = %v; the early close poisoned the store", err)
	}
	if !bytes.Equal(result.Stdout, []byte("ok")) {
		t.Fatalf("second Run() Stdout = %q, want %q", result.Stdout, "ok")
	}
}

func TestRunOutputCapTerminatesChildPromptly(t *testing.T) {
	s, _ := newTestStore(t)
	start := time.Now()
	result, err := s.Run(runContext(t, 10*time.Second), Cmd{
		Path:      "/bin/sh",
		Args:      []string{"-c", "while :; do printf 0123456789; done"},
		MaxStdout: 4,
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Run() took %v; the cap drained to the deadline instead of terminating the child", elapsed)
	}
	if !result.Truncated {
		t.Fatal("Truncated = false for a capped stream")
	}
	if !bytes.Equal(result.Stdout, []byte("0123")) {
		t.Fatalf("Stdout = %q, want the 4-byte prefix", result.Stdout)
	}
	if result.Exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", result.Exit.Reap)
	}
}

func TestRunRejectsInexactPathBeforeSpawn(t *testing.T) {
	tests := []struct {
		name string
		cmd  Cmd
	}{
		{"relative path", Cmd{Path: "bin/echo"}},
		{"non-clean path", Cmd{Path: "/bin/../bin/echo"}},
		{"relative dir", Cmd{Path: "/bin/echo", Dir: "rel"}},
		{"non-clean dir", Cmd{Path: "/bin/echo", Dir: "/tmp/../tmp"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestStore(t)
			spawned := false
			s.beforeRelease = func(int) { spawned = true }
			if _, err := s.Run(runContext(t, time.Second), tt.cmd); err == nil {
				t.Fatalf("Run(%+v) accepted an inexact path", tt.cmd)
			}
			if spawned {
				t.Fatal("Run() spawned a child before rejecting the path")
			}
		})
	}
}
