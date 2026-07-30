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
