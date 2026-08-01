package proc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func runContext(t testing.TB, d time.Duration) context.Context {
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
	}, unheld)
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
		MaxOutput: 8,
	}, unheld)
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
	}, unheld)
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
	if _, err := s.Run(context.Background(), Cmd{Path: "/usr/bin/true"}, unheld); err == nil {
		t.Fatal("Run() accepted a context without a deadline")
	}
}

// TestRunSettlesADescendantHoldingItsPipe is what the dedicated session buys
// the drain. A child that exits leaving a fork on the run's stdout used to
// hold the pipe open to the deadline and hand back a truncated stream over an
// unowned survivor; the fork is inside the run's own session now, so it
// settles with the leader and the whole stream arrives.
func TestRunSettlesADescendantHoldingItsPipe(t *testing.T) {
	s, _ := newTestStore(t)
	dir := t.TempDir()
	holderPath := filepath.Join(dir, "holder.pid")
	t.Cleanup(func() {
		if pid := publishedPID(holderPath); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	var leader int
	start := time.Now()
	result, err := s.Run(runContext(t, 20*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", "sleep 600 & " + publishPID(holderPath, "$!") + "exit 0"},
	}, func(c *Child) {
		leader = c.PID()
	})
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Run took %v: the descendant on the stdout pipe was never settled with the session", elapsed)
	}
	if result.Truncated {
		t.Error("Truncated = true though the descendant holding the drain was settled")
	}
	if result.Exit.Code != 0 {
		t.Errorf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	if publishedPID(holderPath) == 0 {
		t.Fatal("the descendant never published its pid, so this run proved nothing")
	}
	members, err := probeGroupMembers(leader)
	if err != nil {
		t.Fatalf("enumerate the run's session: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("the run's session still has members after Run returned: %v", members)
	}
}

// TestRunDeadlineBoundsDrainHeldByAnEscapedDescendant is the one drain the
// session cannot close, and the reason severing still exists: a descendant
// that setsid()s out leaves the only scope macOS offers, keeps the stdout
// pipe, and is neither signalled nor counted — so the run severs at its
// deadline and says the stream is not whole. macOS ships no setsid(1) and the
// shell has no builtin, so perl's POSIX binding is the seam.
func TestRunDeadlineBoundsDrainHeldByAnEscapedDescendant(t *testing.T) {
	s, _ := newTestStore(t)
	dir := t.TempDir()
	escapeePath := filepath.Join(dir, "escapee.pid")
	t.Cleanup(func() {
		if pid := publishedPID(escapeePath); pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	escapee := `use POSIX; POSIX::setsid(); open(F, ">", "` + escapeePath + `.tmp"); print F "$$\n"; close F;` +
		` rename("` + escapeePath + `.tmp", "` + escapeePath + `"); sleep 600`
	script := perlBinary + " -e '" + escapee + "' & while [ ! -s " + escapeePath + " ]; do sleep 0.02; done; exit 0"
	start := time.Now()
	result, err := s.Run(runContext(t, 2*time.Second), Cmd{
		Path: "/bin/sh",
		Args: []string{"-c", script},
	}, unheld)
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Run took %v with a 2s deadline: the drain was never severed", elapsed)
	}
	if !result.Truncated {
		t.Error("Truncated = false for a drain severed at the deadline")
	}
	if result.Exit.Code != 0 {
		t.Errorf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	pid := publishedPID(escapeePath)
	if pid == 0 {
		t.Fatal("the escapee never published its pid")
	}
	if syscall.Kill(pid, 0) != nil {
		t.Fatalf("the escapee %d was gone, so it never held the drain and this run proved nothing", pid)
	}
}

// perlBinary is a standalone literal so the module's system-binary guard
// checks it: the shell line it joins carries arguments and is not a path the
// guard can see through.
const perlBinary = "/usr/bin/perl"

func TestRunRejectsNegativeOutputLimit(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Run(runContext(t, time.Second), Cmd{Path: "/usr/bin/true", MaxOutput: -1}, unheld); err == nil {
		t.Fatal("Run() accepted a negative MaxOutput")
	}
}

func TestRunStdinReaderClosedEarlyIsNotAFailure(t *testing.T) {
	s, _ := newTestStore(t)
	result, err := s.Run(runContext(t, 5*time.Second), Cmd{
		Path:  "/bin/sh",
		Args:  []string{"-c", "exec 0<&-; sleep 0.05"},
		Stdin: bytes.Repeat([]byte("x"), 1<<20),
	}, unheld)
	if err != nil {
		t.Fatalf("Run() = %v; a child closing stdin early is the child's prerogative, not a delivery failure", err)
	}
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	result, err = s.Run(runContext(t, 5*time.Second), Cmd{Path: "/bin/cat", Stdin: []byte("ok")}, unheld)
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
		MaxOutput: 4,
	}, unheld)
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
			tt.cmd.Verify = func(int) error { spawned = true; return nil }
			if _, err := s.Run(runContext(t, time.Second), tt.cmd, unheld); err == nil {
				t.Fatalf("Run(%+v) accepted an inexact path", tt.cmd)
			}
			if spawned {
				t.Fatal("Run() spawned a child before rejecting the path")
			}
		})
	}
}

// TestRunDrainBoundedBySettlementNotTheCallersDeadline is the pinned run: an
// owner settling its scope demands the run's child by the owner's own budget,
// and the drain behind that child must be bounded by the same demand rather
// than by the run's far later deadline. A holder the ladder can dislodge frees
// the pipe on its own and proves nothing about the bound, so the signaler here
// records signals and delivers none: the ladder times out unproven with the
// child still holding the run's stdout pipe, and the bound is then the only
// thing that can end the drain.
func TestRunDrainBoundedBySettlementNotTheCallersDeadline(t *testing.T) {
	s, _ := newTestStore(t)
	s.signaler = &funcSignaler{}
	const budget = 60 * time.Second
	held := make(chan *Child, 1)
	returned := make(chan time.Time, 1)
	go func() {
		_, _ = s.Run(runContext(t, budget), Cmd{Path: "/bin/sleep", Args: []string{"600"}}, func(c *Child) {
			held <- c
		})
		returned <- time.Now()
	}()
	child := <-held
	t.Cleanup(func() {
		_ = syscall.Kill(-child.PID(), syscall.SIGKILL)
		_ = syscall.Kill(child.PID(), syscall.SIGKILL)
	})

	settleBy := time.Now().Add(2 * time.Second)
	child.TerminateBy(settleBy)
	if settled := <-child.Done(); settled.Reap != reapUndetermined {
		t.Fatalf("Exit.Reap = %d, want the undetermined terminal a ladder that dislodges nothing publishes", settled.Reap)
	}
	select {
	case at := <-returned:
		if slack := at.Sub(settleBy); slack > SettleGrace {
			t.Errorf("Run returned %v past the settlement it was demanded by; the child still holding the stdout pipe pinned the drain to the run's own %v deadline", slack, budget)
		}
	case <-time.After(2 * SettleGrace):
		t.Fatalf("Run never returned within %v of its child's settlement: the child still holding the stdout pipe pinned the drain to the run's own %v deadline", 2*SettleGrace, budget)
	}
}

func publishPID(path, pid string) string {
	return "echo " + pid + " > " + path + ".tmp; mv " + path + ".tmp " + path + "; "
}

func publishedPID(path string) int {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0
	}
	return pid
}

// BenchmarkRunAllocations is the floor D14's raised cap must not have raised
// with it: the limit bounds what a run may retain, not what it allocates. The
// silent command measures the floor; the at-cap command measures the ceiling,
// and it is the one a /usr/bin/true benchmark cannot see, because the growth
// ladder only runs when a stream actually fills.
func BenchmarkRunAllocations(b *testing.B) {
	benchmarks := []struct {
		name string
		cmd  Cmd
	}{
		{"no output", Cmd{Path: "/usr/bin/true"}},
		{"at cap", Cmd{
			Path: "/usr/bin/head",
			Args: []string{"-c", strconv.Itoa(int(defaultRunLimit)), "/dev/zero"},
		}},
	}
	for _, bb := range benchmarks {
		b.Run(bb.name, func(b *testing.B) {
			s, _ := newTestStore(b)
			for b.Loop() {
				if _, err := s.Run(runContext(b, 30*time.Second), bb.cmd, unheld); err != nil {
					b.Fatalf("Run() = %v", err)
				}
			}
		})
	}
}

// TestRunRetainsExactlyTheCap is the pair the allocation shape must not move:
// an overproducer stops at the cap and says so, an exact-fit producer fills it
// and does not.
func TestRunRetainsExactlyTheCap(t *testing.T) {
	tests := []struct {
		name          string
		produce       int
		maxOutput     Bytes
		want          int
		wantTruncated bool
	}{
		{"overproducer stops at the default cap", 8 << 20, 0, int(defaultRunLimit), true},
		{"exact fit under a stated cap", 102400, 102400, 102400, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestStore(t)
			result, _ := s.Run(runContext(t, 30*time.Second), Cmd{
				Path:      "/usr/bin/head",
				Args:      []string{"-c", strconv.Itoa(tt.produce), "/dev/zero"},
				MaxOutput: tt.maxOutput,
			}, unheld)
			if len(result.Stdout) != tt.want {
				t.Errorf("len(Stdout) = %d, want %d", len(result.Stdout), tt.want)
			}
			if result.Truncated != tt.wantTruncated {
				t.Errorf("Truncated = %v, want %v", result.Truncated, tt.wantTruncated)
			}
		})
	}
}
