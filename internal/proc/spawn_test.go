package proc

import (
	"os"
	"testing"
	"time"
)

func TestRecoverSettlesSuspendedChildWithDurableRecord(t *testing.T) {
	s, path := newTestStore(t)
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := startChild(
		Cmd{Path: "/bin/sleep", Args: []string{"60"}},
		spawnFiles{stdin: devNull, stdout: devNull, stderr: devNull},
	)
	_ = devNull.Close()
	if err != nil {
		t.Fatalf("startChild() = %v", err)
	}
	t.Cleanup(func() { awaitExit(pid) })
	boot, err := s.prober.boot()
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.prober.probe(pid)
	if err != nil {
		t.Fatal(err)
	}
	if !info.stopped {
		t.Fatal("spawned child is not suspended")
	}
	rec := record{PID: pid, Start: info.start, Boot: boot, Generation: s.generation}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	next := openTestStore(t, path)
	reclaimed, _, err := next.Recover(ladderContext(t, 1500*time.Millisecond))
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].PID != pid {
		t.Fatalf("Recover() = %v, want the suspended child reclaimed", reclaimed)
	}
	if reclaimed[0].Exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated (SIGKILL reaches SSTOP)", reclaimed[0].Exit.Reap)
	}
	if reclaimed[0].Exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", reclaimed[0].Exit.Record)
	}
	if storeHolds(t, path, rec.id()) {
		t.Fatal("settled record survived recovery")
	}
}
