package proc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/state"
)

func TestSpawnRecordDurableBeforeFirstInstruction(t *testing.T) {
	s, path := newTestStore(t)
	marker := filepath.Join(t.TempDir(), "ran")
	checked := false
	s.beforeRelease = func(pid int) {
		checked = true
		loaded, err := state.New[records](path, recordSchema).Load()
		if err != nil {
			t.Errorf("raw re-read before release: %v", err)
			return
		}
		found := false
		for _, rec := range loaded.Value.Live {
			if rec.PID == pid && rec.Generation == s.generation && rec.Start != 0 && rec.Boot != 0 {
				found = true
			}
		}
		if !found {
			t.Errorf("no durable record for pid %d before its release; live = %v", pid, loaded.Value.Live)
		}
		info, err := probeProc(pid)
		if err != nil {
			t.Errorf("probe suspended child: %v", err)
			return
		}
		if !info.stopped {
			t.Error("child is not SSTOP before its record's fsync returned")
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Errorf("command side effect exists before release: %v", err)
		}
	}

	child, err := s.Spawn(Cmd{Path: "/bin/sh", Args: []string{"-c", "touch " + marker}})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	if !checked {
		t.Fatal("release hook never ran")
	}
	exit := <-child.Done()
	if exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", exit.Code)
	}
	if exit.Reap != ReapAbsent {
		t.Fatalf("Exit.Reap = %d, want ReapAbsent for a natural exit", exit.Reap)
	}
	if exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", exit.Record)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("released command never ran: %v", err)
	}
}

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
	if err := s.add(rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	next, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore() = %v", err)
	}
	t.Cleanup(func() { _ = next.Close() })
	reclaimed, _, err := next.Recover(ladderContext(t, 1500*time.Millisecond), nil)
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
