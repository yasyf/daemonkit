package proc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTerminateDemandWinsAgainstFreshSpawn(t *testing.T) {
	s, _ := newTestStore(t)
	child, err := s.Spawn(Cmd{Path: "/bin/sleep", Args: []string{"60"}})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	child.Terminate()
	child.Terminate()
	var exit Exit
	select {
	case exit = <-child.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("terminated child never settled")
	}
	if exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", exit.Reap)
	}
	if exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", exit.Record)
	}
	loaded := s.snapshot()
	if len(loaded) != 0 {
		t.Fatalf("terminated child left records: %v", loaded)
	}
}

func TestRetireFailureIsAbandonedAndDoneStillCloses(t *testing.T) {
	s, path := newTestStore(t)
	child, err := s.Spawn(Cmd{Path: "/bin/sleep", Args: []string{"60"}})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	child.Terminate()
	var exit Exit
	select {
	case exit = <-child.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done never closed while the store was wedged")
	}
	if exit.Record != RecordAbandoned {
		t.Fatalf("Exit.Record = %d, want RecordAbandoned on a wedged store", exit.Record)
	}
	if exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated", exit.Reap)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	found := false
	for _, rec := range s.snapshot() {
		if rec.PID == child.PID() {
			found = true
		}
	}
	if !found {
		t.Fatal("abandoned record is gone; the next open has nothing to reclaim")
	}
}

func TestSessionLeaderExitSettlesDescendants(t *testing.T) {
	s, path := newTestStore(t)
	marker := filepath.Join(t.TempDir(), "grandchild")
	script := "sleep 60 & echo $! > " + marker + "; exit 0"
	child, err := s.Spawn(Cmd{Path: "/bin/sh", Args: []string{"-c", script}, Session: true})
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	var exit Exit
	select {
	case exit = <-child.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("session leader never settled")
	}
	if exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", exit.Code)
	}
	if exit.Reap != ReapTerminated {
		t.Fatalf("Exit.Reap = %d, want ReapTerminated after killing the survivor", exit.Reap)
	}
	if exit.Record != RecordRemoved {
		t.Fatalf("Exit.Record = %d, want RecordRemoved", exit.Record)
	}
	members, err := probeGroupMembers(child.PID())
	if err != nil {
		t.Fatalf("enumerate session: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("dedicated session still has members: %v", members)
	}
	if storeHolds(t, path, identity{pid: child.PID()}) {
		t.Fatal("session record survived settlement")
	}
}

func TestTerminateChildReadyExitWinsBeforeAnySignal(t *testing.T) {
	s, _ := newTestStore(t)
	sig := &funcSignaler{}
	s.signaler = sig

	exited := make(chan int, 1)
	exited <- 7

	code, reap := s.terminateChild(999999, time.Now().Add(time.Second), exited, realClock{})
	if code != 7 {
		t.Fatalf("terminateChild() = %d, want the buffered exit code 7", code)
	}
	if reap != ReapAbsent {
		t.Fatalf("terminateChild() reap = %d, want ReapAbsent for a natural exit", reap)
	}
	if got := sig.signals(); len(got) != 0 {
		t.Fatalf("terminateChild signaled an already-reaped PID: %v", got)
	}
}

func TestTerminateChildBoundsPostKillWaitByDeadline(t *testing.T) {
	s, _ := newTestStore(t)
	s.signaler = &funcSignaler{}

	neverExits := make(chan int, 1)
	type terminal struct {
		code int
		reap Reap
	}
	done := make(chan terminal, 1)
	go func() {
		code, reap := s.terminateChild(999999, time.Now().Add(50*time.Millisecond), neverExits, realClock{})
		done <- terminal{code: code, reap: reap}
	}()

	select {
	case got := <-done:
		if got.reap != reapUndetermined {
			t.Errorf("terminateChild() reap = %d, want undetermined for a child that never left the table", got.reap)
		}
		if got.code != -1 {
			t.Errorf("terminateChild() code = %d, want -1", got.code)
		}
	case <-time.After(750 * time.Millisecond):
		t.Errorf("terminateChild did not return within 750ms though its deadline was 50ms")
	}
	neverExits <- -1
}

func TestZeroOutcomesAreUnpublishable(t *testing.T) {
	var exit Exit
	if exit.Reap != reapUndetermined {
		t.Fatal("zero Reap is not the undetermined value")
	}
	if exit.Record != recordInvalid {
		t.Fatal("zero RecordFate is not the invalid value")
	}
}

func TestRecoverRequiresDeadline(t *testing.T) {
	s, _ := newTestStore(t)
	if _, _, err := s.Recover(context.Background(), nil); err == nil {
		t.Fatal("Recover() accepted a context without a deadline")
	}
}
