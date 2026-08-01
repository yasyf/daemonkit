package proc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestTerminateDemandWinsAgainstFreshSpawn(t *testing.T) {
	s, _ := newTestStore(t)
	child, err := s.Spawn(t.Context(), Cmd{Path: "/bin/sleep", Args: []string{"60"}}, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	child.TerminateBy(time.Now().Add(5 * time.Second))
	child.TerminateBy(time.Now().Add(5 * time.Second))
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
	child, err := s.Spawn(t.Context(), Cmd{Path: "/bin/sleep", Args: []string{"60"}}, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	child.TerminateBy(time.Now().Add(5 * time.Second))
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
	child, err := s.Spawn(t.Context(), Cmd{Path: "/bin/sh", Args: []string{"-c", script}, Session: true}, nil)
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

	exited := make(chan status, 1)
	exited <- status{code: 7}

	terminal, reap := s.terminateChild(999999, time.Now().Add(time.Second), exited, realClock{})
	if terminal.code != 7 {
		t.Fatalf("terminateChild() = %d, want the buffered exit code 7", terminal.code)
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

	neverExits := make(chan status, 1)
	type outcome struct {
		terminal status
		reap     Reap
	}
	done := make(chan outcome, 1)
	go func() {
		terminal, reap := s.terminateChild(999999, time.Now().Add(50*time.Millisecond), neverExits, realClock{})
		done <- outcome{terminal: terminal, reap: reap}
	}()

	select {
	case got := <-done:
		if got.reap != reapUndetermined {
			t.Errorf("terminateChild() reap = %d, want undetermined for a child that never left the table", got.reap)
		}
		if got.terminal.code != -1 {
			t.Errorf("terminateChild() code = %d, want -1", got.terminal.code)
		}
	case <-time.After(750 * time.Millisecond):
		t.Errorf("terminateChild did not return within 750ms though its deadline was 50ms")
	}
	neverExits <- status{code: -1}
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

// TestSessionThatDidNotSettleIsPublishedAsProvenGone: drive tracks the
// dedicated session's fate in sessionSettled but never folds it into Reap, so
// a leader that settled over a session that did not is published as
// ReapTerminated — the value Owned.settle reads as "proven gone".
func TestSessionThatDidNotSettleIsPublishedAsProvenGone(t *testing.T) {
	s, _ := newTestStore(t)
	system := sysProber{}
	blind := errors.New("session enumeration failed")
	s.prober = &funcProber{
		probeFn:   system.probe,
		bootFn:    system.boot,
		membersFn: func(int) ([]groupMember, error) { return nil, blind },
	}
	holderFile := t.TempDir() + "/holder.pid"
	child, err := s.Spawn(t.Context(), Cmd{
		Path:    "/bin/sh",
		Args:    []string{"-c", "sleep 600 & echo $! > " + holderFile + ".tmp; mv " + holderFile + ".tmp " + holderFile + "; exec sleep 600"},
		Session: true,
	}, nil)
	if err != nil {
		t.Fatalf("Spawn() = %v", err)
	}
	leader := child.PID()
	t.Cleanup(func() { _ = syscall.Kill(-leader, syscall.SIGKILL) })

	holder := 0
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		raw, readErr := os.ReadFile(holderFile)
		if readErr == nil {
			holder, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
			if holder > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if holder == 0 {
		t.Fatal("the session survivor never published its pid")
	}
	t.Cleanup(func() { _ = syscall.Kill(holder, syscall.SIGKILL) })

	child.TerminateBy(time.Now().Add(5 * time.Second))
	var exit Exit
	select {
	case exit = <-child.Done():
	case <-time.After(20 * time.Second):
		t.Fatal("the child never settled")
	}
	members, err := sysProber{}.groupMembers(leader)
	if err != nil {
		t.Fatalf("groupMembers() = %v", err)
	}
	holderAlive := syscall.Kill(holder, 0) == nil
	sid, sidErr := unix.Getsid(holder)
	t.Logf("Exit = %+v; enumerated session members = %v; holder %d alive = %t sid = %d (%v)",
		exit, members, holder, holderAlive, sid, sidErr)
	if !holderAlive {
		t.Fatal("the session survivor was gone, so this run proved nothing")
	}
	if exit.Record != RecordAbandoned {
		t.Fatalf("Exit.Record = %d, want RecordAbandoned: the session did not settle", exit.Record)
	}
	if exit.Reap != reapUndetermined {
		t.Fatalf("Exit.Reap = %d over a session settlement that proved nothing; the caller reads that as proven gone", exit.Reap)
	}
}
