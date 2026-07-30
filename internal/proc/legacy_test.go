package proc

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestRecoverSweepsLegacyBboltStore(t *testing.T) {
	fixture := filepath.Join("testdata", "legacy_"+runtime.GOOS+".db")
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	legacy := filepath.Join(t.TempDir(), "legacy.db")
	if err := os.WriteFile(legacy, raw, 0o600); err != nil {
		t.Fatalf("stage fixture: %v", err)
	}
	s, _ := newTestStore(t)

	reclaimed, archived, err := s.Recover(ladderContext(t, 2*time.Second), []string{legacy})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	pids := make([]int, 0, len(reclaimed))
	for _, r := range reclaimed {
		pids = append(pids, r.PID)
		if r.Exit.Reap != ReapCrossBoot {
			t.Fatalf("pid %d Exit.Reap = %d, want ReapCrossBoot", r.PID, r.Exit.Reap)
		}
		if r.Exit.Record != RecordRemoved {
			t.Fatalf("pid %d Exit.Record = %d, want RecordRemoved", r.PID, r.Exit.Record)
		}
	}
	slices.Sort(pids)
	if !slices.Equal(pids, []int{54321, 54322}) {
		t.Fatalf("Recover() reclaimed pids %v, want [54321 54322]", pids)
	}
	if archived == "" {
		t.Fatal("Recover() reported no archive path for the swept store")
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy store still present after sweep: %v", err)
	}

	again, archivedAgain, err := s.Recover(ladderContext(t, time.Second), []string{legacy})
	if err != nil {
		t.Fatalf("second Recover() error = %v", err)
	}
	if len(again) != 0 || archivedAgain != "" {
		t.Fatalf("second Recover() = %v, %q, want a no-op", again, archivedAgain)
	}
}

func TestLegacyReadFailsFastOnLockedStoreUnderExpiredDeadline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.db")
	holder, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open exclusive holder: %v", err)
	}
	defer holder.Close()

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := readLegacyIdentities(expired, path, realClock{})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("readLegacyIdentities succeeded on a locked db under an expired deadline; want a fast timeout error")
		}
	case <-time.After(1 * time.Second):
		t.Error("readLegacyIdentities blocked past its expired deadline instead of failing fast")
	}
	_ = holder.Close()
}
