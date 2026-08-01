package proc

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/state"
)

func storeHolds(t *testing.T, path string, id identity) bool {
	t.Helper()
	loaded, err := state.New[records](path, recordSchema).Load()
	if err != nil {
		t.Fatalf("raw load %s: %v", path, err)
	}
	return holds(loaded.Value, id)
}

func TestAddIsObservedByPostWriteReRead(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation, Comm: "child"}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	if !storeHolds(t, path, rec.id()) {
		t.Fatal("added record absent from the file")
	}
}

func TestRetireByIdentityCoreSurvivesCosmeticDrift(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation, Comm: "before"}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	file := state.New[records](path, recordSchema)
	loaded, err := file.Load()
	if err != nil {
		t.Fatalf("raw load: %v", err)
	}
	loaded.Value.Live[0].Comm = "drifted-between-generations"
	if err := file.Store(loaded.Value); err != nil {
		t.Fatalf("raw store: %v", err)
	}

	fate := <-s.retire(rec.id())
	if fate != RecordRemoved {
		t.Fatalf("retire() = %d, want RecordRemoved despite cosmetic drift", fate)
	}
	if storeHolds(t, path, rec.id()) {
		t.Fatal("retired record still in the file")
	}
}

func TestRetireAbsentIdentityObservesRemoval(t *testing.T) {
	s, _ := newTestStore(t)
	fate := <-s.retire(identity{pid: 999, start: 9, boot: testBoot})
	if fate != RecordRemoved {
		t.Fatalf("retire() = %d, want RecordRemoved from observed absence", fate)
	}
}

func TestRetireStoreFailureIsAbandonedNeverClaimed(t *testing.T) {
	s, path := newTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatalf("add() = %v", err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	select {
	case fate := <-s.retire(rec.id()):
		if fate != RecordAbandoned {
			t.Fatalf("retire() = %d, want RecordAbandoned on a wedged store", fate)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retire() never answered")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if !storeHolds(t, path, rec.id()) {
		t.Fatal("abandoned record is not still durable for the next open")
	}
}

func TestOpenStoreArchivesUnknownSchemaAndRecoverReapsItsCores(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.dkstate")
	future := state.New[records](path, recordSchema+41)
	err := future.Store(records{Live: []record{
		{PID: 60000, Start: 77, Boot: testBoot + 1, Generation: 12345},
	}})
	if err != nil {
		t.Fatalf("store future frame: %v", err)
	}

	s := openTestStore(t, path)
	prober := &funcProber{probeFn: func(int) (procInfo, error) {
		t.Fatal("cross-boot core was probed")
		return procInfo{}, nil
	}}
	s.prober = prober

	reclaimed, archived, err := s.Recover(ladderContext(t, time.Second), nil)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if archived == "" {
		t.Fatal("Recover() named no archive for the unknown-schema file")
	}
	if len(reclaimed) != 1 || reclaimed[0].PID != 60000 {
		t.Fatalf("Recover() = %v, want the archived era's core reclaimed", reclaimed)
	}
	if reclaimed[0].Exit.Reap != ReapCrossBoot {
		t.Fatalf("Exit.Reap = %d, want ReapCrossBoot", reclaimed[0].Exit.Reap)
	}
}

func TestReopenBeforeRecoverStillReclaimsArchivedCores(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.dkstate")
	future := state.New[records](path, recordSchema+41)
	err := future.Store(records{Live: []record{
		{PID: 60000, Start: 77, Boot: testBoot + 1, Generation: 12345},
	}})
	if err != nil {
		t.Fatalf("store future frame: %v", err)
	}

	first, err := OpenStore(ladderContext(t, 5*time.Second), path)
	if err != nil {
		t.Fatalf("first OpenStore() = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() = %v", err)
	}

	second := openTestStore(t, path)
	second.prober = &funcProber{probeFn: func(int) (procInfo, error) { return procInfo{}, errNoProc }}

	reclaimed, _, err := second.Recover(ladderContext(t, time.Second), nil)
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].PID != 60000 {
		t.Fatalf("reopen after archive reclaimed %v, want the archived era's core", reclaimed)
	}
}

func TestArchivedSessionCoreRecoversItsSessionMembers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "records.dkstate")
	future := state.New[records](path, recordSchema+41)
	err := future.Store(records{Live: []record{
		{PID: 60000, Start: 77, Boot: testBoot, Generation: 12345, Session: 60000},
	}})
	if err != nil {
		t.Fatalf("store future frame: %v", err)
	}

	s := openTestStore(t, path)

	enumerated := false
	s.prober = &funcProber{
		probeFn:   func(int) (procInfo, error) { return procInfo{}, errNoProc },
		membersFn: func(int) ([]groupMember, error) { enumerated = true; return nil, nil },
	}

	if _, _, err := s.Recover(ladderContext(t, time.Second), nil); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if !enumerated {
		t.Fatal("archived dedicated-session core recovered without enumerating its surviving descendants")
	}
}
