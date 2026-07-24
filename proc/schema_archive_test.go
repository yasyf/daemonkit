package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func wedgeSchemaFingerprint(t *testing.T, path string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(fileStoreMetaBucket).Put(fileStoreFingerprintKey, []byte("foreign"))
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func storeBackups(t *testing.T, path string) []string {
	t.Helper()
	matches, err := filepath.Glob(path + ".*.bak")
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

func TestFileStoreFailsClosedOnUnsupportedSchemaByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workers.db")
	seed := &FileStore{Path: path}
	if err := seed.Add(t.Context(), storeRecord(RecoveryTask, 42)); err != nil {
		t.Fatal(err)
	}
	wedgeSchemaFingerprint(t, path)

	store := &FileStore{Path: path}
	if _, err := store.Load(t.Context()); !errors.Is(err, ErrRecordSchema) {
		t.Fatalf("Load error = %v, want ErrRecordSchema", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fail-closed must preserve the store: %v", err)
	}
	if bak := storeBackups(t, path); len(bak) != 0 {
		t.Fatalf("fail-closed must not archive; found %v", bak)
	}
}

func TestFileStoreArchivesUnsupportedSchemaWhenOptedIn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workers.db")
	seed := &FileStore{Path: path}
	if err := seed.Add(t.Context(), storeRecord(RecoveryTask, 42)); err != nil {
		t.Fatal(err)
	}
	wedgeSchemaFingerprint(t, path)

	store := &FileStore{Path: path, UnsupportedSchema: ArchiveUnsupportedSchema}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatalf("Load after archive = %v, want nil", err)
	}
	if len(records) != 0 {
		t.Fatalf("fresh store after archive holds %d records, want 0", len(records))
	}
	if bak := storeBackups(t, path); len(bak) != 1 {
		t.Fatalf("archive must leave exactly one .bak, found %v", bak)
	}
	if err := store.Add(t.Context(), storeRecord(RecoveryTask, 7)); err != nil {
		t.Fatalf("Add to fresh store = %v", err)
	}
}

func TestFileStoreArchiveIgnoresTransientOpenFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workers.db")
	seed := &FileStore{Path: path}
	if err := seed.Add(t.Context(), storeRecord(RecoveryTask, 42)); err != nil {
		t.Fatal(err)
	}
	wedgeSchemaFingerprint(t, path)

	// Hold the bolt file lock so a concurrent open blocks and times out rather
	// than reaching the schema check.
	held, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	store := &FileStore{Path: path, UnsupportedSchema: ArchiveUnsupportedSchema}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if db, err := store.open(ctx); err == nil {
		_ = db.Close()
		t.Fatal("open of a locked store succeeded, want a timeout error")
	}
	if bak := storeBackups(t, path); len(bak) != 0 {
		t.Fatalf("a transient open failure must not archive; found %v", bak)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("store must be preserved on a transient failure: %v", err)
	}
}

func TestFileStoreArchiveIsSingleFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workers.db")
	seed := &FileStore{Path: path}
	if err := seed.Add(t.Context(), storeRecord(RecoveryTask, 42)); err != nil {
		t.Fatal(err)
	}
	wedgeSchemaFingerprint(t, path)

	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			records, err := (&FileStore{Path: path, UnsupportedSchema: ArchiveUnsupportedSchema}).Load(t.Context())
			if err == nil && len(records) != 0 {
				err = fmt.Errorf("fresh store returned %d records, want 0", len(records))
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent opener %d: %v", i, err)
		}
	}
	if bak := storeBackups(t, path); len(bak) != 1 {
		t.Fatalf("concurrent archive must leave exactly one .bak, found %d: %v", len(bak), bak)
	}
}

func TestArchiveUnsupportedStoreRenamesAside(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workers.db")
	if err := os.WriteFile(path, []byte("wedged"), 0o600); err != nil {
		t.Fatal(err)
	}
	backup, err := ArchiveUnsupportedStore(path)
	if err != nil {
		t.Fatalf("ArchiveUnsupportedStore = %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original must be renamed away, stat = %v", err)
	}
	base := filepath.Base(backup)
	if filepath.Dir(backup) != dir || !strings.HasPrefix(base, "workers.db.") || !strings.HasSuffix(base, ".bak") {
		t.Fatalf("backup path = %q, want workers.db.<fp>.<ts>.bak in %q", backup, dir)
	}
	if data, err := os.ReadFile(backup); err != nil || string(data) != "wedged" {
		t.Fatalf("backup contents = %q, %v; want %q", data, err, "wedged")
	}
}
