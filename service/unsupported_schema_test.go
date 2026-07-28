package service

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
	"github.com/yasyf/daemonkit/proc"
	bolt "go.etcd.io/bbolt"
)

// supersededControllerFingerprint is the store seal daemonkit wrote while Agent
// still carried LimitLoadToSessionType. It is a literal on purpose: the constant
// it pins moves, this value must not.
const supersededControllerFingerprint = "2f828880de0cd3fb5f645bc6c859064fddf5a401fe088c782c22577c59e14be0"

func TestControllerConfigThreadsSchemaPolicyToProcessStore(t *testing.T) {
	config := controllerConfig(t)
	config.UnsupportedSchema = proc.ArchiveUnsupportedSchema
	store := config.processStore()
	if store.Path != config.ProcessPath {
		t.Fatalf("process store path = %q, want %q", store.Path, config.ProcessPath)
	}
	if store.UnsupportedSchema != proc.ArchiveUnsupportedSchema {
		t.Fatalf("process store policy = %v, want ArchiveUnsupportedSchema", store.UnsupportedSchema)
	}
}

func TestControllerConfigDefaultsToFailClosedSchema(t *testing.T) {
	var failClosed proc.UnsupportedSchemaPolicy
	if store := controllerConfig(t).processStore(); store.UnsupportedSchema != failClosed {
		t.Fatalf("default process store policy = %v, want zero (fail-closed)", store.UnsupportedSchema)
	}
}

func TestControllerStoreArchivesSupersededAgentFieldSetAndNamesAbandonedAgents(t *testing.T) {
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	logs := captureDefaultLog(t)
	path := filepath.Join(t.TempDir(), "services.db")
	const label = "com.example.superseded"
	writeSupersededControllerStore(t, path, label)

	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatalf("openControllerStore() error = %v, want archive aside", err)
	}
	defer store.Close()

	backups := controllerStoreBackups(t, path)
	if len(backups) != 1 {
		t.Fatalf("archived backups = %v, want exactly one", backups)
	}
	archived, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(archived, []byte(supersededControllerFingerprint)) ||
		!bytes.Contains(archived, []byte("LimitLoadToSessionType")) {
		t.Fatalf("backup %q does not preserve the superseded store", backups[0])
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v, want a fresh store", err)
	}
	if len(state.Desired) != 0 || len(state.Applied) != 0 {
		t.Fatalf("state after archive = %#v, want empty", state)
	}

	logged := logs.String()
	wantPlist := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
	for _, want := range []string{
		"abandoning applied LaunchAgent bookkeeping", "label=" + label, "plist=" + wantPlist,
		"archived unsupported-schema controller state", "backup=" + backups[0],
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("archive log missing %q\n%s", want, logged)
		}
	}
}

func writeSupersededControllerStore(t *testing.T, path, label string) {
	t.Helper()
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	agent := controllerAgent(t, label)
	if _, err := store.ReplaceDesired(context.Background(), map[string]Agent{label: agent}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.SetApplied(context.Background(), label, &agent); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(controllerMetaBucket).Put(
			controllerFingerprintKey, []byte(supersededControllerFingerprint),
		); err != nil {
			return err
		}
		for _, name := range [][]byte{controllerDesiredBucket, controllerAppliedBucket} {
			bucket := tx.Bucket(name)
			payload := append([]byte(nil), bucket.Get([]byte(label))...)
			payload = append(payload[:len(payload)-1], []byte(`,"LimitLoadToSessionType":0}`)...)
			if err := bucket.Put([]byte(label), payload); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func controllerStoreBackups(t *testing.T, path string) []string {
	t.Helper()
	backups, err := filepath.Glob(path + ".*.bak")
	if err != nil {
		t.Fatal(err)
	}
	return backups
}

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func captureDefaultLog(t *testing.T) *syncBuffer {
	t.Helper()
	captured := &syncBuffer{}
	prior := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prior) })
	return captured
}
