package service

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestControllerStorePersistsExactDesiredAndAppliedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.db")
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("state mode = %v, want regular 0600", info.Mode())
	}
	first := controllerAgent(t, "com.example.first")
	second := controllerAgent(t, "com.example.second")
	prior, err := store.ReplaceDesired(context.Background(), map[string]Agent{
		first.Label: first, second.Label: second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prior.Desired) != 0 || len(prior.Applied) != 0 {
		t.Fatalf("prior state = %#v, want empty", prior)
	}
	if err := store.SetApplied(context.Background(), first.Label, &first); err != nil {
		t.Fatal(err)
	}
	prior, err = store.ReplaceDesired(context.Background(), map[string]Agent{second.Label: second})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prior.Desired, map[string]Agent{first.Label: first, second.Label: second}) ||
		!reflect.DeepEqual(prior.Applied, map[string]Agent{first.Label: first}) {
		t.Fatalf("transactional prior = %#v", prior)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.Desired, map[string]Agent{second.Label: second}) ||
		!reflect.DeepEqual(state.Applied, map[string]Agent{first.Label: first}) {
		t.Fatalf("reopened state = %#v", state)
	}
	if err := store.SetApplied(context.Background(), first.Label, nil); err != nil {
		t.Fatal(err)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Applied) != 0 {
		t.Fatalf("removed applied state = %#v", state.Applied)
	}
}

func TestControllerStateCanonicalizesAndClonesAssociatedBundleIdentifiers(t *testing.T) {
	agent := controllerAgent(t, "com.example.associated")
	agent.AssociatedBundleIdentifiers = []string{"com.example.z", "com.example.a"}
	desired, err := desiredAgents([]Agent{agent})
	if err != nil {
		t.Fatal(err)
	}
	agent.AssociatedBundleIdentifiers[0] = "com.example.mutated"
	want := []string{"com.example.a", "com.example.z"}
	if got := desired[agent.Label].AssociatedBundleIdentifiers; !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical associated bundle identifiers = %v, want %v", got, want)
	}

	path := filepath.Join(t.TempDir(), "services.db")
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.ReplaceDesired(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Desired[agent.Label].AssociatedBundleIdentifiers; !reflect.DeepEqual(got, want) {
		t.Fatalf("stored associated bundle identifiers = %v, want %v", got, want)
	}
}

func TestControllerStoreRejectsConcurrentOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.db")
	first, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := openControllerStore(ctx, path); err == nil {
		t.Fatal("second controller acquired the same lifetime state database")
	}
}

func TestControllerStoreRejectsUnknownSchemaSurfaces(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*bolt.Tx) error
	}{
		{
			name: "bucket", want: "unknown",
			mutate: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("future"))
				return err
			},
		},
		{
			name: "metadata", want: "unknown",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put([]byte("future"), []byte("1"))
			},
		},
		{
			name: "foreign epoch", want: "unsupported",
			mutate: func(tx *bolt.Tx) error {
				var schema [8]byte
				binary.BigEndian.PutUint64(schema[:], 2)
				return tx.Bucket(controllerMetaBucket).Put(controllerSchemaKey, schema[:])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "services.db")
			store, err := openControllerStore(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(path, 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(test.mutate); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := openControllerStore(context.Background(), path); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("openControllerStore() error = %v, want %s schema rejection", err, test.want)
			}
		})
	}
}

func TestControllerStoreRejectsUnknownAgentFieldsAndLegacyJSON(t *testing.T) {
	t.Run("agent field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "services.db")
		store, err := openControllerStore(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		agent := controllerAgent(t, "com.example.strict")
		if _, err := store.ReplaceDesired(context.Background(), map[string]Agent{agent.Label: agent}); err != nil {
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
			bucket := tx.Bucket(controllerDesiredBucket)
			payload := append([]byte(nil), bucket.Get([]byte(agent.Label))...)
			payload = append(payload[:len(payload)-1], []byte(`,"future":true}`)...)
			return bucket.Put([]byte(agent.Label), payload)
		}); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = openControllerStore(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("Load() error = %v, want strict field rejection", err)
		}
	})

	t.Run("legacy json", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "services.db")
		if err := os.WriteFile(path, []byte(`{"desired":{},"applied":{}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := openControllerStore(context.Background(), path); err == nil {
			t.Fatal("legacy JSON was accepted")
		}
	})
}

func TestControllerStoreRejectsUnsafeModeAndInvalidKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.db")
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openControllerStore(context.Background(), path); err == nil {
		t.Fatal("controller state with unsafe mode was accepted")
	}
	target := filepath.Join(t.TempDir(), "target.db")
	targetStore, err := openControllerStore(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if err := targetStore.Close(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "linked.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := openControllerStore(context.Background(), link); err == nil {
		t.Fatal("symlinked controller state was accepted")
	}

	path = filepath.Join(t.TempDir(), "valid.db")
	store, err = openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	agent := controllerAgent(t, "com.example.key")
	if _, err := store.ReplaceDesired(context.Background(), map[string]Agent{"other": agent}); err == nil {
		t.Fatal("mismatched desired key was accepted")
	}
	if err := store.SetApplied(context.Background(), "other", &agent); err == nil {
		t.Fatal("mismatched applied key was accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Load(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load(canceled) = %v", err)
	}
}
