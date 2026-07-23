package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type unpublishedControllerDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c unpublishedControllerDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (unpublishedControllerDeadlineContext) Done() <-chan struct{}         { return nil }
func (unpublishedControllerDeadlineContext) Err() error                    { return nil }

func TestControllerStoreExpiredDeadlineNeverReturnsNilSuccess(t *testing.T) {
	ctx := unpublishedControllerDeadlineContext{
		Context: context.Background(), deadline: time.Now().Add(-time.Second),
	}
	store, err := openControllerStore(ctx, filepath.Join(t.TempDir(), "services.db"))
	if store != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open = %v, %v; want nil, deadline exceeded", store, err)
	}
}

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

func TestControllerStoreRejectsEveryPreexistingSchemaLessLayout(t *testing.T) {
	tests := []struct {
		name    string
		buckets [][]byte
		want    string
	}{
		{
			name: "expected buckets without metadata",
			buckets: [][]byte{
				controllerDesiredBucket, controllerAppliedBucket, controllerReplacementBucket,
			},
			want: "schema-less",
		},
		{
			name: "empty expected metadata and buckets",
			buckets: [][]byte{
				controllerMetaBucket, controllerDesiredBucket, controllerAppliedBucket, controllerReplacementBucket,
			},
			want: "schema-less",
		},
		{name: "unknown bucket", buckets: [][]byte{[]byte("legacy")}, want: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "services.db")
			db, err := bolt.Open(path, 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *bolt.Tx) error {
				for _, bucket := range test.buckets {
					if _, err := tx.CreateBucket(bucket); err != nil {
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
			if _, err := openControllerStore(context.Background(), path); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("openControllerStore() error = %v, want %s rejection", err, test.want)
			}
		})
	}
}

func TestControllerStoreInitializesOnlyTrulyEmptyBoltFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Desired) != 0 || len(state.Applied) != 0 ||
		state.Replacement != nil || state.ReplacementCommit != nil {
		t.Fatalf("fresh state = %#v", state)
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
		{
			name: "foreign identity", want: "identity",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put(controllerIdentityKey, []byte("foreign"))
			},
		},
		{
			name: "foreign fingerprint", want: "fingerprint",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put(controllerFingerprintKey, []byte("foreign"))
			},
		},
		{
			name: "missing identity", want: "identity",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Delete(controllerIdentityKey)
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
		if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "field set") {
			t.Fatalf("Load() error = %v, want strict field rejection", err)
		}
	})

	t.Run("missing agent field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "services.db")
		store, err := openControllerStore(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		agent := controllerAgent(t, "com.example.missing")
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
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(bucket.Get([]byte(agent.Label)), &fields); err != nil {
				return err
			}
			delete(fields, "Program")
			payload, err := json.Marshal(fields)
			if err != nil {
				return err
			}
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
		if _, err := store.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "field set") {
			t.Fatalf("Load() error = %v, want missing field rejection", err)
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

func TestControllerStoreRejectsPersistedUnsafeProgram(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "executable")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "executable-link")
	if err := os.Symlink(executable, link); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		program string
	}{
		{name: "empty", program: ""},
		{name: "relative", program: "usr/bin/true"},
		{name: "symlink", program: link},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "services.db")
			store, err := openControllerStore(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			agent := controllerAgent(t, "com.example.persisted-unsafe")
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
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(bucket.Get([]byte(agent.Label)), &fields); err != nil {
					return err
				}
				payload, err := json.Marshal(test.program)
				if err != nil {
					return err
				}
				fields["Program"] = payload
				payload, err = json.Marshal(fields)
				if err != nil {
					return err
				}
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
			if _, err := store.Load(context.Background()); err == nil {
				t.Fatal("Load accepted persisted unsafe program")
			}
		})
	}
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
