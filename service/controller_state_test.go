package service

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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

func TestControllerStoreLoadsStaleDesiredAndAppliedPrograms(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*boltControllerStore, Agent) error
		load  func(controllerState, string) (Agent, bool)
	}{
		{
			name: "desired",
			write: func(store *boltControllerStore, agent Agent) error {
				_, err := store.ReplaceDesired(t.Context(), map[string]Agent{agent.Label: agent})
				return err
			},
			load: func(state controllerState, label string) (Agent, bool) {
				agent, ok := state.Desired[label]
				return agent, ok
			},
		},
		{
			name: "applied",
			write: func(store *boltControllerStore, agent Agent) error {
				return store.SetApplied(t.Context(), agent.Label, &agent)
			},
			load: func(state controllerState, label string) (Agent, bool) {
				agent, ok := state.Applied[label]
				return agent, ok
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "services.db")
			store, err := openControllerStore(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			agent := controllerAgent(t, "com.example.stale-"+test.name)
			agent.Program = controllerExecutable(t, "executable")
			if err := test.write(store, agent); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if err := os.Remove(agent.Program); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			store, err = openControllerStore(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			state, err := store.Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			got, ok := test.load(state, agent.Label)
			if !ok || !reflect.DeepEqual(got, agent) {
				t.Fatalf("loaded stale agent = %#v, %t; want %#v, true", got, ok, agent)
			}
		})
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

func TestControllerStoreNeverPersistsSessionType(t *testing.T) {
	captureDefaultLog(t)
	agent := controllerAgent(t, "com.example.session-type")
	agent.LimitLoadToSessionType = SessionTypeBackground
	payload, err := encodeControllerAgent(agent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), `"LimitLoadToSessionType":`) {
		t.Fatalf("stored agent carries the ignored field\n%s", payload)
	}

	desired, err := desiredAgents([]Agent{agent})
	if err != nil {
		t.Fatal(err)
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
	if got := state.Desired[agent.Label].LimitLoadToSessionType; got != sessionTypeUnset {
		t.Fatalf("loaded session type = %d, want unset", got)
	}
	if got := state.Desired[agent.Label]; !reflect.DeepEqual(got, desired[agent.Label]) {
		t.Fatalf("loaded agent = %#v, want the canonical desired agent %#v", got, desired[agent.Label])
	}
}

func TestSessionTypeNeverSplitsPlanEquality(t *testing.T) {
	captureDefaultLog(t)
	agent := controllerAgent(t, "com.example.plan-equality")
	agent.LimitLoadToSessionType = SessionTypeAqua
	live, err := NewPlan([]Agent{agent})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "services.db")
	store, err := openControllerStore(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.ReplaceDesired(context.Background(), live.agents); err != nil {
		t.Fatal(err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestorePlan(slices.Collect(maps.Values(state.Desired)), live.Digest())
	if err != nil {
		t.Fatal(err)
	}
	if !plansEqual(live, restored) {
		t.Fatalf("stored plan is unequal to the live plan it round-tripped from\nlive %#v\nrestored %#v",
			live.agents, restored.agents)
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

func TestControllerStoreArchivesEveryPreexistingSchemaLessLayout(t *testing.T) {
	tests := []struct {
		name    string
		buckets [][]byte
	}{
		{
			name: "expected buckets without metadata",
			buckets: [][]byte{
				controllerDesiredBucket, controllerAppliedBucket, controllerReplacementBucket,
			},
		},
		{
			name: "empty expected metadata and buckets",
			buckets: [][]byte{
				controllerMetaBucket, controllerDesiredBucket, controllerAppliedBucket, controllerReplacementBucket,
			},
		},
		{name: "unknown bucket", buckets: [][]byte{[]byte("legacy")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			captureDefaultLog(t)
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
			store, err := openControllerStore(context.Background(), path)
			if err != nil {
				t.Fatalf("openControllerStore() error = %v, want archive aside", err)
			}
			defer store.Close()
			if backups := controllerStoreBackups(t, path); len(backups) != 1 {
				t.Fatalf("archived backups = %v, want exactly one", backups)
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

func TestControllerStoreArchivesUnknownSchemaSurfaces(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bolt.Tx) error
	}{
		{
			name: "bucket",
			mutate: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("future"))
				return err
			},
		},
		{
			name: "metadata",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put([]byte("future"), []byte("1"))
			},
		},
		{
			name: "foreign epoch",
			mutate: func(tx *bolt.Tx) error {
				var schema [8]byte
				binary.BigEndian.PutUint64(schema[:], 2)
				return tx.Bucket(controllerMetaBucket).Put(controllerSchemaKey, schema[:])
			},
		},
		{
			name: "foreign identity",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put(controllerIdentityKey, []byte("foreign"))
			},
		},
		{
			name: "foreign fingerprint",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Put(controllerFingerprintKey, []byte("foreign"))
			},
		},
		{
			name: "missing identity",
			mutate: func(tx *bolt.Tx) error {
				return tx.Bucket(controllerMetaBucket).Delete(controllerIdentityKey)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			captureDefaultLog(t)
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
			store, err = openControllerStore(context.Background(), path)
			if err != nil {
				t.Fatalf("openControllerStore() error = %v, want archive aside", err)
			}
			defer store.Close()
			if backups := controllerStoreBackups(t, path); len(backups) != 1 {
				t.Fatalf("archived backups = %v, want exactly one", backups)
			}
			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Desired) != 0 || len(state.Applied) != 0 {
				t.Fatalf("state after archive = %#v, want empty", state)
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

func TestControllerStoreRejectsPersistedInvalidProgramAndLoadsStaleProgram(t *testing.T) {
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
		wantErr bool
	}{
		{name: "empty", program: "", wantErr: true},
		{name: "relative", program: "usr/bin/true", wantErr: true},
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
			state, err := store.Load(context.Background())
			if test.wantErr {
				if err == nil {
					t.Fatal("Load accepted persisted invalid program")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			agent.Program = test.program
			if got := state.Desired[agent.Label]; !reflect.DeepEqual(got, agent) {
				t.Fatalf("loaded stale agent = %#v, want %#v", got, agent)
			}
		})
	}
}

func TestControllerStoreRejectsMissingProgramAtWrite(t *testing.T) {
	store, err := openControllerStore(t.Context(), filepath.Join(t.TempDir(), "services.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	agent := controllerAgent(t, "com.example.missing-at-write")
	agent.Program = filepath.Join(t.TempDir(), "missing")
	if _, err := store.ReplaceDesired(t.Context(), map[string]Agent{agent.Label: agent}); err == nil {
		t.Fatal("ReplaceDesired() accepted a missing program")
	}
	if err := store.SetApplied(t.Context(), agent.Label, &agent); err == nil {
		t.Fatal("SetApplied() accepted a missing program")
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
