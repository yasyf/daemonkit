package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	dkdaemon "github.com/yasyf/daemonkit/daemon"
	bolt "go.etcd.io/bbolt"
)

const (
	controllerStateSchema = 1
	controllerOpenBound   = 5 * time.Second
)

var (
	controllerMetaBucket    = []byte("meta")
	controllerDesiredBucket = []byte("desired")
	controllerAppliedBucket = []byte("applied")
	controllerSchemaKey     = []byte("schema")
)

type controllerState struct {
	Desired map[string]Agent
	Applied map[string]Agent
}

type controllerStateStore interface {
	Load(context.Context) (controllerState, error)
	ReplaceDesired(context.Context, map[string]Agent) (controllerState, error)
	SetApplied(context.Context, string, *Agent) error
	Close() error
}

type boltControllerStore struct {
	db *bolt.DB
}

func openControllerStore(ctx context.Context, path string) (*boltControllerStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := exactControllerPath(path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("service: create controller state directory: %w", err)
	}
	timeout := controllerOpenBound
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		timeout = min(timeout, remaining)
	}
	info, statErr := os.Lstat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("service: inspect controller state: %w", statErr)
	}
	if !created && (!info.Mode().IsRegular() || info.Mode().Perm() != 0o600) {
		return nil, errors.New("service: controller state must be a regular 0600 file")
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("service: open controller state: %w", err)
	}
	store := &boltControllerStore{db: db}
	if err := db.Update(initializeControllerState); err != nil {
		_ = db.Close()
		return nil, err
	}
	info, err = os.Lstat(path)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("service: inspect opened controller state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = db.Close()
		return nil, errors.New("service: controller state must be a regular 0600 file")
	}
	if created {
		if err := dkdaemon.SyncDir(filepath.Dir(path)); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("service: persist controller state directory entry: %w", err)
		}
	}
	return store, nil
}

func exactControllerPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("service: controller state path %q is not exact and absolute", path)
	}
	return nil
}

func initializeControllerState(tx *bolt.Tx) error {
	expected := map[string]bool{
		string(controllerMetaBucket): true, string(controllerDesiredBucket): true,
		string(controllerAppliedBucket): true,
	}
	present := make(map[string]bool, len(expected))
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if !expected[string(name)] {
			return fmt.Errorf("service: unknown controller state bucket %q", name)
		}
		present[string(name)] = true
		return nil
	}); err != nil {
		return err
	}
	meta := tx.Bucket(controllerMetaBucket)
	var schema []byte
	if meta != nil {
		schema = meta.Get(controllerSchemaKey)
	}
	if schema == nil {
		for name := range present {
			key, _ := tx.Bucket([]byte(name)).Cursor().First()
			if key != nil {
				return fmt.Errorf("service: uninitialized controller bucket %q is not empty", name)
			}
		}
		var err error
		meta, err = tx.CreateBucketIfNotExists(controllerMetaBucket)
		if err != nil {
			return fmt.Errorf("service: create controller metadata: %w", err)
		}
		for _, name := range [][]byte{controllerDesiredBucket, controllerAppliedBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("service: create controller bucket %q: %w", name, err)
			}
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], controllerStateSchema)
		return meta.Put(controllerSchemaKey, encoded[:])
	}
	for name := range expected {
		if !present[name] {
			return fmt.Errorf("service: initialized controller state is missing bucket %q", name)
		}
	}
	if len(schema) != 8 || binary.BigEndian.Uint64(schema) != controllerStateSchema {
		return errors.New("service: unsupported controller state schema")
	}
	if err := meta.ForEach(func(key, _ []byte) error {
		if !bytes.Equal(key, controllerSchemaKey) {
			return fmt.Errorf("service: unknown controller metadata key %q", key)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *boltControllerStore) Load(ctx context.Context) (controllerState, error) {
	if err := ctx.Err(); err != nil {
		return controllerState{}, err
	}
	var state controllerState
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		state, err = loadControllerStateTx(tx)
		return err
	})
	return state, err
}

// ReplaceDesired atomically replaces every desired key and returns the exact
// state that preceded the transaction.
func (s *boltControllerStore) ReplaceDesired(
	ctx context.Context,
	desired map[string]Agent,
) (controllerState, error) {
	if err := ctx.Err(); err != nil {
		return controllerState{}, err
	}
	encoded := make(map[string][]byte, len(desired))
	for label, agent := range desired {
		if label != agent.Label {
			return controllerState{}, fmt.Errorf("service: desired state key %q does not match agent label %q", label, agent.Label)
		}
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return controllerState{}, err
		}
		encoded[label] = payload
	}
	var prior controllerState
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		prior, err = loadControllerStateTx(tx)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(controllerDesiredBucket)
		cursor := bucket.Cursor()
		for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
			if _, keep := encoded[string(key)]; keep {
				continue
			}
			if err := cursor.Delete(); err != nil {
				return fmt.Errorf("service: clear desired agent %q: %w", key, err)
			}
		}
		labels := make([]string, 0, len(desired))
		for label := range desired {
			labels = append(labels, label)
		}
		slices.Sort(labels)
		for _, label := range labels {
			payload := encoded[label]
			if bytes.Equal(bucket.Get([]byte(label)), payload) {
				continue
			}
			if err := bucket.Put([]byte(label), payload); err != nil {
				return fmt.Errorf("service: persist desired agent %q: %w", label, err)
			}
		}
		return nil
	})
	return prior, err
}

func (s *boltControllerStore) SetApplied(
	ctx context.Context,
	label string,
	agent *Agent,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(controllerAppliedBucket)
		if agent == nil {
			return bucket.Delete([]byte(label))
		}
		if label != agent.Label {
			return fmt.Errorf("service: applied state key %q does not match agent label %q", label, agent.Label)
		}
		payload, err := encodeControllerAgent(*agent)
		if err != nil {
			return err
		}
		if bytes.Equal(bucket.Get([]byte(label)), payload) {
			return nil
		}
		return bucket.Put([]byte(label), payload)
	})
}

func (s *boltControllerStore) Close() error {
	return s.db.Close()
}

func loadControllerStateTx(tx *bolt.Tx) (controllerState, error) {
	desired, err := loadControllerAgents(tx.Bucket(controllerDesiredBucket))
	if err != nil {
		return controllerState{}, fmt.Errorf("service: load desired agents: %w", err)
	}
	applied, err := loadControllerAgents(tx.Bucket(controllerAppliedBucket))
	if err != nil {
		return controllerState{}, fmt.Errorf("service: load applied agents: %w", err)
	}
	return controllerState{Desired: desired, Applied: applied}, nil
}

func loadControllerAgents(bucket *bolt.Bucket) (map[string]Agent, error) {
	agents := make(map[string]Agent)
	err := bucket.ForEach(func(key, payload []byte) error {
		agent, err := decodeControllerAgent(payload)
		if err != nil {
			return fmt.Errorf("agent %q: %w", key, err)
		}
		label := string(key)
		if label != agent.Label {
			return fmt.Errorf("state key %q does not match agent label %q", label, agent.Label)
		}
		agents[label] = agent
		return nil
	})
	return agents, err
}

func encodeControllerAgent(agent Agent) ([]byte, error) {
	if _, err := agent.Plist(); err != nil {
		return nil, fmt.Errorf("service: validate stored agent %q: %w", agent.Label, err)
	}
	agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
		agent.AssociatedBundleIdentifiers,
	)
	payload, err := json.Marshal(agent)
	if err != nil {
		return nil, fmt.Errorf("service: encode stored agent %q: %w", agent.Label, err)
	}
	return payload, nil
}

func decodeControllerAgent(payload []byte) (Agent, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var agent Agent
	if err := decoder.Decode(&agent); err != nil {
		return Agent{}, fmt.Errorf("decode stored agent: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Agent{}, errors.New("stored agent has trailing JSON")
	}
	if _, err := agent.Plist(); err != nil {
		return Agent{}, fmt.Errorf("validate stored agent: %w", err)
	}
	agent.Args = append([]string(nil), agent.Args...)
	agent.Env = cloneStrings(agent.Env)
	agent.AssociatedBundleIdentifiers, _ = canonicalAssociatedBundleIdentifiers(
		agent.AssociatedBundleIdentifiers,
	)
	return agent, nil
}

func copyAgents(agents map[string]Agent) map[string]Agent {
	copied := make(map[string]Agent, len(agents))
	for label, agent := range agents {
		agent.Args = append([]string(nil), agent.Args...)
		agent.Env = cloneStrings(agent.Env)
		agent.AssociatedBundleIdentifiers = append(
			[]string(nil), agent.AssociatedBundleIdentifiers...,
		)
		copied[label] = agent
	}
	return copied
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
