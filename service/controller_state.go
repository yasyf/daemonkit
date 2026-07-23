package service

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
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
	controllerMetaBucket           = []byte("meta")
	controllerDesiredBucket        = []byte("desired")
	controllerAppliedBucket        = []byte("applied")
	controllerReplacementBucket    = []byte("replacement")
	controllerReplacementKey       = []byte("fence")
	controllerReplacementCommitKey = []byte("commit")
	controllerReplacementAckKey    = []byte("acknowledged")
	controllerIdentityKey          = []byte("identity")
	controllerSchemaKey            = []byte("schema")
	controllerFingerprintKey       = []byte("fingerprint")
	controllerIdentity             = []byte("daemonkit.service.controller-store.v1")
	controllerFingerprint          = []byte("41187f10b4271970a39b5f7beef1972e1c3b802e93eb46f219171fb0b12034cc")
	replacementIdentity            = "daemonkit.service.replacement-fence.v1"
	replacementFingerprint         = "d2ad7d3d5fb6b835099c6301c285791b1cd026f859387e0c7e9bdcac23b0285e"
	replacementCommitIdentity      = "daemonkit.service.replacement-commit.v1"
	replacementCommitFingerprint   = "709c4597c1d1ad13efb7a6682d39b6aa37304c49f7e5b56653536b72710ab7a9"
	replacementAckIdentity         = "daemonkit.service.replacement-acknowledgement.v1"
	replacementAckFingerprint      = "baacdf40ef2760ac95294c4662d5c54e555e36f40f9763602714dff9e67de1cf"
)

type controllerState struct {
	Desired           map[string]Agent
	Applied           map[string]Agent
	Replacement       *replacementState
	ReplacementCommit *replacementCommit
	ReplacementAck    *replacementCommit
}

type controllerStateStore interface {
	Load(context.Context) (controllerState, error)
	ReplaceDesired(context.Context, map[string]Agent) (controllerState, error)
	SetReplacement(
		context.Context, map[string]Agent, *replacementState, *replacementCommit, *replacementCommit,
	) (controllerState, error)
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
		string(controllerAppliedBucket): true, string(controllerReplacementBucket): true,
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
		if len(present) != 0 {
			return errors.New("service: preexisting schema-less controller state is not supported")
		}
		var err error
		meta, err = tx.CreateBucketIfNotExists(controllerMetaBucket)
		if err != nil {
			return fmt.Errorf("service: create controller metadata: %w", err)
		}
		for _, name := range [][]byte{
			controllerDesiredBucket, controllerAppliedBucket, controllerReplacementBucket,
		} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("service: create controller bucket %q: %w", name, err)
			}
		}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], controllerStateSchema)
		if err := meta.Put(controllerIdentityKey, controllerIdentity); err != nil {
			return err
		}
		if err := meta.Put(controllerSchemaKey, encoded[:]); err != nil {
			return err
		}
		return meta.Put(controllerFingerprintKey, controllerFingerprint)
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
		if !bytes.Equal(key, controllerIdentityKey) && !bytes.Equal(key, controllerSchemaKey) &&
			!bytes.Equal(key, controllerFingerprintKey) {
			return fmt.Errorf("service: unknown controller metadata key %q", key)
		}
		return nil
	}); err != nil {
		return err
	}
	if !bytes.Equal(meta.Get(controllerIdentityKey), controllerIdentity) ||
		!bytes.Equal(meta.Get(controllerFingerprintKey), controllerFingerprint) {
		return errors.New("service: controller state identity or fingerprint mismatch")
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
		payload, err := encodeLiveControllerAgent(agent)
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

func (s *boltControllerStore) SetReplacement(
	ctx context.Context,
	desired map[string]Agent,
	replacement *replacementState,
	commit *replacementCommit,
	acknowledged *replacementCommit,
) (controllerState, error) {
	if err := ctx.Err(); err != nil {
		return controllerState{}, err
	}
	encoded, err := encodeControllerAgents(desired)
	if err != nil {
		return controllerState{}, err
	}
	var payload []byte
	if replacement != nil {
		payload, err = encodeReplacementState(replacement)
		if err != nil {
			return controllerState{}, err
		}
	}
	var commitPayload []byte
	if commit != nil {
		commitPayload, err = encodeReplacementCommit(commit, replacementCommitIdentity, replacementCommitFingerprint)
		if err != nil {
			return controllerState{}, err
		}
	}
	var acknowledgedPayload []byte
	if acknowledged != nil {
		acknowledgedPayload, err = encodeReplacementCommit(
			acknowledged, replacementAckIdentity, replacementAckFingerprint,
		)
		if err != nil {
			return controllerState{}, err
		}
	}
	var state controllerState
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := replaceControllerAgents(tx.Bucket(controllerDesiredBucket), encoded); err != nil {
			return err
		}
		bucket := tx.Bucket(controllerReplacementBucket)
		if replacement == nil {
			if err := bucket.Delete(controllerReplacementKey); err != nil {
				return fmt.Errorf("service: clear replacement fence: %w", err)
			}
		} else if err := bucket.Put(controllerReplacementKey, payload); err != nil {
			return fmt.Errorf("service: persist replacement fence: %w", err)
		}
		if commit == nil {
			if err := bucket.Delete(controllerReplacementCommitKey); err != nil {
				return fmt.Errorf("service: clear replacement commit: %w", err)
			}
		} else if err := bucket.Put(controllerReplacementCommitKey, commitPayload); err != nil {
			return fmt.Errorf("service: persist replacement commit: %w", err)
		}
		if acknowledged == nil {
			if err := bucket.Delete(controllerReplacementAckKey); err != nil {
				return fmt.Errorf("service: clear replacement acknowledgement: %w", err)
			}
		} else if err := bucket.Put(controllerReplacementAckKey, acknowledgedPayload); err != nil {
			return fmt.Errorf("service: persist replacement acknowledgement: %w", err)
		}
		var err error
		state, err = loadControllerStateTx(tx)
		return err
	})
	return state, err
}

func encodeControllerAgents(agents map[string]Agent) (map[string][]byte, error) {
	encoded := make(map[string][]byte, len(agents))
	for label, agent := range agents {
		if label != agent.Label {
			return nil, fmt.Errorf("service: state key %q does not match agent label %q", label, agent.Label)
		}
		payload, err := encodeLiveControllerAgent(agent)
		if err != nil {
			return nil, err
		}
		encoded[label] = payload
	}
	return encoded, nil
}

func replaceControllerAgents(bucket *bolt.Bucket, encoded map[string][]byte) error {
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		if _, keep := encoded[string(key)]; keep {
			continue
		}
		if err := cursor.Delete(); err != nil {
			return fmt.Errorf("service: clear agent %q: %w", key, err)
		}
	}
	for _, label := range slices.Sorted(maps.Keys(encoded)) {
		payload := encoded[label]
		if bytes.Equal(bucket.Get([]byte(label)), payload) {
			continue
		}
		if err := bucket.Put([]byte(label), payload); err != nil {
			return fmt.Errorf("service: persist agent %q: %w", label, err)
		}
	}
	return nil
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
		payload, err := encodeLiveControllerAgent(*agent)
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
	replacement, commit, acknowledged, err := loadReplacementState(tx.Bucket(controllerReplacementBucket))
	if err != nil {
		return controllerState{}, err
	}
	state := controllerState{
		Desired: desired, Applied: applied, Replacement: replacement,
		ReplacementCommit: commit, ReplacementAck: acknowledged,
	}
	if err := validateReplacementState(state); err != nil {
		return controllerState{}, err
	}
	return state, nil
}

func loadControllerAgents(bucket *bolt.Bucket) (map[string]Agent, error) {
	agents := make(map[string]Agent)
	err := bucket.ForEach(func(key, payload []byte) error {
		agent, err := decodeLiveControllerAgent(payload)
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

type replacementProofWire struct {
	Epoch        uint64   `json:"epoch"`
	PlanDigest   string   `json:"plan_digest"`
	ProgramPaths []string `json:"program_paths"`
	ProvedAt     string   `json:"proved_at"`
}

type replacementStateWire struct {
	Identity      string                     `json:"identity"`
	Schema        int                        `json:"schema"`
	Fingerprint   string                     `json:"fingerprint"`
	OperationID   string                     `json:"operation_id"`
	Binding       string                     `json:"binding"`
	Phase         ReplacementPhase           `json:"phase"`
	Epoch         uint64                     `json:"epoch"`
	PriorDigest   string                     `json:"prior_digest"`
	CurrentDigest string                     `json:"current_digest"`
	Prior         map[string]json.RawMessage `json:"prior"`
	Current       map[string]json.RawMessage `json:"current"`
	Proofs        []replacementProofWire     `json:"proofs"`
}

type replacementCommitWire struct {
	Identity    string                     `json:"identity"`
	Schema      int                        `json:"schema"`
	Fingerprint string                     `json:"fingerprint"`
	OperationID string                     `json:"operation_id"`
	Binding     string                     `json:"binding"`
	PriorDigest string                     `json:"prior_digest"`
	NextDigest  string                     `json:"next_digest"`
	Prior       map[string]json.RawMessage `json:"prior"`
	Next        map[string]json.RawMessage `json:"next"`
}

func encodeReplacementState(replacement *replacementState) ([]byte, error) {
	if err := validateReplacement(replacement); err != nil {
		return nil, err
	}
	wire := replacementStateWire{
		Identity: replacementIdentity, Schema: 1, Fingerprint: replacementFingerprint,
		OperationID: replacement.OperationID, Binding: replacement.Binding.String(),
		Phase: replacement.Phase, Epoch: replacement.Epoch,
		PriorDigest: replacement.Prior.digest.String(), CurrentDigest: replacement.Current.digest.String(),
		Prior:   make(map[string]json.RawMessage, len(replacement.Prior.agents)),
		Current: make(map[string]json.RawMessage, len(replacement.Current.agents)),
	}
	for label, agent := range replacement.Prior.agents {
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return nil, err
		}
		wire.Prior[label] = payload
	}
	for label, agent := range replacement.Current.agents {
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return nil, err
		}
		wire.Current[label] = payload
	}
	for _, proof := range replacement.Proofs {
		wire.Proofs = append(wire.Proofs, replacementProofWire{
			Epoch: proof.Epoch, PlanDigest: proof.PlanDigest.String(),
			ProgramPaths: append([]string(nil), proof.ProgramPaths...),
			ProvedAt:     proof.ProvedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("service: encode replacement fence: %w", err)
	}
	return payload, nil
}

func encodeReplacementCommit(commit *replacementCommit, identity, fingerprint string) ([]byte, error) {
	if err := validateReplacementCommit(commit); err != nil {
		return nil, err
	}
	wire := replacementCommitWire{
		Identity: identity, Schema: 1, Fingerprint: fingerprint,
		OperationID: commit.OperationID, Binding: commit.Binding.String(),
		PriorDigest: commit.Prior.digest.String(), NextDigest: commit.Next.digest.String(),
		Prior: make(map[string]json.RawMessage, len(commit.Prior.agents)),
		Next:  make(map[string]json.RawMessage, len(commit.Next.agents)),
	}
	for label, agent := range commit.Prior.agents {
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return nil, err
		}
		wire.Prior[label] = payload
	}
	for label, agent := range commit.Next.agents {
		payload, err := encodeControllerAgent(agent)
		if err != nil {
			return nil, err
		}
		wire.Next[label] = payload
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("service: encode replacement commit: %w", err)
	}
	return payload, nil
}

func loadReplacementState(bucket *bolt.Bucket) (*replacementState, *replacementCommit, *replacementCommit, error) {
	var payload, commitPayload, acknowledgedPayload []byte
	if err := bucket.ForEach(func(key, value []byte) error {
		switch {
		case bytes.Equal(key, controllerReplacementKey):
			payload = append([]byte(nil), value...)
		case bytes.Equal(key, controllerReplacementCommitKey):
			commitPayload = append([]byte(nil), value...)
		case bytes.Equal(key, controllerReplacementAckKey):
			acknowledgedPayload = append([]byte(nil), value...)
		default:
			return fmt.Errorf("service: unknown replacement state key %q", key)
		}
		return nil
	}); err != nil {
		return nil, nil, nil, err
	}
	var replacement *replacementState
	var err error
	if payload != nil {
		replacement, err = decodeReplacementState(payload)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	var commit *replacementCommit
	if commitPayload != nil {
		commit, err = decodeReplacementCommit(
			commitPayload, replacementCommitIdentity, replacementCommitFingerprint,
		)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	var acknowledged *replacementCommit
	if acknowledgedPayload != nil {
		acknowledged, err = decodeReplacementCommit(
			acknowledgedPayload, replacementAckIdentity, replacementAckFingerprint,
		)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return replacement, commit, acknowledged, nil
}

func decodeReplacementState(payload []byte) (*replacementState, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fmt.Errorf("service: decode replacement fence: %w", err)
	}
	expected := []string{
		"identity", "schema", "fingerprint", "operation_id", "binding", "phase", "epoch",
		"prior_digest", "current_digest", "prior", "current", "proofs",
	}
	if len(fields) != len(expected) {
		return nil, errors.New("service: replacement fence field set is not exact")
	}
	for _, field := range expected {
		if _, ok := fields[field]; !ok {
			return nil, fmt.Errorf("service: replacement fence field %q is missing", field)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var wire replacementStateWire
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("service: decode replacement fence: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("service: replacement fence has trailing JSON")
	}
	if wire.Identity != replacementIdentity || wire.Schema != 1 || wire.Fingerprint != replacementFingerprint {
		return nil, errors.New("service: replacement fence identity, schema, or fingerprint mismatch")
	}
	bindingDigest, err := decodeExactDigest(wire.Binding)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement binding: %w", err)
	}
	prior, err := decodeReplacementAgents(wire.Prior)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement prior plan: %w", err)
	}
	current, err := decodeReplacementAgents(wire.Current)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement current plan: %w", err)
	}
	if wire.PriorDigest != prior.digest.String() || wire.CurrentDigest != current.digest.String() {
		return nil, errors.New("service: replacement plan digest mismatch")
	}
	replacement := &replacementState{
		OperationID: wire.OperationID, Phase: wire.Phase, Epoch: wire.Epoch,
		Binding: ReplacementBinding(bindingDigest),
		Prior:   prior, Current: current,
	}
	for _, proof := range wire.Proofs {
		digest, err := decodePlanDigest(proof.PlanDigest)
		if err != nil {
			return nil, err
		}
		provedAt, err := time.Parse(time.RFC3339Nano, proof.ProvedAt)
		if err != nil {
			return nil, fmt.Errorf("service: decode replacement proof time: %w", err)
		}
		replacement.Proofs = append(replacement.Proofs, replacementProof{
			Epoch: proof.Epoch, PlanDigest: digest,
			ProgramPaths: append([]string(nil), proof.ProgramPaths...), ProvedAt: provedAt,
		})
	}
	if err := validateReplacement(replacement); err != nil {
		return nil, err
	}
	return replacement, nil
}

func decodeReplacementCommit(payload []byte, identity, fingerprint string) (*replacementCommit, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, fmt.Errorf("service: decode replacement commit: %w", err)
	}
	expected := []string{
		"identity", "schema", "fingerprint", "operation_id", "binding",
		"prior_digest", "next_digest", "prior", "next",
	}
	if len(fields) != len(expected) {
		return nil, errors.New("service: replacement commit field set is not exact")
	}
	for _, field := range expected {
		if _, ok := fields[field]; !ok {
			return nil, fmt.Errorf("service: replacement commit field %q is missing", field)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var wire replacementCommitWire
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("service: decode replacement commit: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("service: replacement commit has trailing JSON")
	}
	if wire.Identity != identity || wire.Schema != 1 || wire.Fingerprint != fingerprint {
		return nil, errors.New("service: replacement commit identity, schema, or fingerprint mismatch")
	}
	binding, err := decodeExactDigest(wire.Binding)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement commit binding: %w", err)
	}
	prior, err := decodeReplacementAgents(wire.Prior)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement commit prior plan: %w", err)
	}
	next, err := decodeReplacementAgents(wire.Next)
	if err != nil {
		return nil, fmt.Errorf("service: decode replacement commit next plan: %w", err)
	}
	if wire.PriorDigest != prior.digest.String() || wire.NextDigest != next.digest.String() {
		return nil, errors.New("service: replacement commit plan digest mismatch")
	}
	commit := &replacementCommit{
		OperationID: wire.OperationID, Binding: ReplacementBinding(binding), Prior: prior, Next: next,
	}
	if err := validateReplacementCommit(commit); err != nil {
		return nil, err
	}
	return commit, nil
}

func decodeReplacementAgents(encoded map[string]json.RawMessage) (Plan, error) {
	agents := make(map[string]Agent, len(encoded))
	for label, payload := range encoded {
		agent, err := decodeControllerAgent(payload)
		if err != nil {
			return Plan{}, err
		}
		if label != agent.Label {
			return Plan{}, fmt.Errorf("state key %q does not match agent label %q", label, agent.Label)
		}
		agents[label] = agent
	}
	return planFromAgents(agents)
}

func decodePlanDigest(value string) (PlanDigest, error) {
	decoded, err := decodeExactDigest(value)
	if err != nil {
		return PlanDigest{}, errors.New("service: replacement proof digest is invalid")
	}
	return PlanDigest(decoded), nil
}

func decodeExactDigest(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) || hex.EncodeToString(decoded) != value {
		return digest, errors.New("digest is not exact lowercase sha256")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func validateReplacementState(state controllerState) error {
	if err := validateReplacementCommit(state.ReplacementAck); err != nil {
		return fmt.Errorf("service: replacement acknowledgement: %w", err)
	}
	if state.Replacement != nil && state.ReplacementCommit != nil {
		return errors.New("service: active replacement and committed replacement coexist")
	}
	if state.ReplacementCommit != nil {
		if err := validateReplacementCommit(state.ReplacementCommit); err != nil {
			return err
		}
		if !reflect.DeepEqual(state.Desired, state.ReplacementCommit.Next.agents) ||
			!reflect.DeepEqual(state.Applied, state.ReplacementCommit.Next.agents) {
			return errors.New("service: committed replacement plan differs from desired or applied state")
		}
		return nil
	}
	if state.Replacement == nil {
		return nil
	}
	if err := validateReplacement(state.Replacement); err != nil {
		return err
	}
	switch state.Replacement.Phase {
	case ReplacementUnloaded, ReplacementQuiesced:
		if len(state.Desired) != 0 {
			return errors.New("service: quiesced replacement retains desired agents")
		}
	case ReplacementRunningOwned:
		if !reflect.DeepEqual(state.Desired, state.Replacement.Current.agents) {
			return errors.New("service: running replacement desired plan differs from fence")
		}
	}
	return nil
}

func validateReplacement(replacement *replacementState) error {
	if replacement == nil {
		return nil
	}
	if err := validateReplacementOperationID(replacement.OperationID); err != nil {
		return err
	}
	if err := replacement.Binding.validate(); err != nil {
		return err
	}
	if replacement.Epoch == 0 {
		return errors.New("service: replacement epoch must be positive")
	}
	if err := replacement.Prior.validate(); err != nil {
		return err
	}
	if err := replacement.Current.validate(); err != nil {
		return err
	}
	switch replacement.Phase {
	case ReplacementUnloaded, ReplacementQuiesced, ReplacementRunningOwned:
	default:
		return fmt.Errorf("service: unknown replacement phase %q", replacement.Phase)
	}
	for index, proof := range replacement.Proofs {
		if proof.Epoch != uint64(index+1) || proof.Epoch > replacement.Epoch || proof.ProvedAt.IsZero() {
			return errors.New("service: replacement proofs are not one-per-epoch in exact order")
		}
		paths, err := exactProgramPaths(proof.ProgramPaths)
		if err != nil || !slices.Equal(paths, proof.ProgramPaths) {
			return errors.New("service: replacement proof paths are not canonical")
		}
	}
	switch replacement.Phase {
	case ReplacementUnloaded:
		if uint64(len(replacement.Proofs))+1 != replacement.Epoch {
			return errors.New("service: unloaded replacement proof history differs from epoch")
		}
	case ReplacementQuiesced, ReplacementRunningOwned:
		if uint64(len(replacement.Proofs)) != replacement.Epoch {
			return errors.New("service: proved replacement proof history differs from epoch")
		}
	}
	if replacement.Phase == ReplacementQuiesced {
		if len(replacement.Proofs) == 0 {
			return errors.New("service: quiesced replacement lacks an exact stop proof")
		}
		last := replacement.Proofs[len(replacement.Proofs)-1]
		if last.Epoch != replacement.Epoch || last.PlanDigest != replacement.Current.digest {
			return errors.New("service: quiesced replacement proof is stale")
		}
		paths, err := replacement.Current.programPaths()
		if err != nil {
			return err
		}
		if !slices.Equal(last.ProgramPaths, paths) {
			return errors.New("service: quiesced replacement proof paths differ from current plan")
		}
	}
	return nil
}

func validateReplacementCommit(commit *replacementCommit) error {
	if commit == nil {
		return nil
	}
	if err := validateReplacementOperationID(commit.OperationID); err != nil {
		return err
	}
	if err := commit.Binding.validate(); err != nil {
		return err
	}
	if err := commit.Prior.validate(); err != nil {
		return err
	}
	return commit.Next.validate()
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

func encodeLiveControllerAgent(agent Agent) ([]byte, error) {
	if err := validateProgramTree(agent); err != nil {
		return nil, fmt.Errorf("service: validate live stored agent %q: %w", agent.Label, err)
	}
	return encodeControllerAgent(agent)
}

func decodeControllerAgent(payload []byte) (Agent, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return Agent{}, fmt.Errorf("decode stored agent: %w", err)
	}
	expected := []string{
		"Label", "Program", "Args", "LogPath", "Env", "AssociatedBundleIdentifiers",
		"RestartPolicy", "StartInterval", "WatchPaths", "StartCalendarInterval",
		"ProcessType", "LimitLoadToSessionType",
	}
	if len(fields) != len(expected) {
		return Agent{}, errors.New("stored agent field set is not exact")
	}
	for _, field := range expected {
		if _, ok := fields[field]; !ok {
			return Agent{}, fmt.Errorf("stored agent field %q is missing", field)
		}
	}
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

func decodeLiveControllerAgent(payload []byte) (Agent, error) {
	agent, err := decodeControllerAgent(payload)
	if err != nil {
		return Agent{}, err
	}
	if err := validateProgramTree(agent); err != nil {
		return Agent{}, fmt.Errorf("validate live stored agent: %w", err)
	}
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
