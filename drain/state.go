package drain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/yasyf/daemonkit/daemon"
)

const (
	journalStateIdentity    = "daemonkit.drain.journal.v1"
	journalStateFingerprint = "041bf534940f6f73947eeb4dbc75b8ca2fcb344190d67a11438afd117e2ec561"
	strikeStateIdentity     = "daemonkit.drain.strike.v1"
	strikeStateFingerprint  = "4397346f748d73e36ecd548007ce5c6a9eaa4c03a9540bfa6682005d7aab5f3a"
	ownerStateIdentity      = "daemonkit.drain.owner.v1"
	ownerStateFingerprint   = "190afaed6df0b33ef525d73cd31a31d68eabb156936075544719a15011883c5b"
)

type journalState struct {
	Rows       map[string]Row    `json:"rows"`
	Sequence   uint64            `json:"sequence"`
	Transition *transitionRecord `json:"transition"`
	Complete   string            `json:"complete"`
}

func newJournalState() journalState {
	return journalState{Rows: make(map[string]Row)}
}

func journalStateFile(path string) daemon.ExactStateFile[journalState] {
	return daemon.ExactStateFile[journalState]{
		Path: path,
		Codec: daemon.ExactStateCodec[journalState]{
			Identity: journalStateIdentity, Fingerprint: journalStateFingerprint,
			New: func() (journalState, error) { return newJournalState(), nil },
			Encode: func(state journalState) (json.RawMessage, error) {
				return encodeExactPayload(state, validateJournalState)
			},
			Decode: func(raw json.RawMessage) (journalState, error) {
				return decodeExactPayload(raw, []string{"rows", "sequence", "transition", "complete"}, validateJournalState)
			},
		},
	}
}

func validateJournalState(state journalState) error {
	if state.Rows == nil {
		return errors.New("journal rows are required")
	}
	for key, row := range state.Rows {
		if key == "" || string(row.Key) != key || row.Seq == 0 {
			return fmt.Errorf("journal row %q has invalid identity", key)
		}
		if row.State != RowPending && row.State != RowYielded {
			return fmt.Errorf("journal row %q has invalid state %q", key, row.State)
		}
	}
	if state.Transition != nil {
		if err := validateTransition(*state.Transition); err != nil {
			return err
		}
	}
	return nil
}

func validateTransition(record transitionRecord) error {
	if record.Generation == "" {
		return errors.New("active transition generation is required")
	}
	if err := validateOwner(record.Owner); err != nil {
		return fmt.Errorf("active transition owner: %w", err)
	}
	if record.Step < 0 || record.Step > StepSpawn {
		return fmt.Errorf("active transition step %d is invalid", record.Step)
	}
	return nil
}

func strikeStateFile(path string) daemon.ExactStateFile[strikeState] {
	return daemon.ExactStateFile[strikeState]{
		Path: path,
		Codec: daemon.ExactStateCodec[strikeState]{
			Identity: strikeStateIdentity, Fingerprint: strikeStateFingerprint,
			New: func() (strikeState, error) { return strikeState{Times: []time.Time{}}, nil },
			Encode: func(state strikeState) (json.RawMessage, error) {
				return encodeExactPayload(state, validateStrikeState)
			},
			Decode: func(raw json.RawMessage) (strikeState, error) {
				return decodeExactPayload(raw, []string{"times", "level", "parked_until"}, validateStrikeState)
			},
		},
	}
}

func validateStrikeState(state strikeState) error {
	if state.Times == nil {
		return errors.New("strike times are required")
	}
	if state.Level < 0 {
		return errors.New("strike level is negative")
	}
	return nil
}

func ownerStateFile(path string) daemon.ExactStateFile[ownerFile] {
	return daemon.ExactStateFile[ownerFile]{
		Path: path,
		Codec: daemon.ExactStateCodec[ownerFile]{
			Identity: ownerStateIdentity, Fingerprint: ownerStateFingerprint,
			New: func() (ownerFile, error) { return ownerFile{}, os.ErrNotExist },
			Encode: func(state ownerFile) (json.RawMessage, error) {
				return encodeExactPayload(state, validateOwnerFile)
			},
			Decode: func(raw json.RawMessage) (ownerFile, error) {
				return decodeExactPayload(raw, []string{"pid", "start_time", "comm", "boot", "inc"}, validateOwnerFile)
			},
		},
	}
}

func validateOwnerFile(state ownerFile) error {
	if err := validateOwner(state.ownerRecord); err != nil {
		return err
	}
	if state.Inc == "" {
		return errors.New("owner incarnation is required")
	}
	return nil
}

func validateOwner(owner ownerRecord) error {
	if owner.PID <= 0 || owner.StartTime == "" || owner.Comm == "" || owner.Boot == "" {
		return errors.New("owner identity is incomplete")
	}
	return nil
}

func encodeExactPayload[T any](state T, validate func(T) error) (json.RawMessage, error) {
	if err := validate(state); err != nil {
		return nil, err
	}
	return json.Marshal(state)
}

func decodeExactPayload[T any](
	raw json.RawMessage,
	fields []string,
	validate func(T) error,
) (T, error) {
	var zero T
	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&object); err != nil {
		return zero, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return zero, errors.New("payload contains trailing JSON")
	}
	if len(object) != len(fields) {
		return zero, errors.New("payload field set is not exact")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return zero, fmt.Errorf("payload field %q is required", field)
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state T
	if err := decoder.Decode(&state); err != nil {
		return zero, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return zero, errors.New("payload contains trailing JSON")
	}
	if err := validate(state); err != nil {
		return zero, err
	}
	return state, nil
}
