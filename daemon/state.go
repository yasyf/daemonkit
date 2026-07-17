package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/yasyf/daemonkit/proc"
)

// StateFile is a JSON daemon-state file at a caller path, guarded by a proc
// TryLock on Path+".lock". Update takes the lock; UpdateUnlocked skips it for a
// caller already inside the critical section (the flock is non-reentrant). Both
// preserve keys the mutate closure does not touch byte-for-byte.
type StateFile struct {
	// Path is the JSON state file.
	Path string
}

// Mutate edits the decoded state in place; keys it leaves untouched survive the
// write byte-for-byte.
type Mutate func(state map[string]json.RawMessage) error

// Update takes the state file's flock and applies mutate, returning
// proc.ErrLockBusy when another writer holds it.
func (s StateFile) Update(ctx context.Context, mutate Mutate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock, err := proc.TryLock(s.Path + ".lock")
	if err != nil {
		return err
	}
	defer lock.Release()
	return s.UpdateUnlocked(mutate)
}

// UpdateUnlocked applies mutate WITHOUT taking the flock, for a caller already
// holding it — the flock is non-reentrant, so a locked caller must use this.
func (s StateFile) UpdateUnlocked(mutate Mutate) error {
	state, err := s.read()
	if err != nil {
		return err
	}
	if err := mutate(state); err != nil {
		return fmt.Errorf("mutate state %s: %w", s.Path, err)
	}
	return s.write(state)
}

func (s StateFile) read() (map[string]json.RawMessage, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) || len(data) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state %s: %w", s.Path, err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parse state %s: %w", s.Path, err)
	}
	if state == nil {
		state = map[string]json.RawMessage{}
	}
	return state, nil
}

// write re-encodes the whole map atomically via encodeState, which writes each
// value's bytes verbatim so untouched foreign keys survive byte-for-byte.
func (s StateFile) write(state map[string]json.RawMessage) error {
	data, err := encodeState(state)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.Path), ".state-*")
	if err != nil {
		return fmt.Errorf("create temp state: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp state: %w", err)
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename state into place: %w", err)
	}
	return nil
}

// encodeState serializes state under sorted keys with each value written
// verbatim, so untouched keys round-trip byte-for-byte (json.Marshal of a
// RawMessage HTML-escapes <>& and compacts whitespace). An invalid value errors.
func encodeState(state map[string]json.RawMessage) ([]byte, error) {
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		v := state[k]
		if len(v) == 0 {
			buf.WriteString("null")
			continue
		}
		if !json.Valid(v) {
			return nil, fmt.Errorf("state key %q holds invalid JSON", k)
		}
		buf.Write(v)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
