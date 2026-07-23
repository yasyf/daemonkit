package daemon

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const exactStateSchema = 1

// ExactStateCodec binds one payload type to its exact on-disk identity.
type ExactStateCodec[T any] struct {
	Identity    string
	Fingerprint string
	New         func() (T, error)
	Encode      func(T) (json.RawMessage, error)
	Decode      func(json.RawMessage) (T, error)
}

// ExactStateFile is a typed JSON state file with a caller-owned exact codec.
type ExactStateFile[T any] struct {
	Path  string
	Codec ExactStateCodec[T]
}

type exactStateEnvelope struct {
	Identity    string          `json:"identity"`
	Schema      uint64          `json:"schema"`
	Fingerprint string          `json:"fingerprint"`
	Payload     json.RawMessage `json:"payload"`
}

// Read decodes the exact current file or returns Codec.New for an absent file.
func (s ExactStateFile[T]) Read() (T, error) {
	return s.read()
}

// Update takes the state file's flock and applies mutate, returning
// proc.ErrLockBusy when another writer holds it.
func (s ExactStateFile[T]) Update(ctx context.Context, mutate func(*T) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock, err := (proc.FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     proc.FileLockExclusive,
		Deadline: time.Second,
	}).TryAcquire()
	if err != nil {
		return err
	}
	defer lock.Close()
	return s.UpdateUnlocked(mutate)
}

// UpdateUnlocked applies mutate WITHOUT taking the flock — the flock is
// non-reentrant, so a caller already holding it must use this.
func (s ExactStateFile[T]) UpdateUnlocked(mutate func(*T) error) error {
	state, err := s.read()
	if err != nil {
		return err
	}
	if err := mutate(&state); err != nil {
		return fmt.Errorf("mutate state %s: %w", s.Path, err)
	}
	return s.write(state)
}

// ReplaceUnlocked writes state without taking the file lock or reading prior state.
func (s ExactStateFile[T]) ReplaceUnlocked(state T) error {
	return s.write(state)
}

func (s ExactStateFile[T]) read() (T, error) {
	var zero T
	if err := s.validate(); err != nil {
		return zero, err
	}
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return s.Codec.New()
	}
	if err != nil {
		return zero, fmt.Errorf("read state %s: %w", s.Path, err)
	}
	var envelope exactStateEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return zero, fmt.Errorf("decode state %s: %w", s.Path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return zero, fmt.Errorf("decode state %s: trailing JSON", s.Path)
	}
	if envelope.Identity != s.Codec.Identity || envelope.Schema != exactStateSchema ||
		envelope.Fingerprint != s.Codec.Fingerprint {
		return zero, fmt.Errorf("decode state %s: foreign schema identity", s.Path)
	}
	if len(envelope.Payload) == 0 || bytes.Equal(envelope.Payload, []byte("null")) {
		return zero, fmt.Errorf("decode state %s: payload is required", s.Path)
	}
	state, err := s.Codec.Decode(envelope.Payload)
	if err != nil {
		return zero, fmt.Errorf("decode state %s payload: %w", s.Path, err)
	}
	return state, nil
}

func (s ExactStateFile[T]) write(state T) error {
	if err := s.validate(); err != nil {
		return err
	}
	payload, err := s.Codec.Encode(state)
	if err != nil {
		return fmt.Errorf("encode state %s payload: %w", s.Path, err)
	}
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) || !json.Valid(payload) {
		return fmt.Errorf("encode state %s payload: exact codec returned invalid JSON", s.Path)
	}
	data, err := json.Marshal(exactStateEnvelope{
		Identity: s.Codec.Identity, Schema: exactStateSchema,
		Fingerprint: s.Codec.Fingerprint, Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("encode state %s: %w", s.Path, err)
	}
	return WriteFileDurable(s.Path, data, 0o600)
}

func (s ExactStateFile[T]) validate() error {
	if !filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path || s.Path == string(filepath.Separator) {
		return errors.New("daemon: exact state path must be absolute, clean, and non-root")
	}
	if s.Codec.Identity == "" || s.Codec.New == nil ||
		s.Codec.Encode == nil || s.Codec.Decode == nil {
		return errors.New("daemon: exact state file configuration is incomplete")
	}
	fingerprint, err := hex.DecodeString(s.Codec.Fingerprint)
	if err != nil || len(fingerprint) != 32 || hex.EncodeToString(fingerprint) != s.Codec.Fingerprint {
		return errors.New("daemon: exact state fingerprint must be 32 lowercase hex bytes")
	}
	return nil
}

// WriteFileDurable writes data to path via write-temp, fsync, rename, then
// fsyncs the directory and every new ancestor link: a power loss leaves the
// previous contents or data, never a truncated or lost file.
func WriteFileDurable(path string, data []byte, perm os.FileMode) error {
	return writeFileDurable(path, data, perm, SyncDir)
}

func writeFileDurable(path string, data []byte, perm os.FileMode, syncDir func(string) error) error {
	dir := filepath.Dir(path)
	if err := mkdirAllDurable(dir, 0o700, syncDir); err != nil {
		return fmt.Errorf("create dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".durable-*")
	if err != nil {
		return fmt.Errorf("create temp %s: %w", path, err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp %s: %w", path, err)
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod temp %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("fsync temp %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename %s into place: %w", path, err)
	}
	return syncDir(dir)
}

func mkdirAllDurable(path string, perm os.FileMode, syncDir func(string) error) error {
	if _, err := os.Stat(path); err == nil {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("fsync parent of %s: %w", path, err)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if err := mkdirAllDurable(parent, perm, syncDir); err != nil {
		return err
	}
	if err := os.Mkdir(path, perm); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := syncDir(parent); err != nil {
		return fmt.Errorf("fsync parent of %s: %w", path, err)
	}
	return nil
}

// SyncDir fsyncs a directory so entry creations, renames, and removals in it
// survive a power loss.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", dir, err)
	}
	return nil
}
