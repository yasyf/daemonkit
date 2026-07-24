package proc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// UnsupportedSchemaPolicy selects how a keyed store reacts to an on-disk schema
// it does not recognize. The zero value fails closed.
type UnsupportedSchemaPolicy uint8

const (
	unsupportedSchemaUnset UnsupportedSchemaPolicy = iota
	// FailUnsupportedSchema fails the open with ErrRecordSchema and leaves the
	// store on disk. It is the zero-value default.
	FailUnsupportedSchema
	// ArchiveUnsupportedSchema renames the offending store aside, opens a fresh
	// one, and logs one warning. No data is read or migrated.
	ArchiveUnsupportedSchema
)

// ArchiveUnsupportedStore renames an unsupported-schema keyed store aside as
// "<path>.<fingerprint>.<timestamp>.bak" and returns the backup path. The
// fingerprint is a short content digest so two distinct wedged stores never
// collide; the caller reopens a fresh store at path afterward. No data is read
// or migrated.
func ArchiveUnsupportedStore(path string) (string, error) {
	fingerprint, err := storeFingerprint(path)
	if err != nil {
		return "", err
	}
	backup := fmt.Sprintf("%s.%s.%s.bak", path, fingerprint, time.Now().UTC().Format("20060102T150405.000000000"))
	if err := os.Rename(path, backup); err != nil {
		return "", fmt.Errorf("proc: archive unsupported store: %w", err)
	}
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return "", fmt.Errorf("proc: persist archived store rename: %w", err)
	}
	return backup, nil
}

func storeFingerprint(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("proc: fingerprint unsupported store: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("proc: fingerprint unsupported store: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil))[:12], nil
}
