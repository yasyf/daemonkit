package drain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Simple drains a request daemon on upgrade.
type Simple struct {
	intake Intake
}

// Admit admits one request at frame receipt; ErrDraining once Drain has begun.
func (s *Simple) Admit() (done func(), err error) { return s.intake.Admit() }

// Draining reports whether Drain has begun.
func (s *Simple) Draining() bool { return s.intake.Draining() }

// SimpleConfig wires Drain's ordered teardown; every field is required.
type SimpleConfig struct {
	Deactivate      func(ctx context.Context) error
	MarkClosing     func()
	CancelExecutors func()
}

// Drain runs the normative request-daemon order: stop intake, settle, mark pools closing, cancel.
func (s *Simple) Drain(ctx context.Context, cfg SimpleConfig) error {
	s.intake.Close()
	if err := cfg.Deactivate(ctx); err != nil {
		return fmt.Errorf("drain: deactivate intake: %w", err)
	}
	if err := s.intake.Settle(ctx); err != nil {
		return fmt.Errorf("drain: settle admitted requests: %w", err)
	}
	cfg.MarkClosing()
	cfg.CancelExecutors()
	return nil
}

// Stamps claims content-hash dedupe keys via O_CREATE|O_EXCL stamp files.
type Stamps struct {
	Dir string
}

// Claim reports whether the request carrying key should execute: false only
// on a proven duplicate (stamp exists); any FS error fails open and executes.
func (s Stamps) Claim(key string) bool {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return true
	}
	sum := sha256.Sum256([]byte(key))
	path := filepath.Join(s.Dir, hex.EncodeToString(sum[:])+".stamp")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false
	}
	if err != nil {
		return true
	}
	f.Close()
	return true
}
