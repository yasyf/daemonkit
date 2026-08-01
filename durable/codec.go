package durable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// Validating is the contract every durable payload type carries: a concrete
// struct with json tags whose Validate rejects any value the type's own
// invariants do not name. Identity and version live inside the payload as
// ordinary fields, checked by Validate — no envelope, no fingerprint.
type Validating interface{ Validate() error }

// Marshal validates v and encodes it as compact JSON with a trailing newline.
func Marshal[T Validating](v T) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, fmt.Errorf("durable: validate: %w", err)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("durable: encode: %w", err)
	}
	return append(data, '\n'), nil
}

// Unmarshal strictly decodes data into a T and validates it: unknown fields,
// trailing values, and duplicate object keys at any depth are all rejected.
// It reads bytes, not files — the same codec serves a file, a database column,
// or a wire payload.
func Unmarshal[T Validating](data []byte) (T, error) {
	v, err := unmarshal[T](data)
	if err != nil {
		return v, fmt.Errorf("durable: %w", err)
	}
	return v, nil
}

// ReadFile reads and strictly decodes the state at path. os.ErrNotExist
// reaches the caller unwrapped: absence is a state every ladder branches on —
// default it, refuse it, or initialize it, at the call site. Corruption is
// never absence: any other failure is an error, so a torn file cannot be
// silently replaced by a default.
func ReadFile[T Validating](path string) (T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		var zero T
		return zero, err
	}
	v, err := unmarshal[T](data)
	if err != nil {
		return v, fmt.Errorf("durable: read %s: %w", path, err)
	}
	return v, nil
}

func unmarshal[T Validating](data []byte) (T, error) {
	var zero T
	if err := rejectDuplicateKeys(json.NewDecoder(bytes.NewReader(data))); err != nil {
		return zero, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var v T
	if err := decoder.Decode(&v); err != nil {
		return zero, fmt.Errorf("decode: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return zero, errors.New("decode: trailing JSON")
	}
	if err := v.Validate(); err != nil {
		return zero, fmt.Errorf("validate: %w", err)
	}
	return v, nil
}

func rejectDuplicateKeys(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			name := key.(string)
			if seen[name] {
				return fmt.Errorf("decode: duplicate object key %q", name)
			}
			seen[name] = true
			if err := rejectDuplicateKeys(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := rejectDuplicateKeys(decoder); err != nil {
				return err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
