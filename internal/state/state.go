// Package state is daemonkit's durable-file primitive: a write lands whole or
// not at all, and a read yields the identity cores a file recorded even when
// nothing else about it can be understood.
//
// A state file is a frozen frame — magic, schema, envelope, payload — whose
// envelope carries one Core per recorded process and is never re-specified.
// A file this era cannot read moves aside and the read still succeeds with the
// cores it extracted, so state written by a broken era can neither block the
// release that repairs it nor orphan the children it recorded.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
)

// ErrUnbound refuses every operation on a File that New never bound: a nil
// handle and the zero File both name no path, and a file that was never looked
// for reads as a refusal, never as fresh state.
var ErrUnbound = errors.New("state: unbound file")

// Schema is a state file's payload generation. A file declaring any other
// schema moves aside: there is no migration and no partial read.
type Schema uint32

// Core is the frozen identity of one recorded process. Its encoding never
// changes — Session, omitted for anything but a dedicated-session leader, is
// the frozen form's one additive field — so any later era extracts it from a
// file whose payload it cannot decode, group authority included.
type Core struct {
	PID        int    `json:"pid"`
	Start      uint64 `json:"start"`
	Boot       uint64 `json:"boot"`
	Generation uint64 `json:"generation"`
	Session    int    `json:"session,omitempty"`
}

// Cored is a payload that names the identities it records. Cores are read off
// the payload at every write, so the envelope a later era extracts cannot
// disagree with the payload it was written beside.
type Cored interface{ Cores() []Core }

// File is one durable state file. Path, schema, and payload type bind once at
// New, so no call site can restate them and disagree.
type File[T Cored] struct {
	path   string
	schema Schema
}

// New binds a state file and performs no I/O.
func New[T Cored](path string, schema Schema) *File[T] {
	return &File[T]{path: path, schema: schema}
}

func (f *File[T]) at() (bound, error) {
	if f == nil || f.path == "" {
		return "", ErrUnbound
	}
	return bound(f.path), nil
}

// Loaded is one read. Archived names where an unusable file was moved and is
// empty otherwise; Cores names every identity the file recorded either way, so
// a reap sweep reads one field and never branches on the file's fate.
type Loaded[T Cored] struct {
	Value    T
	Cores    []Core
	Archived string
}

// Load reads the file. A missing file, an unknown schema, a truncated frame,
// and an undecodable payload are outcomes, never errors: only an unbound file,
// the filesystem refusing the read, or a failed move-aside is an error.
func (f *File[T]) Load() (Loaded[T], error) {
	at, err := f.at()
	if err != nil {
		return Loaded[T]{}, err
	}
	raw, err := at.read()
	if errors.Is(err, fs.ErrNotExist) {
		return Loaded[T]{}, nil
	}
	if err != nil {
		return Loaded[T]{}, err
	}
	schema, cores, payload := scan(raw)
	if schema == f.schema {
		var value T
		if err := json.Unmarshal(payload, &value); err == nil {
			return Loaded[T]{Value: value, Cores: cores}, nil
		}
	}
	archived, err := at.archive()
	if err != nil {
		return Loaded[T]{}, err
	}
	return Loaded[T]{Cores: cores, Archived: archived}, nil
}

// Peek reads the file without owning it: no archive, no repair, no write —
// the shared reader's discipline, safe beside a live writer holding the
// flock. ok is false for a missing, foreign-schema, or undecodable file; the
// extracted cores are still returned either way.
func (f *File[T]) Peek() (Loaded[T], bool, error) {
	at, err := f.at()
	if err != nil {
		return Loaded[T]{}, false, err
	}
	raw, err := at.read()
	if errors.Is(err, fs.ErrNotExist) {
		return Loaded[T]{}, false, nil
	}
	if err != nil {
		return Loaded[T]{}, false, err
	}
	schema, cores, payload := scan(raw)
	if schema == f.schema {
		var value T
		if err := json.Unmarshal(payload, &value); err == nil {
			return Loaded[T]{Value: value, Cores: cores}, true, nil
		}
	}
	return Loaded[T]{Cores: cores}, false, nil
}

// Store replaces the file with v: write a temp, fsync it, rename it into
// place, fsync the directory. A crash leaves the previous contents or v.
func (f *File[T]) Store(v T) error {
	at, err := f.at()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("state: encode %s: %w", at, err)
	}
	raw, err := frame(f.schema, v.Cores(), payload)
	if err != nil {
		return fmt.Errorf("state: frame %s: %w", at, err)
	}
	return at.write(raw)
}
