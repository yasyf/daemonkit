package deploy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/daemonkit/durable"
)

const (
	swapIdentity       = "daemonkit.deploy.swap.v1"
	activationIdentity = "daemonkit.deploy.activation.v1"
	removalIdentity    = "daemonkit.deploy.removal.v1"
	serviceIdentity    = "daemonkit.deploy.services.v1"
	recordSchema       = 1

	metadataDir       = ".daemonkit-deploy"
	legacyMetadataDir = ".daemonkit-deployment"
)

// swapRecord names the one fact a resume cannot recompute: which generation
// the canonical path held before the rename pair began, since a superseded
// bundle's bytes are unrecoverable from bare stat once they move. Which of
// the two renames landed is deliberately absent — settle re-derives it from
// stat plus a codesign re-verify on every call, so a record can never
// disagree with the filesystem it describes.
type swapRecord struct {
	Identity  string      `json:"identity"`
	Schema    int         `json:"schema"`
	Target    string      `json:"target"`
	Prior     *Generation `json:"prior,omitempty"`
	Candidate Generation  `json:"candidate"`
}

func (r swapRecord) validate() error {
	if r.Identity != swapIdentity || r.Schema != recordSchema || r.Target != r.Candidate.Path {
		return ErrState
	}
	if err := r.Candidate.validate(); err != nil {
		return err
	}
	if r.Prior == nil {
		return nil
	}
	if r.Prior.Path != r.Target {
		return ErrState
	}
	return r.Prior.validate()
}

// activationRecord seals what one converged generation proved about itself.
// The proof is durable because it is evidence about a moment, not a state of
// the filesystem: nothing on disk can reconstruct it after the fact.
type activationRecord struct {
	Identity   string      `json:"identity"`
	Schema     int         `json:"schema"`
	Generation Generation  `json:"generation"`
	Readiness  storedProof `json:"readiness"`
}

func (r activationRecord) validate() error {
	if r.Identity != activationIdentity || r.Schema != recordSchema ||
		r.Readiness.Build == "" || r.Readiness.Generation == 0 || !validDigest(r.Readiness.Digest) {
		return ErrState
	}
	return r.Generation.validate()
}

// removalRecord is the tombstone: the generation that was removed and the
// absence proof that authorized removing it.
type removalRecord struct {
	Identity   string        `json:"identity"`
	Schema     int           `json:"schema"`
	Generation Generation    `json:"generation"`
	Runtime    storedRuntime `json:"runtime"`
}

func (r removalRecord) validate() error {
	if r.Identity != removalIdentity || r.Schema != recordSchema ||
		!r.Runtime.Absent || !validDigest(r.Runtime.Digest) {
		return ErrState
	}
	return r.Generation.validate()
}

// serviceRecord names the exact labels this deployment last asked launchd to
// hold. It is the only thing a converge may take a label away from: daemonkit
// never asks the machine what it owns, so a label deploy did not write down is
// a label deploy will not touch.
type serviceRecord struct {
	Identity string   `json:"identity"`
	Schema   int      `json:"schema"`
	Labels   []string `json:"labels"`
}

func (r serviceRecord) validate() error {
	if r.Identity != serviceIdentity || r.Schema != recordSchema || len(r.Labels) == 0 {
		return ErrState
	}
	if !slices.IsSorted(r.Labels) || slices.Contains(r.Labels, "") ||
		len(slices.Compact(slices.Clone(r.Labels))) != len(r.Labels) {
		return ErrState
	}
	return nil
}

type storedProof struct {
	Build      string `json:"build"`
	Generation uint64 `json:"generation"`
	Digest     string `json:"digest"`
}

type storedRuntime struct {
	Absent     bool   `json:"absent"`
	Generation uint64 `json:"generation"`
	Digest     string `json:"digest"`
}

type validating interface{ validate() error }

// readRecord decodes path into value, refusing any byte the record's own type
// does not name. os.ErrNotExist reaches the caller unwrapped: absence is a
// state every ladder branches on, never an error to dress up.
func readRecord[T validating](path string, value *T) error {
	payload, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w: decode %s: %w", ErrState, path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing JSON in %s", ErrState, path)
	}
	return (*value).validate()
}

func writeRecord(path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("deploy: encode %s: %w", path, err)
	}
	if err := durable.WriteFile(path, append(payload, '\n'), 0o600); err != nil {
		return fmt.Errorf("deploy: persist %s: %w", path, err)
	}
	return nil
}

// layout is every path one deployment owns, all derived from the canonical
// app path so no two of them can disagree about which app they describe.
type layout struct {
	canonical  string
	metadata   string
	lock       string
	swap       string
	activation string
	removal    string
	services   string
	candidate  string
	prior      string
	removed    string
	legacy     string
}

func layoutFor(appPath string) layout {
	name := strings.TrimSuffix(filepath.Base(appPath), ".app")
	root := filepath.Dir(appPath)
	metadata := filepath.Join(root, metadataDir, name)
	return layout{
		canonical:  appPath,
		metadata:   metadata,
		lock:       filepath.Join(metadata, "deployment.lock"),
		swap:       filepath.Join(metadata, "swap.json"),
		activation: filepath.Join(metadata, "activation.json"),
		removal:    filepath.Join(metadata, "removal.json"),
		services:   filepath.Join(metadata, "services.json"),
		candidate:  filepath.Join(root, "."+name+".daemonkit-candidate.app"),
		prior:      filepath.Join(metadata, "prior.app"),
		removed:    filepath.Join(metadata, "removed.app"),
		legacy:     filepath.Join(root, legacyMetadataDir, name),
	}
}

func (l layout) ensureMetadata() error {
	root := filepath.Dir(l.canonical)
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return fmt.Errorf("%w: install directory is not a canonical real path", ErrConflict)
	}
	if err := l.archiveLegacy(); err != nil {
		return err
	}
	for _, dir := range []string{filepath.Dir(l.metadata), l.metadata} {
		if err := durable.Mkdir(dir, 0o700); err != nil {
			return fmt.Errorf("deploy: create metadata directory %q: %w", dir, err)
		}
		if err := requirePrivateDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}

// archiveLegacy moves this deployment's pre-rename metadata tree aside. Every
// record in it names fields this package no longer has, and readRecord refuses
// an unknown field, so the first read on an installed machine would fail hard
// rather than converge; a rename turns that into a clean re-install and keeps
// the old tree as evidence instead of destroying it. The legacy directory is
// shared by every product installed beside this one, so only this deployment's
// own subtree moves — a sibling an older binary still manages keeps the lock
// file path that binary opens by name.
//
// The name rotates because a downgrade to a pre-cut release recreates the tree,
// and nothing on either side of that downgrade removes an archive: an occupied
// name has to yield the next one rather than fail, or the re-upgrade wedges the
// deployment for good. os.Rename never replaces a directory, so the attempt is
// itself both the occupancy test and the one-shot — no stat precedes it, and
// two openers racing the same tree still produce one archive, the loser seeing
// the source gone rather than a free name.
func (l layout) archiveLegacy() error {
	archive := l.legacy + ".bak"
	for n := 2; ; n++ {
		switch err := durable.Rename(l.legacy, archive); {
		case err == nil, errors.Is(err, os.ErrNotExist):
			return nil
		case !errors.Is(err, os.ErrExist):
			return fmt.Errorf("deploy: archive legacy metadata %q: %w", l.legacy, err)
		}
		archive = fmt.Sprintf("%s.bak.%d", l.legacy, n)
	}
}
