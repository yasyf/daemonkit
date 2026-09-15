package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/yasyf/daemonkit/bundle"
)

// SHA256 is one exact bundle, entitlement, or proof digest.
type SHA256 [sha256.Size]byte

// ParseSHA256 parses one lowercase hexadecimal digest.
func ParseSHA256(raw string) (SHA256, error) {
	var digest SHA256
	if len(raw) != hex.EncodedLen(len(digest)) {
		return digest, errors.New("deploy: sha256 must contain 64 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return digest, fmt.Errorf("deploy: parse sha256: %w", err)
	}
	copy(digest[:], decoded)
	return digest, nil
}

// String returns the lowercase hexadecimal digest.
func (d SHA256) String() string { return hex.EncodeToString(d[:]) }

// Generation is one exact attested application generation: what codesign, the
// bundle's Info.plist, and its whole file tree all agreed the bytes at Path
// were, at one moment. CDHash is the fine-grained identity — it covers the
// code directory, entitlements included — and the designated requirement is
// the coarse trusted-publisher policy those bytes satisfied.
type Generation struct {
	Path                  string `json:"path"`
	Version               string `json:"version"`
	TeamID                string `json:"team_id"`
	SigningIdentifier     string `json:"signing_identifier"`
	DesignatedRequirement string `json:"designated_requirement"`
	CDHash                string `json:"cdhash"`
	EntitlementsDigest    string `json:"entitlements_digest"`
	BundleDigest          string `json:"bundle_digest"`
	FileID                FileID `json:"file_id"`
}

func (g Generation) validate() error {
	if !validAppPath(g.Path) || g.Version == "" || g.TeamID == "" || g.SigningIdentifier == "" ||
		g.DesignatedRequirement == "" || !validCDHash(g.CDHash) || !validDigest(g.EntitlementsDigest) ||
		!validDigest(g.BundleDigest) || g.FileID == (FileID{}) {
		return ErrState
	}
	return nil
}

// sameBytes compares two generations by everything except where they sit: the
// swap moves a bundle between three paths and three inodes without changing
// what it is.
func (g Generation) sameBytes(other Generation) bool {
	g.Path, g.FileID = "", FileID{}
	other.Path, other.FileID = "", FileID{}
	return reflect.DeepEqual(g, other)
}

// sameTree is sameBytes plus the inode, which is what "this rename already
// landed" actually means. A candidate byte-identical to the prior it replaces
// is the same bytes at the canonical path before either rename has happened,
// so bytes alone cannot answer the swap's completion question; a rename
// carries the inode along, so identity can.
func (g Generation) sameTree(other Generation) bool {
	return g.FileID == other.FileID && g.sameBytes(other)
}

type signatureAttestation struct {
	CDHash             string
	EntitlementsDigest SHA256
}

// inspect attests the bundle at appPath against the deployment's designated
// requirement and reports the version it declares. It is the only path to a
// Generation: nothing else in this package mints one, so no fact in one is
// ever a caller's claim about the bytes rather than an observation of them.
func (d *Deployment) inspect(ctx context.Context, appPath string) (Generation, error) {
	if err := validateCanonicalAppPath(appPath); err != nil {
		return Generation{}, err
	}
	signature, err := codesignVerifier{}.Verify(ctx, appPath, d.requirement)
	if err != nil {
		return Generation{}, fmt.Errorf("deploy: verify signed bundle: %w", err)
	}
	if !validCDHash(signature.CDHash) {
		return Generation{}, errors.New("deploy: verifier returned an invalid CDHash")
	}
	version, err := bundle.ShortVersion(appPath)
	if err != nil {
		return Generation{}, fmt.Errorf("deploy: read bundle version: %w", err)
	}
	tree, err := BundleDigest(appPath)
	if err != nil {
		return Generation{}, err
	}
	id, err := identifyPath(appPath)
	if err != nil {
		return Generation{}, fmt.Errorf("deploy: identify bundle: %w", err)
	}
	return Generation{
		Path: appPath, Version: version,
		TeamID: d.config.Requirement.TeamID, SigningIdentifier: d.config.Requirement.SigningIdentifier,
		DesignatedRequirement: d.requirement, CDHash: strings.ToLower(signature.CDHash),
		EntitlementsDigest: signature.EntitlementsDigest.String(), BundleDigest: tree.String(),
		FileID: id,
	}, nil
}

// attest re-verifies that the bundle at expected.Path is still byte-for-byte
// the generation named. Every irreversible filesystem step runs it first, so
// no rename ever moves bytes nobody just re-verified.
func (d *Deployment) attest(ctx context.Context, expected Generation) error {
	current, err := d.inspect(ctx, expected.Path)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, expected) {
		return fmt.Errorf("%w: bundle at %q changed", ErrConflict, expected.Path)
	}
	return nil
}

// BundleDigest hashes a bundle's whole tree under an os.Root scope, and
// refuses a file that moved beneath it: identity, size, mtime, and mode must
// agree across the walk's stat, a stat before the read, and a stat after it.
// Fields are length-prefixed so no two trees collide by concatenation.
//
// It computes the value [Candidate.Digest] names. That digest is a caller's
// claim, never an authority: every verb that takes a candidate re-derives the
// digest from the bytes it is about to move and refuses with [ErrConflict] on
// any disagreement.
func BundleDigest(root string) (SHA256, error) {
	h := sha256.New()
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return SHA256{}, fmt.Errorf("deploy: open bundle root: %w", err)
	}
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		writeDigestField(h, filepath.ToSlash(relative))
		writeDigestField(h, fmt.Sprintf("%#o", uint32(info.Mode())))
		switch {
		case info.IsDir():
			writeDigestField(h, "directory")
			return nil
		case info.Mode().IsRegular():
			writeDigestField(h, "regular")
			file, err := rootHandle.Open(relative)
			if err != nil {
				return err
			}
			before, statErr := file.Stat()
			content := sha256.New()
			size, copyErr := io.Copy(content, file)
			after, restatErr := file.Stat()
			closeErr := file.Close()
			if err := errors.Join(statErr, copyErr, restatErr, closeErr); err != nil {
				return err
			}
			if !os.SameFile(info, before) || !os.SameFile(before, after) || size != before.Size() ||
				before.Size() != after.Size() || before.ModTime() != after.ModTime() ||
				info.Mode() != before.Mode() || before.Mode() != after.Mode() {
				return fmt.Errorf("deploy: bundle file changed while digesting %q", path)
			}
			writeDigestField(h, fmt.Sprintf("%d", size))
			writeDigestField(h, hex.EncodeToString(content.Sum(nil)))
			return nil
		case info.Mode()&os.ModeSymlink != 0:
			writeDigestField(h, "symlink")
			target, err := rootHandle.Readlink(relative)
			if err != nil {
				return err
			}
			writeDigestField(h, target)
			return nil
		default:
			return fmt.Errorf("deploy: bundle tree contains unsupported entry %q", path)
		}
	})
	closeErr := rootHandle.Close()
	if err := errors.Join(walkErr, closeErr); err != nil {
		return SHA256{}, fmt.Errorf("deploy: digest bundle tree: %w", err)
	}
	var digest SHA256
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func writeDigestField(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

func validAppPath(appPath string) bool {
	return appPath != "" && filepath.IsAbs(appPath) && filepath.Clean(appPath) == appPath &&
		strings.HasSuffix(filepath.Base(appPath), ".app") && filepath.Base(appPath) != ".app"
}

func validateCanonicalAppPath(appPath string) error {
	if !validAppPath(appPath) {
		return fmt.Errorf("%w: app path must be an exact absolute .app path", ErrConfig)
	}
	resolved, err := filepath.EvalSymlinks(appPath)
	if err != nil {
		return fmt.Errorf("%w: resolve canonical app path: %w", ErrConflict, err)
	}
	if resolved != appPath {
		return fmt.Errorf("%w: canonical app path contains a symlink", ErrConflict)
	}
	return requireRealDirectory(appPath)
}

func requireRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("deploy: inspect directory %q: %w", path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %q is not a real directory", ErrConflict, path)
	}
	return nil
}

// requirePrivateDirectory holds a metadata directory to the terms the records
// inside it are authenticated by. The swap record is the strongest instruction
// deploy takes off disk — it names the bytes a rename destroys — and nothing
// signs it, so the directory permitting only its owner to write is the whole
// authentication. A directory this uid does not own, or one any other user can
// write, is refused rather than adopted.
func requirePrivateDirectory(path string) error {
	if err := requireRealDirectory(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("deploy: inspect directory %q: %w", path, err)
	}
	owner, err := directoryOwner(info)
	if err != nil {
		return err
	}
	if owner != os.Getuid() {
		return fmt.Errorf("%w: %q is owned by uid %d, not %d", ErrConflict, path, owner, os.Getuid())
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("%w: %q is mode %#o, which is not private to its owner", ErrConflict, path, perm)
	}
	return nil
}

func validCDHash(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validDigest(value string) bool {
	_, err := ParseSHA256(value)
	return err == nil
}

func fileExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}
