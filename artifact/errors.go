package artifact

import (
	"errors"
	"fmt"
)

var (
	// ErrSchemaVersion means a descriptor declares a schema this runner does not
	// support.
	ErrSchemaVersion = errors.New("artifact: unsupported descriptor schema")
	// ErrInvalidDescriptor means a descriptor is malformed or incomplete.
	ErrInvalidDescriptor = errors.New("artifact: invalid descriptor")
	// ErrDynamicIntegrity means a descriptor uses a dynamic version for a kind
	// that has no independent integrity gate (a release-binary).
	ErrDynamicIntegrity = errors.New("artifact: dynamic version requires an independent integrity gate")
	// ErrUnsupportedPlatform means a descriptor has no entry for the running host.
	ErrUnsupportedPlatform = errors.New("artifact: no entry for the current platform")
	// ErrChecksumMismatch means a downloaded artifact's sha256 did not match the
	// descriptor.
	ErrChecksumMismatch = errors.New("artifact: artifact checksum mismatch")
	// ErrSizeMismatch means a downloaded artifact's size did not match the
	// descriptor.
	ErrSizeMismatch = errors.New("artifact: artifact size mismatch")
	// ErrUnsupportedFormat means a descriptor names an archive format this runner
	// cannot extract.
	ErrUnsupportedFormat = errors.New("artifact: unsupported artifact format")
	// ErrUnsafeArchive means an archive entry would escape the extraction root or
	// carries a link this runner refuses to materialize.
	ErrUnsafeArchive = errors.New("artifact: unsafe archive entry")
	// ErrManualUpgrade means an attested signed app is missing or stale and must
	// be upgraded out of band. Inspect a ManualUpgradeError for the handoff.
	ErrManualUpgrade = errors.New("artifact: signed app requires a manual upgrade")
)

// ManualUpgradeError is the typed attest failure a caller renders as a
// "brew upgrade --cask <cask>" handoff. It matches ErrManualUpgrade via errors.Is.
type ManualUpgradeError struct {
	Name string
	Cask string
	Want string // descriptor version ("" when the version is host-authoritative)
	Got  string // installed version ("" when the app is absent)
}

func (e *ManualUpgradeError) Error() string {
	if e.Got == "" {
		return fmt.Sprintf("artifact: signed app %q is not installed; run: brew upgrade --cask %s", e.Name, e.Cask)
	}
	return fmt.Sprintf("artifact: signed app %q is version %s, want %s; run: brew upgrade --cask %s", e.Name, e.Got, e.Want, e.Cask)
}

// Is reports whether target is ErrManualUpgrade.
func (e *ManualUpgradeError) Is(target error) bool {
	return target == ErrManualUpgrade
}
