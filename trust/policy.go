// Package trust verifies the code-signing identity of a connected unix-socket
// peer. A Policy enforces a same-UID floor on every platform and, on a signed
// darwin build, a designated requirement pinning the peer's Team ID and signing
// identifier against its audit token. A configured Requirement with no available
// verifier fails closed — it never silently degrades to UID-only. UID-only trust
// exists solely in unsigned local-test builds selected by the daemonkit_unsigned
// build tag, which release CI must reject.
package trust

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/yasyf/daemonkit/wire"
)

// ErrUntrustedPeer is returned when a peer fails the trust check.
var ErrUntrustedPeer = errors.New("trust: untrusted peer")

// ErrNoVerifier is returned when a Requirement is configured but no code-identity
// verifier is available (a non-darwin build, or a darwin build with no audit
// token). It is a fail-closed denial, never a downgrade to UID-only.
var ErrNoVerifier = errors.New("trust: no code-identity verifier for a configured requirement")

// Requirement pins a peer's code signature to a Developer ID identity. Both
// TeamID and Identifier are mandatory: a TeamID-only requirement would grant
// every binary on the team authority over every socket, and an identifier-only
// requirement is not globally unique across teams.
type Requirement struct {
	// TeamID is the Apple Developer Team ID, e.g. "SXKCTF23Q2".
	TeamID string
	// Identifier is the signing identifier (bundle id) the peer must carry.
	Identifier string
	// AllowUnhardened skips the Hardened Runtime, library-validation, and
	// injection-entitlement demands for a consumer that ships an audited
	// third-party dylib it cannot library-validate (fusekit-holder with
	// libfuse-t). It is the ONE bypass, and it never relaxes the designated
	// requirement or the same-UID floor. Off by default; enabling it is a
	// security decision.
	AllowUnhardened bool
}

// validate rejects an incompletely pinned Requirement before it can be used.
func (r Requirement) validate() error {
	if strings.TrimSpace(r.TeamID) == "" {
		return errors.New("trust: Requirement.TeamID is required")
	}
	if strings.TrimSpace(r.Identifier) == "" {
		return errors.New("trust: Requirement.Identifier is required (a TeamID-only requirement is same-team lateral authority)")
	}
	if strings.ContainsAny(r.TeamID, `"\`) || strings.ContainsAny(r.Identifier, `"\`) {
		return errors.New("trust: Requirement fields must not contain quotes or backslashes")
	}
	return nil
}

// DRString renders the canonical designated requirement: a Developer ID anchor
// (Apple root + Developer ID CA + Developer ID Application leaf OIDs) pinned to
// the quoted Team ID and signing identifier. It never uses a cdhash (which
// pins one build) or a TeamID-only clause (same-team lateral authority).
func (r Requirement) DRString() (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	// 1.2.840.113635.100.6.2.6 = Developer ID CA; 1.2.840.113635.100.6.1.13 =
	// Developer ID Application leaf. Together they mean "Developer ID", excluding
	// Mac App Store and development signatures.
	return fmt.Sprintf(
		`identifier "%s" and anchor apple generic and certificate leaf[subject.OU] = "%s" `+
			`and certificate 1[field.1.2.840.113635.100.6.2.6] exists `+
			`and certificate leaf[field.1.2.840.113635.100.6.1.13] exists`,
		r.Identifier, r.TeamID,
	), nil
}

// Policy verifies a peer against an optional Requirement.
type Policy struct {
	// Requirement, when non-nil, must be satisfied by the peer's code signature.
	// nil means UID-only: the same-UID floor is the whole check.
	Requirement *Requirement
}

// Check enforces the same-effective-UID floor on every platform, then — when a
// Requirement is configured — the designated requirement against the peer's
// audit token. A configured Requirement with no verifier fails closed
// (ErrNoVerifier).
//
// LOCAL_PEERTOKEN resolves the peer socket's process at query time, so it does
// NOT bind identity to the connector: a same-UID peer can fork a holder and exec
// a genuine signed binary to lend its identity, and the floor does not confine
// fd delegation across a setuid/SCM_RIGHTS boundary. This is a known-unsound
// binding for code authentication; a surface needing a real per-message identity
// guarantee uses XPC (setCodeSigningRequirement), not this Policy.
func (p Policy) Check(peer wire.Peer) error {
	if peer.UID != os.Geteuid() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, peer.UID, os.Geteuid())
	}
	if p.Requirement == nil {
		return nil
	}
	if err := p.Requirement.validate(); err != nil {
		return err
	}
	return verifyRequirement(peer, *p.Requirement)
}
