// Package trust verifies the code-signing identity of a connected unix-socket
// peer: a same-UID floor on every platform plus, on signed darwin builds, a
// designated requirement checked against the peer's audit token. A configured
// Requirement with no verifier fails closed, never downgrading to UID-only; a
// nil Requirement is explicit UID-only trust (the floor alone). The
// daemonkit_unsigned build tag drops the verifier for local-test builds,
// which release CI must reject.
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

// ErrNoVerifier is the fail-closed denial when a Requirement is configured but
// no code-identity verifier is available — never a downgrade to UID-only.
var ErrNoVerifier = errors.New("trust: no code-identity verifier for a configured requirement")

// Requirement pins a peer's code signature to a Developer ID identity. Both
// fields are mandatory: TeamID-only grants every team binary authority over
// the socket; identifier-only is not globally unique across teams.
type Requirement struct {
	TeamID     string
	Identifier string
	// AllowUnhardened skips the Hardened Runtime / library-validation /
	// injection-entitlement demands, for a consumer shipping an audited dylib
	// it cannot library-validate. The ONE bypass; never relaxes the DR or the
	// UID floor. Enabling it is a security decision.
	AllowUnhardened bool
}

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

// DRString renders the canonical designated requirement: the Developer ID
// anchor pinned to the quoted Team ID and signing identifier — never a cdhash
// (pins one build) or a TeamID-only clause (same-team lateral authority).
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
	// Requirement, when nil, means UID-only: the same-UID floor is the whole check.
	Requirement *Requirement
}

// Check enforces the same-effective-UID floor, then any configured
// Requirement against the peer's audit token; a Requirement with no verifier
// fails closed (ErrNoVerifier). LOCAL_PEERTOKEN binds at query time — a
// known-unsound binding against same-UID fork/exec identity substitution; a
// surface needing a real per-message identity guarantee uses XPC.
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
