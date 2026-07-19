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

const appGroupsEntitlement = "com.apple.security.application-groups"

// EntitlementMatch is one closed required-entitlement predicate.
type EntitlementMatch uint8

const (
	// EntitlementBoolean requires an exact CFBoolean value.
	EntitlementBoolean EntitlementMatch = iota + 1
	// EntitlementString requires an exact string value.
	EntitlementString
	// EntitlementStringArrayContains requires membership in a string array.
	EntitlementStringArrayContains
)

// EntitlementRequirement is one typed entitlement predicate.
type EntitlementRequirement struct {
	Match   EntitlementMatch
	Boolean bool
	String  string
}

// Requirement pins a peer's code signature and mandatory capabilities.
type Requirement struct {
	TeamID               string
	SigningIdentifier    string
	RequiredAppGroup     string
	RequiredEntitlements map[string]EntitlementRequirement
}

func (r Requirement) validate() error {
	if strings.TrimSpace(r.TeamID) == "" {
		return errors.New("trust: Requirement.TeamID is required")
	}
	if strings.TrimSpace(r.SigningIdentifier) == "" {
		return errors.New("trust: Requirement.SigningIdentifier is required (a TeamID-only requirement is same-team lateral authority)")
	}
	if strings.ContainsAny(r.TeamID, `"\`) || strings.ContainsAny(r.SigningIdentifier, `"\`) {
		return errors.New("trust: Requirement fields must not contain quotes or backslashes")
	}
	if r.RequiredAppGroup != "" {
		if _, exists := r.RequiredEntitlements[appGroupsEntitlement]; exists {
			return errors.New("trust: application-groups is specified by both RequiredAppGroup and RequiredEntitlements")
		}
	}
	for key, requirement := range r.RequiredEntitlements {
		if strings.TrimSpace(key) == "" {
			return errors.New("trust: required entitlement key is empty")
		}
		switch requirement.Match {
		case EntitlementBoolean:
		case EntitlementString, EntitlementStringArrayContains:
			if requirement.String == "" {
				return fmt.Errorf("trust: required entitlement %q has an empty string value", key)
			}
		default:
			return fmt.Errorf("trust: required entitlement %q has unknown match %d", key, requirement.Match)
		}
	}
	return nil
}

func (r Requirement) entitlementRequirements() map[string]EntitlementRequirement {
	requirements := make(map[string]EntitlementRequirement, len(r.RequiredEntitlements)+1)
	for key, requirement := range r.RequiredEntitlements {
		requirements[key] = requirement
	}
	if r.RequiredAppGroup != "" {
		requirements[appGroupsEntitlement] = EntitlementRequirement{
			Match: EntitlementStringArrayContains, String: r.RequiredAppGroup,
		}
	}
	return requirements
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
		r.SigningIdentifier, r.TeamID,
	), nil
}

// Policy verifies a peer against an optional Requirement.
type Policy struct {
	// Requirement, when nil, means UID-only: the same-UID floor is the whole check.
	Requirement *Requirement
}

// Check enforces the same-effective-UID floor, then any configured
// Requirement against the peer's audit token; a Requirement with no verifier
// fails closed (ErrNoVerifier). LOCAL_PEERTOKEN is a query-time process
// reference, not an immutable record of the connector. Descriptor delegation
// or substitution by another process satisfying the same signed policy before
// this check remains a platform limitation.
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
