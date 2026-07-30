package trust

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"
)

const appGroupsEntitlement = "com.apple.security.application-groups"

const (
	entAllowJIT     = "com.apple.security.cs.allow-jit"
	entDisableLV    = "com.apple.security.cs.disable-library-validation"
	entDyldEnv      = "com.apple.security.cs.allow-dyld-environment-variables"
	entUnsignedExec = "com.apple.security.cs.allow-unsigned-executable-memory"
	entNoPageProt   = "com.apple.security.cs.disable-executable-page-protection"
	entGetTaskAllow = "com.apple.security.get-task-allow"
)

// injectionEntitlements re-open code injection or debugger attachment on a
// Hardened Runtime binary; a peer signed with any of them is untrusted.
var injectionEntitlements = []string{
	entDisableLV,
	entDyldEnv,
	entUnsignedExec,
	entAllowJIT,
	entNoPageProt,
	entGetTaskAllow,
}

// PolicyDigest is the canonical identity of one complete requirement.
type PolicyDigest [sha256.Size]byte

// EntitlementMatch is one closed required-entitlement predicate.
type EntitlementMatch uint8

const (
	// EntitlementBoolean requires an exact boolean value.
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
//
// A Requirement may only name a peer whose security-relevant behaviour is
// fully determined by its main Mach-O. Bundle resources, interpreted payloads,
// and bundle-relative plugin paths are outside the trust boundary: dynamic
// validation does not check the resource seal, so a same-UID attacker with no
// certificate can present a resource-tampered copy of a signed app that
// satisfies every clause here. SigningIdentifier is a team-scoped namespace,
// not a cryptographic binding — Apple's CA binds subject.OU to the team, and
// nothing to the CodeDirectory identifier — so uniqueness within the team's
// signed corpus is the consumer's obligation, and every build the team ever
// signed under that identifier is admitted, old versions included.
type Requirement struct {
	TeamID               string
	SigningIdentifier    string
	RequiredAppGroup     string
	RequiredEntitlements map[string]EntitlementRequirement

	// AllowJIT admits a peer signed with com.apple.security.cs.allow-jit. It
	// is not a bounded relaxation: allow-jit is the signature of a peer this
	// mechanism cannot authenticate, because the code the kernel hashed no
	// longer determines what the peer executes. A consumer setting it accepts
	// that residual by name. It is the only relaxable injection entitlement —
	// the other five either void the posture floor (disable-library-validation
	// and get-task-allow are denied by the status word; allow-unsigned-
	// executable-memory and disable-executable-page-protection clear CS_HARD
	// and CS_ENFORCEMENT, measured 0x22010211) or have no legitimate use in a
	// peer (allow-dyld-environment-variables), so none of them has a field.
	AllowJIT bool
}

// Validate rejects an incomplete or self-contradictory requirement.
func (r Requirement) Validate() error {
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

// ValidationDigest returns the opaque canonical identity of every code-signing
// and entitlement predicate this requirement checks, relaxations included.
func (r Requirement) ValidationDigest() (PolicyDigest, error) {
	if err := r.Validate(); err != nil {
		return PolicyDigest{}, err
	}
	h := sha256.New()
	writeDigestString(h, r.TeamID)
	writeDigestString(h, r.SigningIdentifier)
	requirements := r.entitlementRequirements()
	keys := make([]string, 0, len(requirements))
	for key := range requirements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		requirement := requirements[key]
		writeDigestString(h, key)
		_, _ = h.Write([]byte{byte(requirement.Match)})
		writeDigestBool(h, requirement.Boolean)
		writeDigestString(h, requirement.String)
	}
	writeDigestString(h, "relaxations")
	writeDigestBool(h, r.AllowJIT)
	var digest PolicyDigest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// DRString renders the canonical designated requirement for static, on-disk
// bundle verification by deploy — the Developer ID anchor pinned to the quoted
// Team ID and signing identifier, never a cdhash (which pins one build). It is
// never used to admit a peer: a signed image satisfies its own DR from any
// path an attacker can write to.
func (r Requirement) DRString() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`identifier "%s" and anchor apple generic and certificate leaf[subject.OU] = "%s" `+
			`and certificate 1[field.1.2.840.113635.100.6.2.6] exists `+
			`and certificate leaf[field.1.2.840.113635.100.6.1.13] exists`,
		r.SigningIdentifier, r.TeamID,
	), nil
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

func writeDigestString(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write([]byte(value))
}

func writeDigestBool(h hash.Hash, value bool) {
	if value {
		_, _ = h.Write([]byte{1})
		return
	}
	_, _ = h.Write([]byte{0})
}
