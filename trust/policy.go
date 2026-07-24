// Package trust verifies the code-signing identity of a connected unix-socket
// peer: a same-UID floor on every platform plus, on signed darwin builds, a
// designated requirement checked against the peer's audit token. A configured
// Requirement with no verifier fails closed, never downgrading to UID-only; a
// nil Requirement is explicit UID-only trust (the floor alone). The
// daemonkit_unsigned build tag drops the verifier for local-test builds,
// which release CI must reject.
package trust

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"os"
	"sort"
	"strings"

	"github.com/yasyf/daemonkit/codeidentity"
	peer "github.com/yasyf/daemonkit/peer"
	"github.com/yasyf/daemonkit/proc"
)

// ErrUntrustedPeer is returned when a peer fails the trust check.
var ErrUntrustedPeer = errors.New("trust: untrusted peer")

// ErrNoVerifier is the fail-closed denial when a Requirement is configured but
// no code-identity verifier is available — never a downgrade to UID-only.
var ErrNoVerifier = errors.New("trust: no code-identity verifier for a configured requirement")

const appGroupsEntitlement = "com.apple.security.application-groups"

// PeerRole names one exact signed peer authority.
type PeerRole string

// UnprotectedRole is the daemonkit-sealed role for same-UID sessions with no
// signed or private-control authority.
const UnprotectedRole PeerRole = "daemonkit.unprotected.v1"

// PolicyDigest is the canonical identity of one complete compiled trust policy.
type PolicyDigest [sha256.Size]byte

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

// ValidationDigest returns the opaque canonical identity of every code-signing
// and entitlement predicate checked by this requirement.
func (r Requirement) ValidationDigest() (codeidentity.PolicyDigest, error) {
	if err := r.validate(); err != nil {
		return codeidentity.PolicyDigest{}, err
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
		if requirement.Boolean {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
		writeDigestString(h, requirement.String)
	}
	var digest codeidentity.PolicyDigest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// CodeIdentity returns the code-only portion of the signed requirement.
func (r Requirement) CodeIdentity() codeidentity.CodeIdentity {
	return codeidentity.CodeIdentity{TeamID: r.TeamID, SigningIdentifier: r.SigningIdentifier}
}

func writeDigestString(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write([]byte(value))
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
	return r.CodeIdentity().DRString()
}

// Policy verifies a peer against an optional Requirement.
type Policy struct {
	// Requirement, when nil, means UID-only: the same-UID floor is the whole check.
	Requirement *Requirement
}

// TrustPolicyConfig declares the complete role and private-control authority
// compiled into one immutable TrustPolicy.
//
//nolint:revive // The exact public name distinguishes the compiled policy from per-peer Policy.
type TrustPolicyConfig struct {
	ExpectedUID      int
	Roles            map[PeerRole]Requirement
	AllowUnprotected bool
	StopRoles        []PeerRole
	ReceiptRoles     []PeerRole
	ReadinessRoles   []PeerRole
	HandoffRoles     []PeerRole
}

// TrustPolicy is an immutable compiled runtime trust policy.
//
//nolint:revive // The exact public name distinguishes runtime authority from per-peer Policy.
type TrustPolicy struct {
	expectedUID      int
	roles            map[PeerRole]Requirement
	roleNames        []PeerRole
	allowUnprotected bool
	stopRoles        map[PeerRole]struct{}
	receiptRoles     map[PeerRole]struct{}
	readinessRoles   map[PeerRole]struct{}
	handoffRoles     map[PeerRole]struct{}
}

// NewTrustPolicy validates and deep-copies a complete runtime trust policy.
func NewTrustPolicy(config TrustPolicyConfig) (TrustPolicy, error) {
	if config.ExpectedUID != os.Geteuid() {
		return TrustPolicy{}, fmt.Errorf("trust: expected UID %d must equal effective UID %d", config.ExpectedUID, os.Geteuid())
	}
	policy := TrustPolicy{
		expectedUID:      config.ExpectedUID,
		roles:            make(map[PeerRole]Requirement, len(config.Roles)),
		allowUnprotected: config.AllowUnprotected,
		stopRoles:        make(map[PeerRole]struct{}, len(config.StopRoles)),
		receiptRoles:     make(map[PeerRole]struct{}, len(config.ReceiptRoles)),
		readinessRoles:   make(map[PeerRole]struct{}, len(config.ReadinessRoles)),
		handoffRoles:     make(map[PeerRole]struct{}, len(config.HandoffRoles)),
	}
	if len(config.Roles) == 0 {
		if !config.AllowUnprotected {
			return TrustPolicy{}, errors.New("trust: at least one protected role or explicit unprotected mode is required")
		}
		if len(config.StopRoles) != 0 || len(config.ReceiptRoles) != 0 ||
			len(config.ReadinessRoles) != 0 || len(config.HandoffRoles) != 0 {
			return TrustPolicy{}, errors.New("trust: unprotected-only policy cannot declare protected authority")
		}
		return policy, nil
	}
	for role, requirement := range config.Roles {
		if strings.TrimSpace(string(role)) == "" {
			return TrustPolicy{}, errors.New("trust: protected role is empty")
		}
		if role == UnprotectedRole {
			return TrustPolicy{}, fmt.Errorf("trust: role %q is reserved by daemonkit", role)
		}
		if err := requirement.validate(); err != nil {
			return TrustPolicy{}, fmt.Errorf("trust: role %q: %w", role, err)
		}
		if _, err := requirement.ValidationDigest(); err != nil {
			return TrustPolicy{}, fmt.Errorf("trust: role %q digest: %w", role, err)
		}
		policy.roles[role] = cloneRequirement(requirement)
		policy.roleNames = append(policy.roleNames, role)
	}
	sort.Slice(policy.roleNames, func(i, j int) bool { return policy.roleNames[i] < policy.roleNames[j] })
	if err := compileRoleSet("stop", config.StopRoles, policy.roles, policy.stopRoles); err != nil {
		return TrustPolicy{}, err
	}
	if len(policy.stopRoles) != 1 {
		return TrustPolicy{}, errors.New("trust: stop authority requires exactly one role")
	}
	if err := compileRoleSet("receipt", config.ReceiptRoles, policy.roles, policy.receiptRoles); err != nil {
		return TrustPolicy{}, err
	}
	if err := compileRoleSet("readiness", config.ReadinessRoles, policy.roles, policy.readinessRoles); err != nil {
		return TrustPolicy{}, err
	}
	if err := compileRoleSetOptional("handoff", config.HandoffRoles, policy.roles, policy.handoffRoles); err != nil {
		return TrustPolicy{}, err
	}
	lifecycleRoles := make(map[PeerRole]struct{}, len(policy.receiptRoles)+len(policy.readinessRoles))
	for role := range policy.receiptRoles {
		lifecycleRoles[role] = struct{}{}
	}
	for role := range policy.readinessRoles {
		lifecycleRoles[role] = struct{}{}
	}
	if len(lifecycleRoles) > 2 {
		return TrustPolicy{}, errors.New("trust: lifecycle roles exceed the fixed capacity of two")
	}
	for role := range policy.stopRoles {
		if _, exists := lifecycleRoles[role]; exists {
			return TrustPolicy{}, fmt.Errorf("trust: role %q overlaps stop and lifecycle authority", role)
		}
		if _, exists := policy.handoffRoles[role]; exists {
			return TrustPolicy{}, fmt.Errorf("trust: role %q overlaps stop and handoff authority", role)
		}
	}
	for role := range lifecycleRoles {
		if _, exists := policy.handoffRoles[role]; exists {
			return TrustPolicy{}, fmt.Errorf("trust: role %q overlaps lifecycle and handoff authority", role)
		}
	}
	return policy, nil
}

func compileRoleSet(name string, source []PeerRole, roles map[PeerRole]Requirement, target map[PeerRole]struct{}) error {
	if len(source) == 0 {
		return fmt.Errorf("trust: %s roles are required", name)
	}
	for _, role := range source {
		if _, ok := roles[role]; !ok {
			return fmt.Errorf("trust: %s role %q is not declared", name, role)
		}
		if _, duplicate := target[role]; duplicate {
			return fmt.Errorf("trust: duplicate %s role %q", name, role)
		}
		target[role] = struct{}{}
	}
	return nil
}

func compileRoleSetOptional(name string, source []PeerRole, roles map[PeerRole]Requirement, target map[PeerRole]struct{}) error {
	if len(source) == 0 {
		return nil
	}
	return compileRoleSet(name, source, roles, target)
}

func cloneRequirement(source Requirement) Requirement {
	clone := source
	clone.RequiredEntitlements = make(map[string]EntitlementRequirement, len(source.RequiredEntitlements))
	for key, value := range source.RequiredEntitlements {
		clone.RequiredEntitlements[key] = value
	}
	return clone
}

// Validate rejects the zero value; constructed policies are already valid.
func (p TrustPolicy) Validate() error {
	if p.roles == nil || p.stopRoles == nil || p.receiptRoles == nil || p.readinessRoles == nil || p.handoffRoles == nil {
		return errors.New("trust: policy was not constructed by NewTrustPolicy")
	}
	if len(p.roles) == 0 {
		if !p.allowUnprotected || len(p.stopRoles) != 0 || len(p.receiptRoles) != 0 ||
			len(p.readinessRoles) != 0 || len(p.handoffRoles) != 0 {
			return errors.New("trust: invalid unprotected-only policy")
		}
		return nil
	}
	if len(p.stopRoles) == 0 || len(p.receiptRoles) == 0 || len(p.readinessRoles) == 0 {
		return errors.New("trust: protected policy is missing required authority")
	}
	return nil
}

// ExpectedUID returns the exact kernel UID floor.
func (p TrustPolicy) ExpectedUID() int { return p.expectedUID }

// RoleNames returns the canonical protected-role order.
func (p TrustPolicy) RoleNames() []PeerRole { return append([]PeerRole(nil), p.roleNames...) }

// AllowsUnprotected reports whether the sealed same-UID role may open a
// session. It never grants signed or private-control authority.
func (p TrustPolicy) AllowsUnprotected() bool { return p.allowUnprotected }

// Requirement returns a deep copy of one compiled role requirement.
func (p TrustPolicy) Requirement(role PeerRole) (Requirement, bool) {
	requirement, ok := p.roles[role]
	return cloneRequirement(requirement), ok
}

// AllowsStop reports exact stop-control authority for role.
func (p TrustPolicy) AllowsStop(role PeerRole) bool { _, ok := p.stopRoles[role]; return ok }

// AllowsReceipt reports exact runtime-receipt authority for role.
func (p TrustPolicy) AllowsReceipt(role PeerRole) bool { _, ok := p.receiptRoles[role]; return ok }

// AllowsReadiness reports exact readiness-subscription authority for role.
func (p TrustPolicy) AllowsReadiness(role PeerRole) bool { _, ok := p.readinessRoles[role]; return ok }

// AllowsHandoff reports exact connected-FD broker authority for role.
func (p TrustPolicy) AllowsHandoff(role PeerRole) bool { _, ok := p.handoffRoles[role]; return ok }

// SignatureDigest returns the exact compiled signed-role policy identity.
func (p TrustPolicy) SignatureDigest(role PeerRole) (proc.SignatureDigest, bool) {
	requirement, ok := p.roles[role]
	if !ok {
		return proc.SignatureDigest{}, false
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		return proc.SignatureDigest{}, false
	}
	signature, err := proc.NewSignatureDigest([32]byte(digest))
	if err != nil {
		return proc.SignatureDigest{}, false
	}
	return signature, true
}

// ValidationDigest returns the deterministic domain-separated identity of
// every compiled role, requirement, UID, and private-control authority set.
func (p TrustPolicy) ValidationDigest() (PolicyDigest, error) {
	if err := p.Validate(); err != nil {
		return PolicyDigest{}, err
	}
	h := sha256.New()
	writeDigestString(h, "daemonkit.trust-policy.v1")
	var uid [8]byte
	binary.BigEndian.PutUint64(uid[:], uint64(p.expectedUID)) //nolint:gosec // Validation pins a nonnegative effective UID.
	_, _ = h.Write(uid[:])
	if p.allowUnprotected {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	for _, role := range p.roleNames {
		writeDigestString(h, string(role))
		requirement, ok := p.roles[role]
		if !ok {
			return PolicyDigest{}, errors.New("trust: compiled role requirement is absent")
		}
		digest, err := requirement.ValidationDigest()
		if err != nil {
			return PolicyDigest{}, err
		}
		writeDigestBytes(h, digest[:])
	}
	writeRoleSetDigest(h, "stop", p.stopRoles)
	writeRoleSetDigest(h, "receipt", p.receiptRoles)
	writeRoleSetDigest(h, "readiness", p.readinessRoles)
	writeRoleSetDigest(h, "handoff", p.handoffRoles)
	var digest PolicyDigest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func writeDigestBytes(h hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}

func writeRoleSetDigest(h hash.Hash, name string, roles map[PeerRole]struct{}) {
	writeDigestString(h, name)
	ordered := make([]PeerRole, 0, len(roles))
	for role := range roles {
		ordered = append(ordered, role)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, role := range ordered {
		writeDigestString(h, string(role))
	}
}

// Check enforces the same-effective-UID floor, then any configured
// Requirement against the peer's audit token; a Requirement with no verifier
// fails closed (ErrNoVerifier). LOCAL_PEERTOKEN is a query-time process
// reference, not an immutable record of the connector. Descriptor delegation
// or substitution by another process satisfying the same signed policy before
// this check remains a platform limitation.
func (p Policy) Check(peer peer.Identity) error {
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
