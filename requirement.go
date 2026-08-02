package daemonkit

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// Requirement pins both halves of a designated requirement, Developer ID
// anchored, plus the entitlement clauses that narrow it. Parent-safe: it
// carries no signed-only literal.
//
// TeamID and SigningIdentifier alone admit every build the team ever signed
// under that identifier, old versions included; RequiredAppGroup and
// RequiredEntitlements are how a consumer narrows stop authority past that
// namespace.
type Requirement struct {
	TeamID               string
	SigningIdentifier    string
	RequiredAppGroup     string
	RequiredEntitlements map[string]EntitlementRequirement

	// AllowJIT admits a peer signed with com.apple.security.cs.allow-jit. It
	// is not a bounded relaxation: the code the kernel hashed no longer
	// determines what such a peer executes. A consumer setting it accepts that
	// residual by name. Every other injection entitlement stays denied.
	AllowJIT bool
}

// EntitlementMatch is one closed required-entitlement predicate.
type EntitlementMatch uint8

const (
	// EntitlementBoolean requires an exact boolean value.
	EntitlementBoolean EntitlementMatch = iota + 1
	// EntitlementString requires an exact string value.
	EntitlementString
	// EntitlementStringArrayContains requires membership in a string array.
	EntitlementStringArrayContains

	entitlementMatchLimit
)

// EntitlementRequirement is one typed entitlement predicate.
type EntitlementRequirement struct {
	Match   EntitlementMatch
	Boolean bool
	String  string
}

// Digest is the opaque policy digest a daemon-facing binary may carry. It
// covers every clause the requirement enforces, so two requirements admitting
// different peers never share one.
func (r Requirement) Digest() PolicyDigest {
	var buf []byte
	for _, field := range []string{r.TeamID, r.SigningIdentifier, r.RequiredAppGroup} {
		buf = appendDigestString(buf, field)
	}
	keys := make([]string, 0, len(r.RequiredEntitlements))
	for key := range r.RequiredEntitlements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(keys)))
	for _, key := range keys {
		entitlement := r.RequiredEntitlements[key]
		buf = appendDigestString(buf, key)
		buf = append(buf, byte(entitlement.Match))
		buf = appendDigestBool(buf, entitlement.Boolean)
		buf = appendDigestString(buf, entitlement.String)
	}
	buf = appendDigestBool(buf, r.AllowJIT)
	sum := sha256.Sum256(buf)
	return PolicyDigest(hex.EncodeToString(sum[:]))
}

// Requirements is a disjunction of full requirements: a peer is admitted by
// any one element. An app and its File Provider extension are two genuinely
// different signed bundles, each carrying its own entitlements and app group,
// and each element states its own — so the disjunction stays strictly stronger
// than the single TeamID-only requirement Validate refuses.
//
// A nil Requirements is the unset field. A non-nil set with no elements is a
// disjunction over nothing, which admits nobody; ValidateForServe refuses it
// rather than letting it read as the unset field.
type Requirements []Requirement

// Digest is the set's policy digest. It covers each element's own digest in
// sorted order, so the same set written in two orders cannot produce two
// digests, and the element count, so a set never shares a digest with one of
// its members.
func (rs Requirements) Digest() PolicyDigest {
	elements := make([]string, len(rs))
	for i, requirement := range rs {
		elements[i] = string(requirement.Digest())
	}
	sort.Strings(elements)
	buf := binary.BigEndian.AppendUint64(nil, uint64(len(elements)))
	for _, element := range elements {
		buf = appendDigestString(buf, element)
	}
	sum := sha256.Sum256(buf)
	return PolicyDigest(hex.EncodeToString(sum[:]))
}

func appendDigestString(buf []byte, value string) []byte {
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(value)))
	return append(buf, value...)
}

func appendDigestBool(buf []byte, value bool) []byte {
	if value {
		return append(buf, 1)
	}
	return append(buf, 0)
}

// PolicyDigest is the opaque canonical digest of a signed-side policy.
type PolicyDigest string
