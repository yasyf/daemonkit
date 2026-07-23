// Package codeidentity defines daemon-safe signed-code identity and opaque
// policy proofs. It deliberately contains no concrete entitlement policy.
package codeidentity

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"

	peer "github.com/yasyf/daemonkit/peer"
)

// ErrUntrustedPeer is returned when a peer fails a code-identity check.
var ErrUntrustedPeer = errors.New("codeidentity: untrusted peer")

// ErrNoVerifier is returned when this build cannot verify signed code.
var ErrNoVerifier = errors.New("codeidentity: no code-identity verifier")

// CodeIdentity is one exact Developer ID team and signing identifier.
type CodeIdentity struct {
	TeamID            string
	SigningIdentifier string
}

// Validate rejects incomplete or ambiguous code identity.
func (i CodeIdentity) Validate() error {
	if strings.TrimSpace(i.TeamID) == "" {
		return errors.New("codeidentity: TeamID is required")
	}
	if strings.TrimSpace(i.SigningIdentifier) == "" {
		return errors.New("codeidentity: SigningIdentifier is required")
	}
	if strings.ContainsAny(i.TeamID, `"\`) || strings.ContainsAny(i.SigningIdentifier, `"\`) {
		return errors.New("codeidentity: fields must not contain quotes or backslashes")
	}
	return nil
}

// DRString returns the exact designated requirement for this code identity.
func (i CodeIdentity) DRString() (string, error) {
	if err := i.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		`identifier "%s" and anchor apple generic and certificate leaf[subject.OU] = "%s" `+
			`and certificate 1[field.1.2.840.113635.100.6.2.6] exists `+
			`and certificate leaf[field.1.2.840.113635.100.6.1.13] exists`,
		i.SigningIdentifier, i.TeamID,
	), nil
}

// PolicyDigest is the opaque canonical digest of a signed-side policy.
type PolicyDigest [32]byte

// Validate rejects a missing policy digest.
func (d PolicyDigest) Validate() error {
	if d == (PolicyDigest{}) {
		return errors.New("codeidentity: policy digest is required")
	}
	return nil
}

// ValidationDigest returns the canonical opaque identity of this code policy.
func (i CodeIdentity) ValidationDigest() (PolicyDigest, error) {
	if err := i.Validate(); err != nil {
		return PolicyDigest{}, err
	}
	h := sha256.New()
	writeDigestString(h, i.TeamID)
	writeDigestString(h, i.SigningIdentifier)
	var digest PolicyDigest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func writeDigestString(h interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write([]byte(value))
}

// CodePolicy verifies same-UID and exact signed-code identity without naming
// or receiving the signed side's concrete policy.
type CodePolicy struct {
	Identity CodeIdentity
}

// Check verifies one peer against the code-only policy.
func (p CodePolicy) Check(peer peer.Identity) error {
	if peer.UID != os.Geteuid() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, peer.UID, os.Geteuid())
	}
	if err := p.Identity.Validate(); err != nil {
		return err
	}
	return verifyCodeIdentity(peer, p.Identity)
}
