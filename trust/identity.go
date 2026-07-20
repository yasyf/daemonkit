package trust

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/wire"
)

// CodeIdentity is one exact Developer ID team and signing identifier.
type CodeIdentity struct {
	TeamID            string
	SigningIdentifier string
}

// Requirement returns the code-only trust requirement.
func (i CodeIdentity) Requirement() Requirement {
	return Requirement{TeamID: i.TeamID, SigningIdentifier: i.SigningIdentifier}
}

// ValidationDigest returns the canonical opaque identity of this code policy.
func (i CodeIdentity) ValidationDigest() ([32]byte, error) {
	return i.Requirement().ValidationDigest()
}

// DRString returns the exact designated requirement for this code identity.
func (i CodeIdentity) DRString() (string, error) {
	return i.Requirement().DRString()
}

// CodeIdentity returns the code-only portion of the signed requirement.
func (r Requirement) CodeIdentity() CodeIdentity {
	return CodeIdentity{TeamID: r.TeamID, SigningIdentifier: r.SigningIdentifier}
}

// CodePolicy verifies code identity and hardened-runtime posture without
// receiving or naming the signed side's concrete entitlement policy.
type CodePolicy struct {
	Identity CodeIdentity
}

// Check verifies one peer against the code-only policy.
func (p CodePolicy) Check(peer wire.Peer) error {
	requirement := p.Identity.Requirement()
	return (Policy{Requirement: &requirement}).Check(peer)
}

// AcceptedIdentity is proof that one exact peer passed a concrete signed-side
// policy. Its entitlement policy remains represented only by an opaque digest.
type AcceptedIdentity struct {
	peer                    wire.Peer
	code                    CodeIdentity
	entitlementPolicyDigest [32]byte
}

// Peer returns the exact accepted kernel process identity.
func (i AcceptedIdentity) Peer() wire.Peer {
	peer := i.peer
	peer.Audit = append([]byte(nil), peer.Audit...)
	return peer
}

// CodeIdentity returns the accepted code identity.
func (i AcceptedIdentity) CodeIdentity() CodeIdentity { return i.code }

// EntitlementPolicyDigest returns the canonical opaque policy digest.
func (i AcceptedIdentity) EntitlementPolicyDigest() [32]byte {
	return i.entitlementPolicyDigest
}

// Accept verifies the exact peer in a disposable verifier and binds the
// canonical signed-side policy digest to the accepted identity.
func (v ProcessVerifier) Accept(ctx context.Context, peer wire.Peer) (AcceptedIdentity, error) {
	if v.Policy.Requirement == nil {
		return AcceptedIdentity{}, errors.New("trust: accepted identity requires a concrete signed policy")
	}
	if err := v.Check(ctx, peer); err != nil {
		return AcceptedIdentity{}, err
	}
	digest, err := v.Policy.Requirement.ValidationDigest()
	if err != nil {
		return AcceptedIdentity{}, fmt.Errorf("trust: digest accepted entitlement policy: %w", err)
	}
	acceptedPeer := peer
	acceptedPeer.Audit = append([]byte(nil), peer.Audit...)
	return AcceptedIdentity{
		peer:                    acceptedPeer,
		code:                    v.Policy.Requirement.CodeIdentity(),
		entitlementPolicyDigest: digest,
	}, nil
}
