package trust

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/wire"
)

type acceptedIdentity struct {
	peer         wire.Peer
	code         codeidentity.CodeIdentity
	policyDigest codeidentity.PolicyDigest
}

func (i acceptedIdentity) Peer() wire.Peer {
	peer := i.peer
	peer.Audit = append([]byte(nil), peer.Audit...)
	return peer
}

func (i acceptedIdentity) CodeIdentity() codeidentity.CodeIdentity { return i.code }

func (i acceptedIdentity) PolicyDigest() codeidentity.PolicyDigest { return i.policyDigest }

// Accept verifies the exact peer in a disposable verifier and binds the
// canonical signed-side policy digest to the accepted identity.
func (v ProcessVerifier) Accept(
	ctx context.Context,
	peer wire.Peer,
) (codeidentity.AcceptedIdentity, error) {
	if v.Policy.Requirement == nil {
		return nil, errors.New("trust: accepted identity requires a concrete signed policy")
	}
	if err := v.Check(ctx, peer); err != nil {
		return nil, err
	}
	digest, err := v.Policy.Requirement.ValidationDigest()
	if err != nil {
		return nil, fmt.Errorf("trust: digest accepted policy: %w", err)
	}
	acceptedPeer := peer
	acceptedPeer.Audit = append([]byte(nil), peer.Audit...)
	return acceptedIdentity{
		peer: acceptedPeer, code: v.Policy.Requirement.CodeIdentity(), policyDigest: digest,
	}, nil
}

var _ codeidentity.AcceptedIdentity = acceptedIdentity{}
