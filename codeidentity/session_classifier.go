package codeidentity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/daemonkit/version"
	"github.com/yasyf/daemonkit/wire"
)

// SessionClassifier is the production protected-session policy for one exact
// daemon or signed-app executable.
type SessionClassifier struct {
	Executable   string
	CodeIdentity CodeIdentity
	Acceptor     IdentityAcceptor
	PolicyDigest PolicyDigest
}

// Validate rejects incomplete or ambiguous classifier configuration.
func (c SessionClassifier) Validate() error {
	if !filepath.IsAbs(c.Executable) || filepath.Clean(c.Executable) != c.Executable {
		return fmt.Errorf("codeidentity: protected executable %q is not an exact absolute path", c.Executable)
	}
	hasDigest := c.PolicyDigest != (PolicyDigest{})
	if (c.Acceptor == nil) != !hasDigest {
		return errors.New("codeidentity: signed acceptor and policy digest must be configured together")
	}
	if c.Acceptor != nil {
		if err := c.CodeIdentity.Validate(); err != nil {
			return fmt.Errorf("codeidentity: protected code identity: %w", err)
		}
	} else if c.CodeIdentity != (CodeIdentity{}) {
		return errors.New("codeidentity: protected code identity requires a signed acceptor")
	}
	return nil
}

// Classify authenticates a candidate without turning ordinary executable
// mismatches into connection-level trust failures.
func (c SessionClassifier) Classify(ctx context.Context, peer wire.Peer) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := c.Validate(); err != nil {
		return false, err
	}
	if peer.UID != os.Geteuid() || peer.Executable != c.Executable {
		return false, nil
	}
	if peer.PID <= 1 || peer.StartTime == "" || peer.Boot == "" {
		return false, fmt.Errorf("%w: protected peer process identity is incomplete", ErrUntrustedPeer)
	}
	if c.Acceptor == nil {
		return true, nil
	}
	accepted, err := c.Acceptor.Accept(ctx, peer)
	if err != nil {
		return false, fmt.Errorf("codeidentity: accept protected peer: %w", err)
	}
	if accepted.PolicyDigest() != c.PolicyDigest {
		return false, fmt.Errorf("%w: protected peer policy digest mismatch", ErrUntrustedPeer)
	}
	if accepted.CodeIdentity() != c.CodeIdentity {
		return false, fmt.Errorf("%w: protected peer code identity mismatch", ErrUntrustedPeer)
	}
	acceptedPeer := accepted.Peer()
	if acceptedPeer.PID != peer.PID || acceptedPeer.UID != peer.UID ||
		acceptedPeer.StartTime != peer.StartTime || acceptedPeer.Boot != peer.Boot ||
		acceptedPeer.Executable != peer.Executable || !bytes.Equal(acceptedPeer.Audit, peer.Audit) {
		return false, fmt.Errorf("%w: accepted protected peer process identity changed", ErrUntrustedPeer)
	}
	return true, nil
}

// AuthorizeBuild admits only the same daemon build or a strictly newer
// successor to lifecycle routes.
func (SessionClassifier) AuthorizeBuild(serverBuild, peerBuild string) bool {
	return peerBuild == serverBuild || version.Newer(peerBuild, serverBuild)
}

var _ wire.ProtectedSessionClassifier = SessionClassifier{}
