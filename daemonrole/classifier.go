// Package daemonrole authenticates unsigned request daemons through an exact
// same-user executable role. It is intentionally separate from signed-code
// identity: a same-UID process can rebind a user-owned role path, so the role
// path is the explicit trust boundary rather than a cryptographic identity.
package daemonrole

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/wire"
)

// ErrUntrustedPeer is returned when a peer does not own the configured role.
var ErrUntrustedPeer = errors.New("daemonrole: untrusted peer")

// Classifier authenticates one request-daemon service role. RoleID is the
// exact launchd/service label shared by launcher and server. RolePath is the
// stable executable alias; its current exact target is resolved for every
// candidate so an atomic package upgrade admits only the new target.
type Classifier struct {
	RoleID   string
	RolePath string
}

// Validate rejects an ambiguous service role without touching the role path.
func (c Classifier) Validate() error {
	if !validRoleID(c.RoleID) {
		return fmt.Errorf("daemonrole: role id %q is not canonical", c.RoleID)
	}
	if !filepath.IsAbs(c.RolePath) || filepath.Clean(c.RolePath) != c.RolePath {
		return fmt.Errorf("daemonrole: role path %q is not exact and absolute", c.RolePath)
	}
	return nil
}

// Classify admits only the complete same-UID process identity whose exact
// executable is the role path's current regular executable target.
func (c Classifier) Classify(ctx context.Context, peer wire.Peer) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := c.Validate(); err != nil {
		return false, err
	}
	if peer.UID != os.Geteuid() {
		return false, nil
	}
	if peer.PID <= 1 || peer.StartTime == "" || peer.Boot == "" || peer.Executable == "" {
		return false, fmt.Errorf("%w: peer process identity is incomplete", ErrUntrustedPeer)
	}
	target, err := filepath.EvalSymlinks(c.RolePath)
	if err != nil {
		return false, fmt.Errorf("daemonrole: resolve role %s: %w", c.RoleID, err)
	}
	if !filepath.IsAbs(target) || filepath.Clean(target) != target {
		return false, fmt.Errorf("daemonrole: resolved role target %q is not exact and absolute", target)
	}
	info, err := os.Stat(target)
	if err != nil {
		return false, fmt.Errorf("daemonrole: inspect role target %q: %w", target, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return false, fmt.Errorf("daemonrole: role target %q is not an executable regular file", target)
	}
	return peer.Executable == target, nil
}

func validRoleID(role string) bool {
	if len(role) == 0 || len(role) > 255 || strings.HasPrefix(role, ".") || strings.HasSuffix(role, ".") {
		return false
	}
	parts := strings.Split(role, ".")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
		for _, char := range []byte(part) {
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '-' {
				continue
			}
			return false
		}
	}
	return true
}

var _ wire.ProtectedSessionClassifier = Classifier{}
