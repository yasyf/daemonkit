package trust

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/proc"
)

// CodeIdentity reports whether this platform's kernel holds a code identity a
// Requirement can be judged against.
const CodeIdentity = false

// Fails closed: linux holds no kernel code identity to judge a requirement
// against, so a configured Requirement is denied, never downgraded to
// UID-only.
func verifyRequirement(_ proc.AuditToken, _ Requirement) error {
	return fmt.Errorf("%w (linux has no code-identity verifier; the only posture is the same-user floor)", ErrNoVerifier)
}

// VerifyProcess denies every requirement for the reason verifyRequirement
// does. An invalid req is still a configuration error and not that denial.
func VerifyProcess(_ int, req Requirement) error {
	if err := req.Validate(); err != nil {
		return err
	}
	return verifyRequirement(proc.AuditToken{}, req)
}

// ProcessToken is pid's transport token. Linux mints no execution identity, so
// it carries the pid alone and is never Valid.
func ProcessToken(pid int) (proc.AuditToken, error) {
	return proc.PeerToken(pid), nil
}
