package wire

import "github.com/yasyf/daemonkit/internal/trust"

// requirementDenial is how this platform denies any peer a configured
// requirement: nothing can judge it, and the denial is never a downgrade to
// the same-user floor.
var requirementDenial = trust.ErrNoVerifier
