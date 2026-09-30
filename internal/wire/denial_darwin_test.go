package wire

import "github.com/yasyf/daemonkit/internal/trust"

// requirementDenial is how this platform denies an unsigned peer a configured
// requirement: the verifier judged it and it did not match.
var requirementDenial = trust.ErrUntrustedPeer
