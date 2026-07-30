package daemonkit

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

// Requirement pins both halves of a designated requirement, Developer ID
// anchored. Parent-safe: it carries no signed-only literal.
type Requirement struct{ TeamID, SigningIdentifier string }

// Digest is the opaque policy digest a daemon-facing binary may carry.
func (r Requirement) Digest() PolicyDigest {
	var buf []byte
	for _, field := range []string{r.TeamID, r.SigningIdentifier} {
		buf = binary.BigEndian.AppendUint64(buf, uint64(len(field)))
		buf = append(buf, field...)
	}
	sum := sha256.Sum256(buf)
	return PolicyDigest(hex.EncodeToString(sum[:]))
}

// PolicyDigest is the opaque canonical digest of a signed-side policy.
type PolicyDigest string
