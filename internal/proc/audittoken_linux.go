package proc

import "encoding/binary"

// PeerToken carries the one fact linux socket credentials hold about a peer's
// process — its pid — in the transport slot darwin fills with an audit token.
// It names no execution: PIDVersion stays zero, so the token is never Valid
// and no code-identity verifier will judge it.
func PeerToken(pid int) AuditToken {
	var token AuditToken
	binary.NativeEndian.PutUint32(token[20:24], uint32(pid)) //nolint:gosec // kernel pids are non-negative
	return token
}
