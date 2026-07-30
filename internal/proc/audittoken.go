package proc

import (
	"encoding/binary"
	"fmt"
)

const auditTokenLength = 32

// AuditToken is Darwin's kernel-stable process execution identity: a transport
// value read off a socket peer, never record identity. Its (pid, pidversion)
// pair survives PID reuse and is valid only for the execution that minted it.
type AuditToken [auditTokenLength]byte

// AuditTokenFromBytes validates and copies one Darwin audit_token_t.
func AuditTokenFromBytes(raw []byte) (AuditToken, error) {
	if len(raw) != auditTokenLength {
		return AuditToken{}, fmt.Errorf("proc: audit token is %d bytes, want %d", len(raw), auditTokenLength)
	}
	var token AuditToken
	copy(token[:], raw)
	return token, nil
}

// PID returns the process ID embedded in the audit token.
func (t AuditToken) PID() int {
	return int(binary.NativeEndian.Uint32(t[20:24]))
}

// PIDVersion returns the kernel execution version embedded in the audit token.
func (t AuditToken) PIDVersion() uint32 {
	return binary.NativeEndian.Uint32(t[28:32])
}

// Valid reports whether the token carries a usable process execution identity.
func (t AuditToken) Valid() bool {
	return t.PID() > 0 && t.PIDVersion() != 0
}
