package proc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const auditTokenLength = 32

// UnmarshalJSON decodes one exact JSON byte array.
func (t *AuditToken) UnmarshalJSON(data []byte) error {
	var elements []json.RawMessage
	if err := json.Unmarshal(data, &elements); err != nil {
		return fmt.Errorf("%w: decode audit token: %w", ErrNoAuditToken, err)
	}
	if len(elements) != auditTokenLength {
		return fmt.Errorf("%w: audit token has %d elements, want %d", ErrNoAuditToken, len(elements), auditTokenLength)
	}
	var token AuditToken
	for index, element := range elements {
		if bytes.Equal(bytes.TrimSpace(element), []byte("null")) {
			return fmt.Errorf("%w: audit token element %d is null", ErrNoAuditToken, index)
		}
		if err := json.Unmarshal(element, &token[index]); err != nil {
			return fmt.Errorf("%w: audit token element %d: %w", ErrNoAuditToken, index, err)
		}
	}
	*t = token
	return nil
}

// AuditTokenFromBytes validates and copies one Darwin audit_token_t.
func AuditTokenFromBytes(raw []byte) (AuditToken, error) {
	if len(raw) != auditTokenLength {
		return AuditToken{}, fmt.Errorf("%w: audit token is %d bytes, want %d", ErrNoAuditToken, len(raw), auditTokenLength)
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

func validateAuditToken(t AuditToken, pid int) error {
	if !t.Valid() {
		return ErrNoAuditToken
	}
	if t.PID() != pid {
		return fmt.Errorf("%w: audit-token pid %d != process pid %d", ErrIdentityChanged, t.PID(), pid)
	}
	return nil
}

// BindAuditTokenIdentity proves that token's kernel execution is live across
// the PID/start-time probe and returns one indivisible kill authority.
func BindAuditTokenIdentity(token AuditToken, pid int) (Identity, error) {
	if err := validateAuditToken(token, pid); err != nil {
		return Identity{}, err
	}
	before, err := ExecutablePathAuditToken(token)
	if err != nil {
		return Identity{}, fmt.Errorf("bind audit token before process probe: %w", err)
	}
	identity, err := Probe(pid)
	if err != nil {
		return Identity{}, fmt.Errorf("probe audit-token pid %d: %w", pid, err)
	}
	after, err := ExecutablePathAuditToken(token)
	if err != nil {
		return Identity{}, fmt.Errorf("bind audit token after process probe: %w", err)
	}
	if before != after {
		return Identity{}, fmt.Errorf("%w: audit-token executable changed from %q to %q", ErrIdentityChanged, before, after)
	}
	identity.Executable = after
	identity.AuditToken = token
	return identity, nil
}
