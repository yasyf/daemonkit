package trust

import (
	"encoding/binary"
	"fmt"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/internal/csposture"
)

// csops op numbers, measured against xnu's not-restricted-to-root set. No SDK
// header declares them: sys/codesign.h ships in no macOS SDK.
const (
	opStatus             = 0  // CS_OPS_STATUS
	opCDHash             = 5  // CS_OPS_CDHASH
	opBlob               = 10 // CS_OPS_BLOB
	opIdentity           = 11 // CS_OPS_IDENTITY
	opTeamID             = 14 // CS_OPS_TEAMID
	opDEREntitlements    = 16 // CS_OPS_DER_ENTITLEMENTS_BLOB
	opValidationCategory = 17 // CS_OPS_VALIDATION_CATEGORY
)

const (
	validationCategoryDeveloperID = 6
	maxStringBlob                 = 4096
	initialEntitlementsBlob       = 4096
	initialSignatureBlob          = 64 << 10
)

// csopsRead answers one csops op for a fixed audit token, returning the errno
// the kernel set or zero on success. It is the package's single kernel seam.
type csopsRead func(op uint32, buf []byte) syscall.Errno

// verifyToken runs every clause against one execution generation, cheapest and
// most structurally guarded first, so a dead or obviously-wrong peer is denied
// before the parsers are reached. Identity is proven in full before any
// attacker-influenced blob is decoded.
func verifyToken(read csopsRead, req Requirement) error {
	if err := checkStatus(read); err != nil {
		return err
	}
	if err := checkValidationCategory(read); err != nil {
		return err
	}
	teamID, err := readSigningString(read, opTeamID)
	if err != nil {
		return err
	}
	if teamID != req.TeamID {
		return fmt.Errorf("%w: peer team identifier does not match the requirement", ErrUntrustedPeer)
	}
	identifier, err := readSigningString(read, opIdentity)
	if err != nil {
		return err
	}
	if identifier != req.SigningIdentifier {
		return fmt.Errorf("%w: peer signing identifier does not match the requirement", ErrUntrustedPeer)
	}
	if err := checkSignature(read, req); err != nil {
		return err
	}
	ents, err := readEntitlements(read)
	if err != nil {
		return err
	}
	if err := rejectInjection(ents, req); err != nil {
		return err
	}
	return requireEntitlements(ents, req)
}

func checkStatus(read csopsRead) error {
	var buf [4]byte
	if errno := read(opStatus, buf[:]); errno != 0 {
		return denyErrno(opStatus, errno)
	}
	status := int64(binary.LittleEndian.Uint32(buf[:]))
	if err := csposture.Check(status); err != nil {
		return fmt.Errorf("%w: %w", ErrUntrustedPeer, err)
	}
	return nil
}

// The validation category has no header, no magic and no sanity bit: its
// fail-closed property is only that the single value 6 admits. An op
// renumbering that happened to return 6 would pass this clause, and the other
// reads are its only backstop.
func checkValidationCategory(read csopsRead) error {
	var buf [4]byte
	if errno := read(opValidationCategory, buf[:]); errno != 0 {
		return denyErrno(opValidationCategory, errno)
	}
	category := binary.LittleEndian.Uint32(buf[:])
	if category != validationCategoryDeveloperID {
		return fmt.Errorf("%w: validation category %d is not Developer ID (%d)",
			ErrUntrustedPeer, category, validationCategoryDeveloperID)
	}
	return nil
}

// readSigningString decodes the {reserved uint32, total length uint32} header
// and NUL-terminated string CS_OPS_IDENTITY and CS_OPS_TEAMID return, where
// the length counts the header and the NUL (measured: 0x1e for a 21-byte
// identifier).
func readSigningString(read csopsRead, op uint32) (string, error) {
	var buf [maxStringBlob]byte
	if errno := read(op, buf[:]); errno != 0 {
		return "", denyErrno(op, errno)
	}
	if reserved := binary.BigEndian.Uint32(buf[0:4]); reserved != 0 {
		return "", fmt.Errorf("%w: csops op %d reserved word is 0x%08x, want 0", ErrNoVerifier, op, reserved)
	}
	length := binary.BigEndian.Uint32(buf[4:8])
	if length < 10 || int(length) > len(buf) {
		return "", fmt.Errorf("%w: csops op %d length header %d is outside [10, %d]", ErrNoVerifier, op, length, len(buf))
	}
	if buf[length-1] != 0 {
		return "", fmt.Errorf("%w: csops op %d string is not NUL-terminated at its declared length", ErrNoVerifier, op)
	}
	value := string(buf[8 : length-1])
	if strings.ContainsRune(value, 0) {
		return "", fmt.Errorf("%w: csops op %d string carries an interior NUL", ErrNoVerifier, op)
	}
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: csops op %d string is not valid UTF-8", ErrNoVerifier, op)
	}
	return value, nil
}

func checkSignature(read csopsRead, req Requirement) error {
	var cdHash [cdHashLength]byte
	if errno := read(opCDHash, cdHash[:]); errno != 0 {
		return denyErrno(opCDHash, errno)
	}
	blob, err := readSignatureBlob(read)
	if err != nil {
		return err
	}
	return verifySignatureBlob(blob, cdHash[:], req)
}

func readSignatureBlob(read csopsRead) ([]byte, error) {
	buf := make([]byte, initialSignatureBlob)
	errno := read(opBlob, buf)
	if errno == syscall.ERANGE {
		required := binary.BigEndian.Uint32(buf[4:8])
		if required < 12 || required > maxSignatureBlob {
			return nil, fmt.Errorf("%w: csops op %d wants %d bytes, outside [12, %d]",
				ErrNoVerifier, opBlob, required, maxSignatureBlob)
		}
		buf = make([]byte, required)
		errno = read(opBlob, buf)
	}
	if errno != 0 {
		return nil, denyErrno(opBlob, errno)
	}
	return buf, nil
}

func readEntitlements(read csopsRead) (entitlements, error) {
	var stack [initialEntitlementsBlob]byte
	buf := stack[:]
	errno := read(opDEREntitlements, buf)
	if errno == syscall.ERANGE {
		required := binary.BigEndian.Uint32(buf[4:8])
		if required < 8 || required > maxEntitlementsBlob {
			return nil, fmt.Errorf("%w: csops op %d wants %d bytes, outside [8, %d]",
				ErrNoVerifier, opDEREntitlements, required, maxEntitlementsBlob)
		}
		buf = make([]byte, required)
		errno = read(opDEREntitlements, buf)
	}
	if errno != 0 {
		return nil, denyErrno(opDEREntitlements, errno)
	}
	return decodeEntitlementsBlob(buf)
}

// denyErrno is the one errno table. ErrNoVerifier is the loud class on
// purpose: an op renumbering or a kernel refusal must be distinguishable from
// a policy denial at every log site.
func denyErrno(op uint32, errno syscall.Errno) error {
	switch {
	case errno == syscall.ESRCH:
		return fmt.Errorf("%w: csops op %d: %w", ErrPeerGone, op, errno)
	case errno == syscall.ENOENT && (op == opIdentity || op == opTeamID):
		return fmt.Errorf("%w: peer CodeDirectory declares no identifier or team (csops op %d: %w)", ErrUntrustedPeer, op, errno)
	default:
		return fmt.Errorf("%w: csops op %d: %w", ErrNoVerifier, op, errno)
	}
}
