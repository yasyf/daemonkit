// Package csposture holds daemonkit's one dynamic code-signing status-word
// posture check. Every daemonkit verifier calls it, so a clause added here
// reaches all of them and no verifier can diverge on the status word.
package csposture

import "fmt"

// Dynamic code-signing status bits, verified against xnu's cs_blobs.h and
// Apple's CSCommon.h.
const (
	GetTaskAllow = 0x00000004 // CS_GET_TASK_ALLOW: get-task-allow entitlement
	ForcedLV     = 0x00000010 // CS_FORCED_LV: library validation forced by system policy
	Hard         = 0x00000100 // CS_HARD (kSecCodeStatusHard): executable page protection
	Enforcement  = 0x00001000 // CS_ENFORCEMENT: unsigned executable memory refused
	RequireLV    = 0x00002000 // CS_REQUIRE_LV: library validation required
	Runtime      = 0x00010000 // CS_RUNTIME: Hardened Runtime (codesign --options runtime)
	Debugged     = 0x10000000 // CS_DEBUGGED: ran with invalid pages under a debugger
)

// LibraryValidation selects which oracle proves that a peer enforces library
// validation.
type LibraryValidation int

const (
	// RequireLibraryValidation denies a status word carrying neither
	// CS_REQUIRE_LV nor CS_FORCED_LV. A verifier that reads only the status
	// word passes this: the LV clause is the sole clause that can deny a
	// binary signed --options runtime with library validation disabled
	// (measured status 0x22011301, whose CS_RUNTIME, CS_HARD and
	// CS_ENFORCEMENT are all set).
	RequireLibraryValidation LibraryValidation = iota
	// LibraryValidationByEntitlement leaves the clause to a verifier that
	// reads the peer's entitlement dictionary and denies the
	// library-validation-disabling entitlement there, exempting it exactly
	// when LibraryValidationEnforced already proves the peer enforces
	// library validation.
	LibraryValidationByEntitlement
)

// Check reports why a peer's dynamic code-signing status word fails
// daemonkit's posture floor, or nil when it passes. The reason carries no
// sentinel; each verifier wraps it in its own untrusted-peer error.
//
// Check proves exactly what the kernel's status word states: the Hardened
// Runtime is on, executable page protection and signature enforcement are
// intact, no debugger is attached or entitled, and — under
// RequireLibraryValidation — library validation is enforced. It cannot prove
// the absence of allow-jit or allow-dyld-environment-variables: both leave
// the status word bit-identical to a clean hardened-runtime binary (measured
// 0x22011311 on Slack, Cursor, 1Password and Claude.app), so no status-word
// clause can ever close them. Only the peer's entitlement dictionary rules
// them out, and that oracle lives in package trust.
func Check(status int64, lv LibraryValidation) error {
	if status&Runtime == 0 {
		return fmt.Errorf("peer lacks the Hardened Runtime (status 0x%x)", status)
	}
	if status&Hard == 0 {
		return fmt.Errorf("peer disables executable page protection (CS_HARD clear, status 0x%x)", status)
	}
	if status&Enforcement == 0 {
		return fmt.Errorf("peer permits unsigned executable memory (CS_ENFORCEMENT clear, status 0x%x)", status)
	}
	if status&GetTaskAllow != 0 {
		return fmt.Errorf("peer permits debugger attachment (CS_GET_TASK_ALLOW, status 0x%x)", status)
	}
	if status&Debugged != 0 {
		return fmt.Errorf("peer ran under a debugger (CS_DEBUGGED, status 0x%x)", status)
	}
	if lv == RequireLibraryValidation && !LibraryValidationEnforced(status) {
		return fmt.Errorf("peer does not enforce library validation (status 0x%x)", status)
	}
	return nil
}

// LibraryValidationEnforced reports whether the status word itself proves the
// peer enforces library validation.
func LibraryValidationEnforced(status int64) bool {
	return status&(RequireLV|ForcedLV) != 0
}
