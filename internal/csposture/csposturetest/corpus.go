// Package csposturetest is the shared corpus of dynamic code-signing status
// words every daemonkit verifier must agree on. Each verifier feeds the same
// cases through its own status-word seam, so a verdict that drifts on one
// side fails on that side's test.
package csposturetest

import "github.com/yasyf/daemonkit/internal/csposture"

// Case is one status word and the verdict each library-validation policy owes
// it: RequireLVDenies under csposture.RequireLibraryValidation,
// EntitlementLVDenies under csposture.LibraryValidationByEntitlement.
type Case struct {
	Name                string
	Status              int64
	RequireLVDenies     bool
	EntitlementLVDenies bool
}

// Cases returns the corpus. The two measured words come from real signed
// binaries: 0x22011301 is a --options runtime binary carrying
// disable-library-validation, 0x22011311 a hardened-runtime binary carrying
// allow-jit, which no status-word clause can deny.
func Cases() []Case {
	const hardened = csposture.Runtime | csposture.Hard | csposture.Enforcement
	return []Case{
		{"hardened runtime with CS_REQUIRE_LV", hardened | csposture.RequireLV, false, false},
		{"hardened runtime with CS_FORCED_LV", hardened | csposture.ForcedLV, false, false},
		{"hardened runtime with both LV bits", hardened | csposture.RequireLV | csposture.ForcedLV, false, false},
		{"measured allow-jit binary", 0x22011311, false, false},
		{"measured disable-library-validation binary", 0x22011301, true, false},
		{"hardened runtime with neither LV bit", hardened, true, false},
		{"no Hardened Runtime", csposture.Hard | csposture.Enforcement | csposture.RequireLV, true, true},
		{"CS_HARD clear", hardened&^csposture.Hard | csposture.ForcedLV, true, true},
		{"CS_ENFORCEMENT clear", hardened&^csposture.Enforcement | csposture.ForcedLV, true, true},
		{"CS_HARD and CS_ENFORCEMENT clear", csposture.Runtime | csposture.ForcedLV, true, true},
		{"CS_GET_TASK_ALLOW set", hardened | csposture.RequireLV | csposture.GetTaskAllow, true, true},
		{"CS_DEBUGGED set", hardened | csposture.RequireLV | csposture.Debugged, true, true},
	}
}
