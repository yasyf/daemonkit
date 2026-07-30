// Package csposturetest is the shared corpus of dynamic code-signing status
// words daemonkit's verifier must agree on. The verifier feeds the same cases
// through its own status-word seam, so a verdict that drifts fails there too.
package csposturetest

import "github.com/yasyf/daemonkit/internal/csposture"

// Case is one status word and the verdict csposture.Check owes it.
type Case struct {
	Name   string
	Status int64
	Denies bool
}

// Cases returns the corpus. The two measured words come from real signed
// binaries: 0x22011301 is a --options runtime binary carrying
// disable-library-validation, 0x22011311 a hardened-runtime binary carrying
// allow-jit, which no status-word clause can deny.
func Cases() []Case {
	const hardened = csposture.Valid | csposture.Runtime | csposture.Hard | csposture.Enforcement
	return []Case{
		{"hardened runtime with CS_REQUIRE_LV", hardened | csposture.RequireLV, false},
		{"hardened runtime with CS_FORCED_LV", hardened | csposture.ForcedLV, false},
		{"hardened runtime with both LV bits", hardened | csposture.RequireLV | csposture.ForcedLV, false},
		{"measured allow-jit binary", 0x22011311, false},
		{"measured disable-library-validation binary", 0x22011301, true},
		{"hardened runtime with neither LV bit", hardened, true},
		{"CS_VALID clear", hardened&^csposture.Valid | csposture.ForcedLV, true},
		{"CS_VALID clear with both LV bits", hardened&^csposture.Valid | csposture.RequireLV | csposture.ForcedLV, true},
		{"no Hardened Runtime", csposture.Valid | csposture.Hard | csposture.Enforcement | csposture.RequireLV, true},
		{"CS_HARD clear", hardened&^csposture.Hard | csposture.ForcedLV, true},
		{"CS_ENFORCEMENT clear", hardened&^csposture.Enforcement | csposture.ForcedLV, true},
		{"CS_HARD and CS_ENFORCEMENT clear", csposture.Valid | csposture.Runtime | csposture.ForcedLV, true},
		{"CS_GET_TASK_ALLOW set", hardened | csposture.RequireLV | csposture.GetTaskAllow, true},
		{"CS_DEBUGGED set", hardened | csposture.RequireLV | csposture.Debugged, true},
	}
}
