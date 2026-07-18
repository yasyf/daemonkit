// Package lifeproto is daemonkit's native lifecycle envelope over wire.Framing:
// one LF-delimited JSON object per message, always {"v":1,"op":...} plus
// op-specific fields at the top level (a FLAT envelope, not a nested payload).
//
// The wire format is frozen — field names, op strings, and key order are a
// compatibility contract with deployed peers and with the Swift DaemonKit peer,
// which speaks the same shape. Both bindings are generated from one schema
// (wire/lifeproto/gen) and pinned to identical bytes by a shared golden fixture
// (testdata/golden.json) that the Go and Swift test suites both load.
package lifeproto

//go:generate go run ./gen
