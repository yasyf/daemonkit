// Package lifeproto defines daemonkit's exact-v2 lifecycle payloads: one flat
// JSON object per message, {"v":2,"op":...} plus op-specific fields. Both
// language bindings are generated from one schema (gen) and pinned
// byte-identical by testdata/golden.json; any schema change is a protocol
// break requiring an exact-version increment.
package lifeproto

//go:generate go run ./gen
