// Package lifeproto defines daemonkit's exact-v2 lifecycle payloads carried by
// wire's length-prefixed persistent session protocol. Each payload is one flat
// JSON object with {"v":2,"op":...} plus operation-specific fields; there is no
// legacy parser, capability negotiation, or nested compatibility envelope.
//
// The Go and Swift bindings are generated from one schema (wire/lifeproto/gen)
// and pinned to identical bytes by testdata/golden.json, which both language
// test suites load. Any schema change is therefore an intentional protocol
// break and requires incrementing the exact protocol version.
package lifeproto

//go:generate go run ./gen
