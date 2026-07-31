// Package launchd is the value-type model for one exact macOS user LaunchAgent
// set and the stateless reconcile primitive that converges launchd to it.
//
// An Agent is one desired LaunchAgent specification; it renders to a
// deterministic plist that self-identifies as daemonkit-owned through the
// [OwnerEnvKey] environment marker. A Plan is an immutable, digest-addressed
// canonical set of agents. Converge drives /bin/launchctl to exactly a desired
// set, discovering the currently installed daemonkit agents from the marker so
// it needs no external store.
package launchd
