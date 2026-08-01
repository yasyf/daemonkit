//go:build darwin

// Package launchd is the value-type model for one exact macOS user LaunchAgent
// and the stateless primitives that apply it.
//
// An Agent is one desired LaunchAgent specification; it renders to a
// deterministic plist that self-identifies as daemonkit-owned through the
// [OwnerEnvKey] environment marker. A Plan is an immutable, digest-addressed
// canonical set of agents. [Apply] installs, repairs, and kickstarts exactly
// one named agent; [Remove] boots out and deletes exactly the one plist
// daemonkit owns at one named label. The caller names every label daemonkit
// touches, because a caller always knows its own labels: nothing here scans
// ~/Library/LaunchAgents for what daemonkit owns, so one consumer's agents can
// never evict another's. The marker answers only "is the plist at my label
// mine?".
package launchd
