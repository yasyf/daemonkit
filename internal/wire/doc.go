// Package wire is daemonkit's persistent multiplexed unix-socket transport:
// the generated session frame codec (frame layout, kind and flag enums, and
// length-prefixed packet encode/decode shared byte-for-byte with the Swift
// SessionFrameCodec), the hand-written I/O codec over it, and the protocol-2
// session machinery — a two-lane {control, business} server bound to one
// Runtime, its client, the broker fd handoff, and spawned sessions over
// internal/proc's Cmd.Handoff.
//
// Admission is DoS-shaped: handshakes run off the accept goroutine, unverified
// connections draw from a bounded pending pool and are dropped without a write
// past it, and the pre-verification hello read runs under its own short
// deadline. A same-EUID local process holding pending slots is out of the
// threat model — the same user can SIGKILL the daemon outright, so there is no
// escalation; cross-user access is blocked by the 0700 user-owned socket
// directory and the unconditional same-EUID trust floor. SIGTERM is the
// guaranteed drain path; the over-the-wire drain verb is best-effort.
package wire

//go:generate go run github.com/yasyf/daemonkit/internal/wiregen
