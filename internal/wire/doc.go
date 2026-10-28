// Package wire is daemonkit's persistent multiplexed unix-socket transport:
// the generated session frame codec (frame layout, kind and flag enums, and
// length-prefixed packet encode/decode shared byte-for-byte with the Swift
// SessionFrameCodec), the hand-written I/O codec over it, and the protocol-3
// session machinery — one session kind bound to one Runtime, its client, the
// broker fd handoff, and spawned sessions over internal/proc's Cmd.Handoff.
//
// Every process that can open the socket is trusted: the socket lives in the
// user's 0700 state directory and no peer is judged. Admission is still
// DoS-shaped: handshakes run off the accept goroutine, pending connections
// draw from a bounded pool and are dropped without a write past it, and the
// hello read runs under its own short deadline. SIGTERM is the guaranteed
// drain path; the over-the-wire drain verb is best-effort.
package wire

//go:generate go run github.com/yasyf/daemonkit/internal/wiregen
