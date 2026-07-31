// Package wire holds daemonkit's generated session frame codec: the exact
// frame layout, kind and flag enums, and length-prefixed packet encode/decode
// shared byte-for-byte with the Swift SessionFrameCodec.
package wire

//go:generate go run github.com/yasyf/daemonkit/internal/wiregen
