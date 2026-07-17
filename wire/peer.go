package wire

import (
	"fmt"
	"net"
)

// Peer is the OS-authenticated identity of a connected unix-socket peer. Audit
// is the raw 32-byte darwin audit_token_t, or nil on linux; wire captures it
// verbatim and never interprets it (the trust layer does).
type Peer struct {
	PID   int
	UID   int
	Audit []byte
}

// PeerFromConn reads conn's peer credentials with one getsockopt. Call it once
// per connection, right after accept.
func PeerFromConn(conn *net.UnixConn) (Peer, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Peer{}, fmt.Errorf("wire: syscall conn: %w", err)
	}
	var (
		peer  Peer
		opErr error
	)
	if err := raw.Control(func(fd uintptr) { peer, opErr = peerFromFD(int(fd)) }); err != nil {
		return Peer{}, fmt.Errorf("wire: control fd: %w", err)
	}
	if opErr != nil {
		return Peer{}, opErr
	}
	return peer, nil
}
