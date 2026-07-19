package wire

import (
	"fmt"
	"net"

	"github.com/yasyf/daemonkit/proc"
)

// Peer is the OS-authenticated identity of a connected unix-socket peer. Audit
// is the raw 32-byte darwin audit_token_t, or nil on linux; wire captures it
// verbatim and never interprets it (the trust layer does).
type Peer struct {
	PID        int
	UID        int
	StartTime  string
	Comm       string
	Boot       string
	Executable string
	Audit      []byte
}

// ProcessIdentity returns the peer's kernel process identity captured at accept.
func (p Peer) ProcessIdentity() proc.Identity {
	identity := proc.Identity{PID: p.PID, StartTime: p.StartTime, Comm: p.Comm, Boot: p.Boot, Executable: p.Executable}
	if token, err := proc.AuditTokenFromBytes(p.Audit); err == nil {
		identity.AuditToken = token
	}
	return identity
}

// MatchesProcess reports whether rec names the exact process instance that
// opened the accepted socket. Comm is informational and may change across exec.
func (p Peer) MatchesProcess(rec proc.Record) bool {
	return p.PID == rec.PID && p.StartTime != "" && p.StartTime == rec.StartTime &&
		p.Boot != "" && p.Boot == rec.Boot
}

// PeerFromConn reads conn's peer credentials and snapshots the corresponding
// kernel process identity. Call it once per connection, right after accept.
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
	identity, err := bindPeerIdentity(peer)
	if err != nil {
		return Peer{}, fmt.Errorf("wire: bind peer pid %d to audit token: %w", peer.PID, err)
	}
	peer.StartTime = identity.StartTime
	peer.Comm = identity.Comm
	peer.Boot = identity.Boot
	peer.Executable = identity.Executable
	return peer, nil
}
