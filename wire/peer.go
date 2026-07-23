package wire

import (
	"net"

	peeridentity "github.com/yasyf/daemonkit/peer"
)

// Peer is the shared OS-authenticated process identity.
type Peer = peeridentity.Identity

// PeerFromConn reads conn's peer credentials and snapshots the corresponding
// kernel process identity. Call it once per connection, right after accept.
func PeerFromConn(conn *net.UnixConn) (Peer, error) {
	return peeridentity.FromConn(conn)
}
