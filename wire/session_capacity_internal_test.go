package wire

import (
	"testing"

	"github.com/yasyf/daemonkit/trust"
)

func TestSessionCapacityBucketsDoNotBorrow(t *testing.T) {
	role := trust.PeerRole("protected")
	newServer := func() *Server {
		return &Server{
			ordinarySessionSlots: make(chan struct{}, 1),
			protectedRoleSlots:   map[trust.PeerRole]chan struct{}{role: make(chan struct{}, 1)},
		}
	}
	t.Run("protected exhaustion preserves ordinary", func(t *testing.T) {
		server := newServer()
		protected, ok := server.acquireSessionCapacity(role, true)
		if !ok || !protected.protected || protected.role != role {
			t.Fatalf("first protected acquisition = %+v, %t", protected, ok)
		}
		if capacity, ok := server.acquireSessionCapacity(role, true); ok {
			server.releaseSessionCapacity(capacity)
			t.Fatal("protected acquisition borrowed ordinary capacity")
		}
		ordinary, ok := server.acquireSessionCapacity("", false)
		if !ok || ordinary.protected {
			t.Fatalf("ordinary capacity after protected exhaustion = %+v, %t", ordinary, ok)
		}
		server.releaseSessionCapacity(ordinary)
		server.releaseSessionCapacity(protected)
	})
	t.Run("ordinary exhaustion preserves protected", func(t *testing.T) {
		server := newServer()
		ordinary, ok := server.acquireSessionCapacity("", false)
		if !ok || ordinary.protected {
			t.Fatalf("first ordinary acquisition = %+v, %t", ordinary, ok)
		}
		if capacity, ok := server.acquireSessionCapacity("", false); ok {
			server.releaseSessionCapacity(capacity)
			t.Fatal("ordinary acquisition borrowed protected capacity")
		}
		protected, ok := server.acquireSessionCapacity(role, true)
		if !ok || !protected.protected {
			t.Fatalf("protected capacity after ordinary exhaustion = %+v, %t", protected, ok)
		}
		server.releaseSessionCapacity(protected)
		server.releaseSessionCapacity(ordinary)
	})
}
