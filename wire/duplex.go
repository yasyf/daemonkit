package wire

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/yasyf/daemonkit/internal/duplexconn"
	"github.com/yasyf/daemonkit/proc"
)

// SessionIdentity is daemonkit-issued authority for the process at the other
// end of an existing duplex session. Its proof cannot be constructed by a
// consumer.
type SessionIdentity struct {
	peer           Peer
	allowProtected bool
}

// SpawnedParentSessionIdentity binds the current process's live parent to an
// ordinary same-user session identity. Spawned identities never authorize
// protected lifecycle traffic.
func SpawnedParentSessionIdentity() (SessionIdentity, error) {
	pid := os.Getppid()
	identity, err := proc.Probe(pid)
	if err != nil {
		return SessionIdentity{}, fmt.Errorf("wire: probe spawned session parent %d: %w", pid, err)
	}
	return SessionIdentity{peer: Peer{
		PID: pid, UID: os.Geteuid(), StartTime: identity.StartTime,
		Comm: identity.Comm, Boot: identity.Boot, Executable: identity.Executable,
	}}, nil
}

func (i SessionIdentity) validate() error {
	if i.peer.PID <= 1 || i.peer.UID < 0 || i.peer.StartTime == "" || i.peer.Boot == "" {
		return errors.New("wire: invalid existing-session identity")
	}
	return nil
}

func (i SessionIdentity) authenticatedPeer() (Peer, bool, error) {
	if err := i.validate(); err != nil {
		return Peer{}, false, err
	}
	identity, err := proc.Probe(i.peer.PID)
	if err != nil {
		return Peer{}, false, fmt.Errorf("wire: revalidate existing-session peer %d: %w", i.peer.PID, err)
	}
	if identity.StartTime != i.peer.StartTime || identity.Boot != i.peer.Boot {
		return Peer{}, false, ErrUntrustedPeer
	}
	i.peer.Comm = identity.Comm
	i.peer.Executable = identity.Executable
	return i.peer, i.allowProtected, nil
}

// NewDuplexConn joins independent read and write streams into one daemonkit
// connection with close and deadline semantics suitable for a framed session.
func NewDuplexConn(reader io.ReadCloser, writer io.WriteCloser) (net.Conn, error) {
	conn, err := duplexconn.New(reader, writer)
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	return conn, nil
}
