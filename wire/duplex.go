package wire

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

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
	if reader == nil || writer == nil {
		return nil, errors.New("wire: duplex reader and writer are required")
	}
	return &duplexConn{reader: reader, writer: writer}, nil
}

type duplexConn struct {
	reader io.ReadCloser
	writer io.WriteCloser

	closeOnce sync.Once
	closeErr  error

	mu         sync.Mutex
	readTimer  *time.Timer
	writeTimer *time.Timer
	closed     bool
}

func (c *duplexConn) Read(payload []byte) (int, error)  { return c.reader.Read(payload) }
func (c *duplexConn) Write(payload []byte) (int, error) { return c.writer.Write(payload) }

func (c *duplexConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		if c.readTimer != nil {
			c.readTimer.Stop()
		}
		if c.writeTimer != nil {
			c.writeTimer.Stop()
		}
		c.mu.Unlock()
		c.closeErr = errors.Join(c.reader.Close(), c.writer.Close())
	})
	return c.closeErr
}

func (c *duplexConn) LocalAddr() net.Addr  { return duplexAddr("local") }
func (c *duplexConn) RemoteAddr() net.Addr { return duplexAddr("spawned") }

func (c *duplexConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.SetReadDeadline(deadline), c.SetWriteDeadline(deadline))
}

func (c *duplexConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	resetDeadlineTimer(&c.readTimer, deadline, func() { _ = c.reader.Close() })
	return nil
}

func (c *duplexConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	resetDeadlineTimer(&c.writeTimer, deadline, func() { _ = c.writer.Close() })
	return nil
}

func resetDeadlineTimer(timer **time.Timer, deadline time.Time, expire func()) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
	if deadline.IsZero() {
		return
	}
	delay := time.Until(deadline)
	if delay < 0 {
		delay = 0
	}
	*timer = time.AfterFunc(delay, expire)
}

type duplexAddr string

func (a duplexAddr) Network() string { return "duplex" }
func (a duplexAddr) String() string  { return string(a) }
