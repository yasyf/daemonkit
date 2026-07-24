// Package spawnedsession carries the module-private authority that lets wire
// consume proc's opaque spawned-session handles.
package spawnedsession

import (
	"net"
	"sync"
)

type authorityToken struct{ marker byte }

var wireToken = &authorityToken{marker: 1}

// Authority is available only inside the daemonkit module.
type Authority struct{ token *authorityToken }

// WireAuthority returns the capability used by wire's sealed session APIs.
func WireAuthority() Authority { return Authority{token: wireToken} }

// Valid reports whether the authority is daemonkit's wire capability.
func (a Authority) Valid() bool { return a.token == wireToken }

// Process identifies one exact live process without exposing proc internals.
type Process struct {
	PID        int
	UID        int
	StartTime  string
	Boot       string
	Comm       string
	Executable string
}

// Opened is one claimed spawned-session connection.
type Opened struct {
	Conn          net.Conn
	Peer          Process
	Nonce         [32]byte
	ReceiptDigest [32]byte
}

// OnceConn retains close ownership while allowing one wire claim.
type OnceConn struct {
	Mu      sync.Mutex
	Conn    net.Conn
	Claimed bool
	Closed  bool
}

// Claim returns the connection exactly once without transferring close ownership.
func (c *OnceConn) Claim() (net.Conn, bool) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.Claimed || c.Closed || c.Conn == nil {
		return nil, false
	}
	c.Claimed = true
	return c.Conn, true
}

// Close closes the retained connection exactly once.
func (c *OnceConn) Close() error {
	c.Mu.Lock()
	if c.Closed {
		c.Mu.Unlock()
		return nil
	}
	c.Closed = true
	conn := c.Conn
	c.Mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}
