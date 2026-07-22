// Package duplexconn joins independent streams into one deadline-aware connection.
package duplexconn

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// New joins reader and writer into one connection.
func New(reader io.ReadCloser, writer io.WriteCloser) (net.Conn, error) {
	if reader == nil || writer == nil {
		return nil, errors.New("duplex reader and writer are required")
	}
	return &conn{reader: reader, writer: writer}, nil
}

type conn struct {
	reader io.ReadCloser
	writer io.WriteCloser

	closeOnce sync.Once
	closeErr  error

	mu         sync.Mutex
	readTimer  *time.Timer
	writeTimer *time.Timer
	closed     bool
}

func (c *conn) Read(payload []byte) (int, error)  { return c.reader.Read(payload) }
func (c *conn) Write(payload []byte) (int, error) { return c.writer.Write(payload) }

func (c *conn) Close() error {
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

func (c *conn) LocalAddr() net.Addr  { return address("local") }
func (c *conn) RemoteAddr() net.Addr { return address("spawned") }

func (c *conn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.SetReadDeadline(deadline), c.SetWriteDeadline(deadline))
}

func (c *conn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	resetTimer(&c.readTimer, deadline, func() { _ = c.reader.Close() })
	return nil
}

func (c *conn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	resetTimer(&c.writeTimer, deadline, func() { _ = c.writer.Close() })
	return nil
}

func resetTimer(timer **time.Timer, deadline time.Time, expire func()) {
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

type address string

func (a address) Network() string { return "duplex" }
func (a address) String() string  { return string(a) }
