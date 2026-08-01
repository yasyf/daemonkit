package daemonkit

import "sync"

// NewCapture builds an io.Writer retaining the first limit bytes and draining
// the rest, so a bounded stderr can never block its child on a full pipe. A
// zero limit retains nothing and still drains.
func NewCapture(limit Bytes) *Capture { return &Capture{limit: int(limit)} }

// Capture is a bounded, never-blocking sink for a spawned child's stderr.
type Capture struct {
	limit int

	mu        sync.Mutex
	data      []byte
	truncated bool
}

// Write retains what still fits under the limit and drains the rest. It never
// errors and never short-writes: a diagnostics sink that refused bytes would
// stall the child whose pipe it drains.
func (c *Capture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	room := min(c.limit-len(c.data), len(p))
	if room > 0 {
		c.data = append(c.data, p[:room]...)
	}
	if room < len(p) {
		c.truncated = true
	}
	return len(p), nil
}

// Bytes returns a copy of what was retained.
func (c *Capture) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.data...)
}

// Truncated reports whether any byte was drained rather than retained.
func (c *Capture) Truncated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.truncated
}
