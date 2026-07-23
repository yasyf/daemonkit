package daemon

import (
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type autoClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAutoClock() *autoClock {
	return &autoClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *autoClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *autoClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	fire := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- fire
	return ch
}

type constProber struct {
	id  proc.Identity
	err error
}

func (p constProber) probe(pid int) (proc.Identity, error) {
	if p.err == nil && p.id.PID == 0 {
		return proc.Identity{PID: pid, StartTime: p.id.StartTime, Comm: p.id.Comm}, nil
	}
	return p.id, p.err
}
