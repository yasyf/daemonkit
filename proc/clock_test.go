package proc

import (
	"sync"
	"time"
)

// fakeClock is a deterministic clock for poll-loop tests: each After advances
// Now by the requested duration and fires at once, so a bounded poll terminates
// without real sleeps.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	fire := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- fire
	return ch
}
