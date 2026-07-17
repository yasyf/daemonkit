package supervise

import "time"

// clock is the package time seam: real wall time in production, a fake in tests
// so the ticker loop and grace/strike math run deterministically without sleeps.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
