package proc

import "time"

// clock is the package time seam: real wall time in production, a fake in tests
// so bounded polls run deterministically without real sleeps.
type clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func clockOrReal(c clock) clock {
	if c == nil {
		return realClock{}
	}
	return c
}
