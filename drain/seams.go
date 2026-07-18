package drain

import (
	"time"

	"github.com/yasyf/daemonkit/proc"
)

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

// prober is the process-table seam, backed by proc.Probe in production.
type prober interface {
	probe(pid int) (proc.Identity, error)
	boot() (string, error)
}

type sysProber struct{}

func (sysProber) probe(pid int) (proc.Identity, error) { return proc.Probe(pid) }

func (sysProber) boot() (string, error) { return proc.BootID() }

func proberOrSys(p prober) prober {
	if p == nil {
		return sysProber{}
	}
	return p
}
