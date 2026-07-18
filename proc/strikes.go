package proc

import "time"

// Strikes is a sliding-window strike counter: the breaker trips once Limit
// strikes land within Window. Not safe for concurrent use.
type Strikes struct {
	Limit  int
	Window time.Duration

	times []time.Time
}

// Strike records a strike at now and reports whether the breaker is tripped
// (Limit or more strikes within Window of now).
func (s *Strikes) Strike(now time.Time) bool {
	s.times = append(s.times, now)
	return s.Struck(now)
}

// Struck reports whether Limit or more strikes lie within Window of now.
func (s *Strikes) Struck(now time.Time) bool {
	s.prune(now)
	return len(s.times) >= s.Limit
}

func (s *Strikes) prune(now time.Time) {
	cutoff := now.Add(-s.Window)
	kept := s.times[:0]
	for _, t := range s.times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.times = kept
}

// Times returns a copy of the retained strike times, for persistence.
func (s *Strikes) Times() []time.Time {
	return append([]time.Time(nil), s.times...)
}

// Load replaces the strike history, restoring a persisted window.
func (s *Strikes) Load(times []time.Time) {
	s.times = append([]time.Time(nil), times...)
}

// Reset clears the strike history.
func (s *Strikes) Reset() { s.times = nil }

// Ladder yields escalating durations across consecutive breaker trips; the
// last step repeats. Not safe for concurrent use.
type Ladder struct {
	// Steps are the durations in escalation order; empty yields zero.
	Steps []time.Duration

	level int
}

// Next returns the current step and advances toward the last, which repeats.
func (l *Ladder) Next() time.Duration {
	if len(l.Steps) == 0 {
		return 0
	}
	step := l.Steps[l.level]
	if l.level < len(l.Steps)-1 {
		l.level++
	}
	return step
}

// Reset returns the ladder to its first step.
func (l *Ladder) Reset() { l.level = 0 }
