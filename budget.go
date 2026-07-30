package daemonkit

import (
	"context"
	"math"
	"time"
)

// Budget is a deadline being spent. Its fields are unexported, so not even
// internal packages can mint one from a duration: it enters from Shutdown or
// Handshake inside Serve, or from a caller ctx deadline at a client entry.
// The zero value is already expired, so an unthreaded Budget refuses work
// instead of silently granting forever.
type Budget struct {
	deadline time.Time
	path     string
}

func (g Grace) mint(name string) Budget {
	return Budget{deadline: time.Now().Add(time.Duration(g)), path: name}
}

// Share carves a named fraction of what remains; its deadline is never later
// than its parent's. An early-finishing phase's surplus flows to the next.
func (b Budget) Share(name string, of float64) Budget {
	withheld := fractionOf(b.Left(), 1-clampFraction(of))
	return Budget{deadline: b.deadline.Add(-withheld), path: b.join(name)}
}

// Reserve carves the tail out of b as a guaranteed remainder: work ends
// reserved before b's deadline and the tail (ack write, child settlement)
// runs to the deadline itself, so however long the work runs the tail still
// has its slice, and the tail cannot outlive the deadline it was carved
// from. work carries the path segment "work"; the tail carries name.
func (b Budget) Reserve(name string, of float64) (work, tail Budget) {
	reserved := fractionOf(b.Left(), clampFraction(of))
	work = Budget{deadline: b.deadline.Add(-reserved), path: b.join("work")}
	tail = Budget{deadline: b.deadline, path: b.join(name)}
	return work, tail
}

// Left is the time remaining before the deadline, never negative.
func (b Budget) Left() time.Duration {
	if left := time.Until(b.deadline); left > 0 {
		return left
	}
	return 0
}

// Context bounds parent by b's deadline.
func (b Budget) Context(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(parent, b.deadline)
}

func (b Budget) join(name string) string {
	if b.path == "" {
		return name
	}
	return b.path + "/" + name
}

func clampFraction(of float64) float64 {
	switch {
	case math.IsNaN(of), of <= 0:
		return 0
	case of >= 1:
		return 1
	default:
		return of
	}
}

func fractionOf(left time.Duration, frac float64) time.Duration {
	scaled := frac * float64(left)
	if scaled >= float64(left) {
		return left
	}
	return time.Duration(scaled)
}
