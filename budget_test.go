package daemonkit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

var budgetFractionEdges = []struct {
	name string
	of   float64
}{
	{"zero", 0},
	{"tiny", 5e-324},
	{"quarter", 0.25},
	{"half", 0.5},
	{"near one", 1 - 1e-9},
	{"one", 1},
	{"above one", 1.5},
	{"huge", 1e300},
	{"negative", -0.5},
	{"hugely negative", -1e300},
	{"nan", math.NaN()},
	{"positive infinity", math.Inf(1)},
	{"negative infinity", math.Inf(-1)},
}

var budgetParents = []struct {
	name  string
	grace Grace
}{
	{"expired", Grace(-time.Hour)},
	{"zero", 0},
	{"millisecond", Grace(time.Millisecond)},
	{"hour", Grace(time.Hour)},
	{"max grace", maxGrace},
	{"saturating", Grace(math.MaxInt64)},
}

func TestShareEdgeFractions(t *testing.T) {
	tests := []struct {
		name        string
		of          float64
		wantFull    bool
		wantExpired bool
	}{
		{"zero", 0, false, true},
		{"negative", -0.3, false, true},
		{"negative infinity", math.Inf(-1), false, true},
		{"nan", math.NaN(), false, true},
		{"interior", 0.5, false, false},
		{"one", 1, true, false},
		{"above one", 1.7, true, false},
		{"positive infinity", math.Inf(1), true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := Grace(time.Hour).mint("p")
			child := parent.Share("c", tt.of)
			if child.deadline.After(parent.deadline) {
				t.Fatalf("child deadline %v after parent %v", child.deadline, parent.deadline)
			}
			if tt.wantFull && !child.deadline.Equal(parent.deadline) {
				t.Errorf("child deadline = %v, want parent's %v", child.deadline, parent.deadline)
			}
			if tt.wantExpired {
				if got := child.Left(); got != 0 {
					t.Errorf("Left() = %v, want 0", got)
				}
			} else if child.Left() == 0 {
				t.Error("Left() = 0, want time remaining")
			}
		})
	}
}

func TestReserveEdgeFractions(t *testing.T) {
	tests := []struct {
		name            string
		of              float64
		wantWorkFull    bool
		wantWorkExpired bool
	}{
		{"zero tail", 0, true, false},
		{"negative tail", -2, true, false},
		{"negative infinity tail", math.Inf(-1), true, false},
		{"nan tail", math.NaN(), true, false},
		{"interior", 0.25, false, false},
		{"whole tail", 1, false, true},
		{"tail exceeding what remains", 3.5, false, true},
		{"positive infinity tail", math.Inf(1), false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := Grace(time.Hour).mint("p")
			work, tail := parent.Reserve("t", tt.of)
			if !tail.deadline.Equal(parent.deadline) {
				t.Fatalf("tail deadline = %v, want parent's %v", tail.deadline, parent.deadline)
			}
			if work.deadline.After(tail.deadline) {
				t.Fatalf("work deadline %v after tail %v", work.deadline, tail.deadline)
			}
			if tt.wantWorkFull && !work.deadline.Equal(parent.deadline) {
				t.Errorf("work deadline = %v, want parent's %v", work.deadline, parent.deadline)
			}
			if tt.wantWorkExpired {
				if got := work.Left(); got != 0 {
					t.Errorf("work Left() = %v, want 0", got)
				}
			}
		})
	}
}

func TestShareContainmentSweep(t *testing.T) {
	for _, pt := range budgetParents {
		for _, fe := range budgetFractionEdges {
			t.Run(pt.name+"/"+fe.name, func(t *testing.T) {
				parent := pt.grace.mint("p")
				child := parent.Share("c", fe.of)
				if child.deadline.After(parent.deadline) {
					t.Errorf("child deadline %v after parent %v", child.deadline, parent.deadline)
				}
			})
		}
	}
}

func TestReservePartitionSweep(t *testing.T) {
	for _, pt := range budgetParents {
		for _, fe := range budgetFractionEdges {
			t.Run(pt.name+"/"+fe.name, func(t *testing.T) {
				parent := pt.grace.mint("p")
				work, tail := parent.Reserve("t", fe.of)
				if !tail.deadline.Equal(parent.deadline) {
					t.Errorf("tail deadline = %v, want parent's %v exactly", tail.deadline, parent.deadline)
				}
				if work.deadline.After(tail.deadline) {
					t.Errorf("work deadline %v after tail %v", work.deadline, tail.deadline)
				}
			})
		}
	}
}

func TestFractionOf(t *testing.T) {
	tests := []struct {
		name string
		left time.Duration
		frac float64
		want time.Duration
	}{
		{"zero left", 0, 0.5, 0},
		{"zero frac", time.Hour, 0, 0},
		{"whole frac", time.Hour, 1, time.Hour},
		{"half hour", time.Hour, 0.5, 30 * time.Minute},
		{"saturated left half", math.MaxInt64, 0.5, 1 << 62},
		{"saturated left whole", math.MaxInt64, 1, math.MaxInt64},
		{"saturated left rounding to the ceiling", math.MaxInt64, 1 - 1e-17, math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fractionOf(tt.left, tt.frac); got != tt.want {
				t.Errorf("fractionOf(%d, %v) = %d, want %d", tt.left, tt.frac, got, tt.want)
			}
		})
	}
}

func TestReservePartitionMagnitude(t *testing.T) {
	parent := Grace(time.Hour).mint("p")
	leftBefore := parent.Left()
	work, tail := parent.Reserve("t", 0.25)
	leftAfter := parent.Left()
	reserved := tail.deadline.Sub(work.deadline)
	lo, hi := fractionOf(leftAfter, 0.25), fractionOf(leftBefore, 0.25)
	if reserved < lo || reserved > hi {
		t.Errorf("reserved = %v, want in [%v, %v]; the interval width measures only the clock advance between this test's Left reads and Reserve's own", reserved, lo, hi)
	}
}

func TestShareWithholdsComplement(t *testing.T) {
	parent := Grace(time.Hour).mint("p")
	leftBefore := parent.Left()
	child := parent.Share("c", 0.25)
	leftAfter := parent.Left()
	withheld := parent.deadline.Sub(child.deadline)
	lo, hi := fractionOf(leftAfter, 0.75), fractionOf(leftBefore, 0.75)
	if withheld < lo || withheld > hi {
		t.Errorf("withheld = %v, want in [%v, %v]; the interval width measures only the clock advance between this test's Left reads and Share's own", withheld, lo, hi)
	}
}

func TestSaturatedParent(t *testing.T) {
	parent := Grace(math.MaxInt64).mint("p")
	if parent.Left() == 0 {
		t.Fatal("saturated parent reads as already expired")
	}
	if got := parent.Share("zero", 0).Left(); got != 0 {
		t.Errorf("Share(zero, 0).Left() = %v, want 0", got)
	}
	work, tail := parent.Reserve("all", 1)
	if got := work.Left(); got != 0 {
		t.Errorf("work Left() = %v, want 0", got)
	}
	if !tail.deadline.Equal(parent.deadline) {
		t.Errorf("tail deadline = %v, want parent's %v", tail.deadline, parent.deadline)
	}
}

func TestShareChainRoundsToZero(t *testing.T) {
	prev := Grace(time.Second).mint("p")
	parent := prev
	for range 64 {
		next := prev.Share("s", 0.5)
		if next.deadline.After(prev.deadline) {
			t.Fatalf("chain extended: %v after %v", next.deadline, prev.deadline)
		}
		prev = next
	}
	if prev.deadline.After(parent.deadline) {
		t.Fatalf("chain end %v after its root %v", prev.deadline, parent.deadline)
	}
	if got := prev.Left(); got != 0 {
		t.Errorf("64-deep half chain Left() = %v, want 0", got)
	}
}

func TestExpiredParent(t *testing.T) {
	parent := Grace(-time.Second).mint("p")
	if got := parent.Left(); got != 0 {
		t.Fatalf("Left() = %v, want 0", got)
	}
	if child := parent.Share("c", 0.5); !child.deadline.Equal(parent.deadline) {
		t.Errorf("child deadline = %v, want parent's %v", child.deadline, parent.deadline)
	}
	work, tail := parent.Reserve("t", 0.5)
	if !work.deadline.Equal(parent.deadline) || !tail.deadline.Equal(parent.deadline) {
		t.Errorf("work = %v, tail = %v, want both at parent's %v", work.deadline, tail.deadline, parent.deadline)
	}
}

func TestZeroBudgetExpired(t *testing.T) {
	var b Budget
	if got := b.Left(); got != 0 {
		t.Fatalf("Left() = %v, want 0", got)
	}
	ctx, cancel := b.Context(context.Background())
	defer cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("zero Budget granted a live context")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx.Err() = %v, want DeadlineExceeded", ctx.Err())
	}
	if got := b.Share("c", 0.9).Left(); got != 0 {
		t.Errorf("Share Left() = %v, want 0", got)
	}
	work, tail := b.Reserve("t", 0.5)
	if work.Left() != 0 || tail.Left() != 0 {
		t.Errorf("Reserve Left() = %v, %v, want 0, 0", work.Left(), tail.Left())
	}
}

func TestContextCarriesDeadline(t *testing.T) {
	b := Grace(time.Hour).mint("p")
	ctx, cancel := b.Context(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("context has no deadline")
	}
	if !deadline.Equal(b.deadline) {
		t.Fatalf("ctx deadline = %v, want %v", deadline, b.deadline)
	}
}

func TestBudgetPath(t *testing.T) {
	b := Grace(time.Hour).mint("shutdown")
	if b.path != "shutdown" {
		t.Fatalf("path = %q, want %q", b.path, "shutdown")
	}
	if got := b.Share("requests", 0.4).path; got != "shutdown/requests" {
		t.Errorf("Share path = %q, want %q", got, "shutdown/requests")
	}
	work, tail := b.Reserve("children", 0.15)
	if work.path != "shutdown/work" {
		t.Errorf("work path = %q, want %q", work.path, "shutdown/work")
	}
	if tail.path != "shutdown/children" {
		t.Errorf("tail path = %q, want %q", tail.path, "shutdown/children")
	}
	var zero Budget
	if got := zero.Share("x", 0.5).path; got != "x" {
		t.Errorf("zero Share path = %q, want %q", got, "x")
	}
}
