package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestIdleVetoedByAttachment(t *testing.T) {
	ac := newAutoClock()
	base := ac.Now()
	i := &IdleExit{Timeout: time.Hour, clock: ac, prober: constProber{}}

	i.Attach("cc-pool", 4242)
	if i.idle(base.Add(2 * time.Hour)) {
		t.Fatal("idle = true while an attachment is live, want vetoed")
	}
	i.Detach("cc-pool", 4242)
	if !i.idle(base.Add(2 * time.Hour)) {
		t.Fatal("idle = false after detach past the timeout, want idle")
	}
}

func TestIdleDeadAttachmentStopsVetoing(t *testing.T) {
	ac := newAutoClock()
	base := ac.Now()
	i := &IdleExit{Timeout: time.Hour, clock: ac, prober: constProber{err: proc.ErrNoProcess}}

	i.Attach("cc-pool", 4242)
	if !i.idle(base.Add(2 * time.Hour)) {
		t.Fatal("idle = false with only a dead attachment, want idle (pruned)")
	}
	if len(i.attached) != 0 {
		t.Errorf("attached = %d, want 0 (dead attachment pruned)", len(i.attached))
	}
}

func TestIdleVetoFunc(t *testing.T) {
	ac := newAutoClock()
	base := ac.Now()
	veto := true
	i := &IdleExit{Timeout: time.Hour, Veto: func() bool { return veto }, clock: ac, prober: constProber{}}
	i.Touch()

	if i.idle(base.Add(2 * time.Hour)) {
		t.Fatal("idle = true while Veto returns true, want suppressed")
	}
	veto = false
	if !i.idle(base.Add(2 * time.Hour)) {
		t.Fatal("idle = false after the veto lifts past the timeout, want idle")
	}
}

func TestIdleNotElapsed(t *testing.T) {
	ac := newAutoClock()
	base := ac.Now()
	i := &IdleExit{Timeout: time.Hour, clock: ac, prober: constProber{}}
	i.Touch()

	if i.idle(base.Add(30 * time.Minute)) {
		t.Fatal("idle = true within the timeout window, want not idle")
	}
	if !i.idle(base.Add(90 * time.Minute)) {
		t.Fatal("idle = false past the timeout window, want idle")
	}
}

func TestIdleRunFiresAfterTimeout(t *testing.T) {
	fired := false
	i := &IdleExit{
		Timeout:  time.Hour,
		Interval: 10 * time.Minute,
		Exit:     func(context.Context) { fired = true },
		clock:    newAutoClock(),
		prober:   constProber{},
	}
	i.Run(context.Background())
	if !fired {
		t.Fatal("Exit not called after the idle window elapsed")
	}
}

func TestIdleRunHonorsContext(t *testing.T) {
	fired := false
	i := &IdleExit{
		Timeout: time.Hour,
		Exit:    func(context.Context) { fired = true },
		clock:   neverClock{},
		prober:  constProber{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	i.Run(ctx)
	if fired {
		t.Fatal("Exit fired on a cancelled ctx, want no exit")
	}
}
