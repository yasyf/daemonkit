package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// TestIdleVetoedByAttachment: a live attachment suppresses idle exit; dropping it
// lets the daemon exit.
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

// TestIdleDeadAttachmentStopsVetoing: an attachment whose process has died is
// pruned and no longer vetoes.
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

// TestIdleVetoFunc: a Veto callback suppresses exit independent of attachments.
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

// TestIdleNotElapsed: within the timeout the daemon is not idle.
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

// TestIdleRunFiresAfterTimeout: Run calls Exit once the idle window elapses, with
// no attachment and no veto.
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

// TestIdleRunHonorsContext: a cancelled ctx stops Run without firing Exit.
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
