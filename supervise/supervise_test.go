package supervise

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// fakeSubject is a scriptable Subject: mu guards the mutable signals and the
// recorded action sequence, while lock is the separate per-subject action lock
// the supervisor and a test's manual-kill path share. onStale, when set, fires
// after Stale reads its signal — the hook the race test uses to land a kill in
// the window between nomination and action.
type fakeSubject struct {
	name string
	lock sync.Locker
	mu   *sync.Mutex

	spawnedAt time.Time
	active    bool
	activeErr error
	stale     bool
	alive     bool
	aliveErr  error

	staleCalls int
	aliveCalls int
	seq        []string
	onStale    func()
}

func (f *fakeSubject) Name() string      { return f.name }
func (f *fakeSubject) Lock() sync.Locker { return f.lock }

func (f *fakeSubject) SpawnedAt() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spawnedAt
}

func (f *fakeSubject) Active(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active, f.activeErr
}

func (f *fakeSubject) Stale(context.Context) bool {
	f.mu.Lock()
	f.staleCalls++
	s, on := f.stale, f.onStale
	f.mu.Unlock()
	if on != nil {
		on()
	}
	return s
}

func (f *fakeSubject) Alive(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aliveCalls++
	return f.alive, f.aliveErr
}

func (f *fakeSubject) Kill(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq = append(f.seq, "kill")
	return nil
}

func (f *fakeSubject) Respawn(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq = append(f.seq, "respawn")
	return nil
}

func (f *fakeSubject) Park(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq = append(f.seq, "park")
	f.active = false
	return nil
}

func (f *fakeSubject) actions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seq...)
}

// testClock is a deterministic clock: Now is fixed for grace/strike math and
// After returns a channel a test fires by hand to drive the Run loop.
type testClock struct {
	mu   sync.Mutex
	now  time.Time
	tick chan time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), tick: make(chan time.Time, 1)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) After(time.Duration) <-chan time.Time { return c.tick }

func (c *testClock) fire() { c.tick <- c.Now() }

func newTestSupervisor(clk clock) *Supervisor {
	s := &Supervisor{Interval: time.Millisecond, StartupGrace: 45 * time.Second, Limit: 3, Window: 10 * time.Minute}
	s.clock = clk
	return s
}

func single(subj Subject) Roster {
	return func(context.Context) ([]Subject, error) { return []Subject{subj}, nil }
}

func newSubject(name string, spawnedAt time.Time) *fakeSubject {
	return &fakeSubject{name: name, lock: &sync.Mutex{}, mu: &sync.Mutex{}, spawnedAt: spawnedAt, active: true}
}

func count(seq []string, s string) int {
	n := 0
	for _, x := range seq {
		if x == s {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !pred() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func TestReconcileRespawnsConfirmedDead(t *testing.T) {
	clk := newTestClock()
	subj := newSubject("s", clk.Now().Add(-time.Hour))
	subj.stale = true
	subj.alive = false

	if err := newTestSupervisor(clk).tick(context.Background(), single(subj)); err != nil {
		t.Fatalf("tick: %v", err)
	}

	if got, want := subj.actions(), []string{"kill", "respawn"}; !slices.Equal(got, want) {
		t.Fatalf("actions = %v, want %v (kill the husk, then respawn)", got, want)
	}
	if subj.aliveCalls != 1 {
		t.Fatalf("aliveCalls = %d, want 1 (probe corroborates the staleness signal)", subj.aliveCalls)
	}
}

func TestReconcileSkips(t *testing.T) {
	clk := newTestClock()
	past := clk.Now().Add(-time.Hour)
	tests := []struct {
		name           string
		active         bool
		stale          bool
		alive          bool
		aliveErr       error
		wantStaleCalls int
		wantAliveCalls int
	}{
		{name: "inactive subject is never nominated", active: false, stale: true},
		{name: "no staleness signal, no probe", active: true, stale: false, wantStaleCalls: 1},
		{name: "live process is left running", active: true, stale: true, alive: true, wantStaleCalls: 1, wantAliveCalls: 1},
		{name: "unresolvable probe fails closed", active: true, stale: true, aliveErr: errors.New("probe boom"), wantStaleCalls: 1, wantAliveCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subj := newSubject("s", past)
			subj.active = tt.active
			subj.stale = tt.stale
			subj.alive = tt.alive
			subj.aliveErr = tt.aliveErr

			if err := newTestSupervisor(clk).tick(context.Background(), single(subj)); err != nil {
				t.Fatalf("tick: %v", err)
			}
			if got := subj.actions(); len(got) != 0 {
				t.Fatalf("expected no kill/respawn/park, got %v", got)
			}
			if subj.staleCalls != tt.wantStaleCalls {
				t.Fatalf("staleCalls = %d, want %d", subj.staleCalls, tt.wantStaleCalls)
			}
			if subj.aliveCalls != tt.wantAliveCalls {
				t.Fatalf("aliveCalls = %d, want %d", subj.aliveCalls, tt.wantAliveCalls)
			}
		})
	}
}

func TestStartupGraceSuppressesNomination(t *testing.T) {
	clk := newTestClock()
	subj := newSubject("s", clk.Now()) // spawned "now" → inside the grace window
	subj.stale = true
	subj.alive = false
	sup := newTestSupervisor(clk)

	if err := sup.tick(context.Background(), single(subj)); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := subj.actions(); len(got) != 0 {
		t.Fatalf("a subject inside startup grace must not be actioned, got %v", got)
	}
	if subj.staleCalls != 0 {
		t.Fatalf("a subject inside startup grace must not even be nominated; staleCalls = %d", subj.staleCalls)
	}

	// Age it past the grace window: it is now nominable, so the same dead subject respawns.
	subj.mu.Lock()
	subj.spawnedAt = clk.Now().Add(-time.Hour)
	subj.mu.Unlock()
	if err := sup.tick(context.Background(), single(subj)); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got, want := subj.actions(), []string{"kill", "respawn"}; !slices.Equal(got, want) {
		t.Fatalf("past grace the dead subject must respawn; actions = %v, want %v", got, want)
	}
}

// TestKillRacingRespawn lands a manual kill in the window between nomination and
// action: the supervisor nominates the dead subject, blocks on the shared
// per-subject lock, and a manual-kill path holding that lock flips the subject
// inactive before releasing. The under-lock re-read must observe the kill and
// take no action — no double-kill, no zombie respawn — and never reach the probe.
func TestKillRacingRespawn(t *testing.T) {
	clk := newTestClock()
	lock := &sync.Mutex{}
	nominated := make(chan struct{}, 1)
	subj := &fakeSubject{
		name: "s", lock: lock, mu: &sync.Mutex{},
		spawnedAt: clk.Now().Add(-time.Hour), active: true, stale: true, alive: false,
		onStale: func() { nominated <- struct{}{} },
	}
	sup := newTestSupervisor(clk)

	lock.Lock() // a manual-kill path is mid-action, holding the subject lock

	done := make(chan error, 1)
	go func() { done <- sup.tick(context.Background(), single(subj)) }()

	<-nominated // the tick has nominated and is about to take the lock
	subj.mu.Lock()
	subj.active = false // the manual kill's terminal effect, under the lock
	subj.mu.Unlock()
	lock.Unlock()

	if err := <-done; err != nil {
		t.Fatalf("tick: %v", err)
	}
	if got := subj.actions(); len(got) != 0 {
		t.Fatalf("a subject killed between nomination and action must not be re-actioned, got %v", got)
	}
	if subj.aliveCalls != 0 {
		t.Fatalf("the under-lock re-read must short-circuit before the probe; aliveCalls = %d", subj.aliveCalls)
	}
}

// TestBudgetParkStopsRespawns drives a subject that never recovers: it respawns
// exactly Limit times, then the exhausted strike budget parks it once and leaves
// it inactive, so no further respawn follows.
func TestBudgetParkStopsRespawns(t *testing.T) {
	clk := newTestClock()
	subj := newSubject("s", clk.Now().Add(-time.Hour))
	subj.stale = true
	subj.alive = false
	sup := newTestSupervisor(clk)

	for i := 0; i < sup.Limit+2; i++ {
		if err := sup.tick(context.Background(), single(subj)); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}

	seq := subj.actions()
	if r, k := count(seq, "respawn"), count(seq, "kill"); r != sup.Limit || k != sup.Limit {
		t.Fatalf("respawns = %d, kills = %d, want %d each; seq = %v", r, k, sup.Limit, seq)
	}
	if p := count(seq, "park"); p != 1 {
		t.Fatalf("park count = %d, want exactly 1; seq = %v", p, seq)
	}
	if seq[len(seq)-1] != "park" {
		t.Fatalf("park must be the last action (no respawn after it); seq = %v", seq)
	}
	subj.mu.Lock()
	active := subj.active
	subj.mu.Unlock()
	if active {
		t.Fatal("a parked subject must be left inactive")
	}
}

// TestStrikesPruneWithRoster: a name the roster stops surfacing loses its
// strike history, so churning unique names cannot grow the map forever.
func TestStrikesPruneWithRoster(t *testing.T) {
	clk := newTestClock()
	subj := newSubject("s", clk.Now().Add(-time.Hour))
	subj.stale = true
	subj.alive = false
	sup := newTestSupervisor(clk)

	if err := sup.tick(context.Background(), single(subj)); err != nil {
		t.Fatalf("tick: %v", err)
	}
	sup.mu.Lock()
	_, present := sup.strikes["s"]
	sup.mu.Unlock()
	if !present {
		t.Fatal("strike history expected after a respawn tick")
	}

	empty := func(context.Context) ([]Subject, error) { return nil, nil }
	if err := sup.tick(context.Background(), empty); err != nil {
		t.Fatalf("empty tick: %v", err)
	}
	sup.mu.Lock()
	_, present = sup.strikes["s"]
	sup.mu.Unlock()
	if present {
		t.Fatal("strike history must prune when the roster drops the name")
	}
}

func TestRunTicksUntilCancel(t *testing.T) {
	clk := newTestClock()
	subj := newSubject("s", clk.Now().Add(-time.Hour))
	subj.stale = true
	subj.alive = false
	sup := newTestSupervisor(clk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx, single(subj)) }()

	clk.fire()
	waitFor(t, "respawn", func() bool { return count(subj.actions(), "respawn") >= 1 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestSupervisorValidate(t *testing.T) {
	tests := []struct {
		name string
		sup  *Supervisor
	}{
		{"zero Interval", &Supervisor{Limit: 1, Window: time.Minute}},
		{"zero Limit", &Supervisor{Interval: time.Second, Window: time.Minute}},
		{"zero Window", &Supervisor{Interval: time.Second, Limit: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.sup.Run(context.Background(), single(newSubject("s", time.Now()))); err == nil {
				t.Fatal("Run must reject an incompletely configured Supervisor")
			}
		})
	}
}

func TestKeyedMutex(t *testing.T) {
	var k KeyedMutex
	a1, a2, b := k.Get("a"), k.Get("a"), k.Get("b")
	if a1 != a2 {
		t.Fatal("same key must return the same locker")
	}
	if a1 == b {
		t.Fatal("different keys must return different lockers")
	}

	a1.Lock()
	acquired := make(chan struct{})
	go func() {
		a2.Lock()
		close(acquired)
		a2.Unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("a second Lock on the same key acquired while it was held")
	case <-time.After(20 * time.Millisecond):
	}
	a1.Unlock()
	<-acquired
}
