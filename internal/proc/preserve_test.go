package proc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type observationClock struct {
	tick func()
}

func (c observationClock) Now() time.Time { return time.Now() }
func (c observationClock) After(time.Duration) <-chan time.Time {
	c.tick()
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func preservingTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "records.dkstate")
	s, err := OpenStoreWithPolicy(ladderContext(t, time.Second), path, PreserveOwned)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func recordedTestChild(t *testing.T, s *Store, session int) *Child {
	t.Helper()
	id := identity{pid: 4242, start: 1, boot: testBoot}
	rec := record{PID: id.pid, Start: id.start, Boot: id.boot, Generation: s.generation, Session: session, Policy: s.policy}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	return &Child{pid: id.pid, store: s, id: id, session: session, demand: make(chan time.Time, 1), settled: make(chan struct{})}
}

func TestPreserveNaturalLeaderWaitsForSessionWithoutSignals(t *testing.T) {
	s, path := preservingTestStore(t)
	child := recordedTestChild(t, s, 4242)
	live := true
	member := groupMember{pid: 5000, info: procInfo{start: 2, session: 4242, group: 5000}}
	s.prober = &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == member.pid && live {
				return member.info, nil
			}
			return procInfo{}, errNoProc
		},
		membersFn: func(int) ([]groupMember, error) {
			if live {
				return []groupMember{member}, nil
			}
			return nil, nil
		},
	}
	signals := &funcSignaler{}
	s.signaler = signals
	s.clock = observationClock{tick: func() {
		if !storeHolds(t, path, child.id) {
			t.Fatal("natural leader exit retired live session")
		}
		select {
		case <-child.settled:
			t.Fatal("natural leader exit published a live session as terminal")
		default:
		}
		live = false
	}}
	exited := make(chan status, 1)
	exited <- status{}
	s.driveExit(child, child.id, child.session, exited, s.clock)
	if len(signals.signals()) != 0 || child.exit.Reap != ReapAbsent || child.exit.Record != RecordRemoved {
		t.Fatalf("exit=%+v signals=%v", child.exit, signals.signals())
	}
}

func TestPreserveWaitTimeoutRetainsRecord(t *testing.T) {
	s, path := preservingTestStore(t)
	child := recordedTestChild(t, s, 0)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	cancel()
	exit, err := child.WaitNatural(ctx)
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, context.Canceled) || exit.Reap != reapUndetermined {
		t.Fatalf("WaitNatural() = %+v, %v", exit, err)
	}
	if !storeHolds(t, path, child.id) {
		t.Fatal("timed-out wait retired record")
	}
}

func TestPreserveObservationExcludesOnlyExactLeader(t *testing.T) {
	s, _ := preservingTestStore(t)
	child := recordedTestChild(t, s, 4242)
	member := groupMember{pid: 5000, info: procInfo{start: 2, session: 4242}}
	s.prober = &funcProber{
		probeFn: func(pid int) (procInfo, error) {
			if pid == child.pid {
				return procInfo{start: 1, session: 4242}, nil
			}
			return member.info, nil
		},
		membersFn: func(int) ([]groupMember, error) { return []groupMember{member}, nil },
	}
	o, err := s.Observe(t.Context(), []*Child{child})
	if err != nil || o.Complete() || len(o.Scopes) != 1 || !o.Scopes[0].Excluded || len(o.Scopes[0].Members) != 2 {
		t.Fatalf("Observe() = %+v, %v", o, err)
	}
	s.prober = &funcProber{probeFn: func(int) (procInfo, error) { return procInfo{start: 1, session: 4242}, nil }}
	o, err = s.Observe(t.Context(), []*Child{child})
	if err != nil || !o.Complete() {
		t.Fatalf("leader-only exclusion = %+v, %v", o, err)
	}
	replacement := Child{store: s, id: identity{pid: child.pid, start: 3, boot: testBoot}}
	o, err = s.Observe(t.Context(), []*Child{&replacement})
	if err != nil || o.Complete() || o.Scopes[0].Excluded {
		t.Fatalf("replacement exclusion = %+v, %v", o, err)
	}
}

func TestPreserveReusedPIDNeverSignaled(t *testing.T) {
	s, _ := preservingTestStore(t)
	child := recordedTestChild(t, s, 0)
	s.prober = &funcProber{probeFn: func(int) (procInfo, error) { return procInfo{start: 99}, nil }}
	signals := &funcSignaler{}
	s.signaler = signals
	scope, err := child.Observe(t.Context())
	if err != nil || scope.Reap != ReapReused || !scope.complete() || len(signals.signals()) != 0 {
		t.Fatalf("Observe() = %+v, %v signals=%v", scope, err, signals.signals())
	}
}

func TestPreserveAdoptedWaitsForExactExit(t *testing.T) {
	s, path := preservingTestStore(t)
	live := true
	s.prober = &funcProber{probeFn: func(int) (procInfo, error) {
		if live {
			return procInfo{start: 1}, nil
		}
		return procInfo{}, errNoProc
	}}
	signals := &funcSignaler{}
	s.signaler = signals
	adopted, err := s.Adopt(ladderContext(t, time.Second), 4242)
	if err != nil {
		t.Fatal(err)
	}
	if err := adopted.Release(); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("live Release() = %v", err)
	}
	s.clock = observationClock{tick: func() {
		if !storeHolds(t, path, adopted.id) {
			t.Fatal("live adopted record retired")
		}
		live = false
	}}
	reap, err := adopted.ObserveAndRetire(ladderContext(t, time.Second))
	if err != nil || reap != ReapAbsent || storeHolds(t, path, adopted.id) || len(signals.signals()) != 0 {
		t.Fatalf("ObserveAndRetire() = %d, %v signals=%v", reap, err, signals.signals())
	}
}

func TestPreserveRecoveryPolicySurvivesDefaultReopen(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "preserved", true: "legacy-upgraded"}[legacy], func(t *testing.T) {
			s, path := newTestStore(t)
			policy := PreserveOwned
			if legacy {
				policy = TerminateOwned
			}
			rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation, Policy: policy}
			if err := s.add(t.Context(), rec); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if legacy {
				upgraded, err := OpenStoreWithPolicy(ladderContext(t, time.Second), path, PreserveOwned)
				if err != nil {
					t.Fatal(err)
				}
				if err := upgraded.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened := openTestStore(t, path)
			reopened.prober = &funcProber{probeFn: func(int) (procInfo, error) { return procInfo{start: 1}, nil }}
			signals := &funcSignaler{}
			reopened.signaler = signals
			reclaimed, _, err := reopened.Recover(ladderContext(t, time.Second))
			if !errors.Is(err, ErrUnsettled) || len(reclaimed) != 0 || len(signals.signals()) != 0 || !storeHolds(t, path, rec.id()) {
				t.Fatalf("Recover() = %+v, %v signals=%v", reclaimed, err, signals.signals())
			}
		})
	}
}

func TestPreserveSnapshotDeadlineAndClosedStoreFailClosed(t *testing.T) {
	s, _ := preservingTestStore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	if !s.send(func(*records) { close(entered); <-release }) {
		t.Fatal("writer closed")
	}
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	o, err := s.Observe(ctx, nil)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) || o.Complete() {
		t.Fatalf("blocked Observe() = %+v, %v", o, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = s.Observe(t.Context(), nil)
	if err == nil || o.Complete() {
		t.Fatalf("closed Observe() = %+v, %v", o, err)
	}
}

func TestPreserveRunRefusesBeforeSpawn(t *testing.T) {
	s, _ := preservingTestStore(t)
	_, err := s.Run(ladderContext(t, time.Second), Cmd{Path: "/bin/cat"}, func(*Child) { t.Fatal("Run spawned") })
	if err == nil {
		t.Fatal("Run accepted preserving store")
	}
}

func TestPreserveUncertainAndLiveScopesKeepRecords(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "uncertain"}[uncertain], func(t *testing.T) {
			s, path := preservingTestStore(t)
			child := recordedTestChild(t, s, 0)
			s.prober = &funcProber{probeFn: func(int) (procInfo, error) {
				if uncertain {
					return procInfo{}, errors.New("probe denied")
				}
				return procInfo{start: 1}, nil
			}}
			o, err := s.Observe(t.Context(), nil)
			if o.Complete() || (err != nil) != uncertain || o.Scopes[0].Uncertain != uncertain {
				t.Fatalf("Observe() = %+v, %v", o, err)
			}
			if err := s.RetireQuiet(t.Context()); !errors.Is(err, ErrUnsettled) {
				t.Fatalf("RetireQuiet() = %v", err)
			}
			if !storeHolds(t, path, child.id) {
				t.Fatal("unsettled record retired")
			}
		})
	}
}

func TestPreserveQuietPriorGenerationRetires(t *testing.T) {
	s, path := preservingTestStore(t)
	rec := record{PID: 4242, Start: 1, Boot: testBoot, Generation: s.generation + 1, Policy: PreserveOwned}
	if err := s.add(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	s.prober = &funcProber{probeFn: func(int) (procInfo, error) { return procInfo{}, errNoProc }}
	o, err := s.Observe(t.Context(), nil)
	if err != nil || !o.Complete() || len(o.Scopes) != 1 {
		t.Fatalf("Observe() = %+v, %v", o, err)
	}
	if err := s.RetireQuiet(t.Context()); err != nil {
		t.Fatal(err)
	}
	if storeHolds(t, path, rec.id()) {
		t.Fatal("quiet prior record retained")
	}
}

func TestPreserveNaturalWaitsRequireDeadline(t *testing.T) {
	s, path := preservingTestStore(t)
	child := recordedTestChild(t, s, 0)
	if _, err := child.WaitNatural(context.Background()); err == nil {
		t.Fatal("WaitNatural accepted an unbounded context")
	}
	adopted := &Adopted{store: s, id: child.id}
	if _, err := adopted.ObserveAndRetire(context.Background()); err == nil {
		t.Fatal("ObserveAndRetire accepted an unbounded context")
	}
	if !storeHolds(t, path, child.id) {
		t.Fatal("unbounded wait retired record")
	}
}

func TestPreserveDemandAfterLeaderExitNeverSignalsReplacement(t *testing.T) {
	s, path := preservingTestStore(t)
	child := recordedTestChild(t, s, 4242)
	child.demanded = time.Now().Add(time.Second)
	s.prober = &funcProber{
		probeFn: func(int) (procInfo, error) {
			return procInfo{start: 99, session: 4242, group: 4242}, nil
		},
		membersFn: func(int) ([]groupMember, error) {
			t.Fatal("replacement session enumerated for termination")
			return nil, nil
		},
	}
	signals := &funcSignaler{}
	s.signaler = signals
	exited := make(chan status, 1)
	exited <- status{}
	s.driveExit(child, child.id, child.session, exited, realClock{})
	if child.exit.Reap != ReapReused || len(signals.signals()) != 0 {
		t.Fatalf("exit=%+v signals=%v", child.exit, signals.signals())
	}
	if storeHolds(t, path, child.id) {
		t.Fatal("exact replaced identity was not retired")
	}
}
