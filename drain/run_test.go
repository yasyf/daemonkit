package drain

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

type runEnv struct {
	dotdir  string
	gen     Generation
	res     *fakeResources
	strikes *StrikeStore
	spawns  atomic.Int32
}

func newRunEnv(t *testing.T, rows ...Row) (*runEnv, *RunConfig) {
	t.Helper()
	e := &runEnv{dotdir: t.TempDir()}
	e.gen = seedOwner(t, newGen(t, e.dotdir, "g1"), deadOwner())
	keys := make([]Key, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, r.Key)
	}
	if len(rows) > 0 {
		mustApply(t, e.gen.journal(), rows...)
	}
	e.res = &fakeResources{keys: keys}
	e.strikes = &StrikeStore{Path: filepath.Join(e.dotdir, "strikes.json")}
	cfg := &RunConfig{
		Generation:     e.gen,
		Canonical:      NewJournal(filepath.Join(e.dotdir, "canonical.json")),
		Resources:      e.res,
		CanonicalAlive: func(context.Context) Liveness { return Alive },
		Ready:          func(context.Context) bool { return true },
		Spawn: func(ctx context.Context) error {
			if err := e.strikes.SpawnGate()(ctx); err != nil {
				return err
			}
			e.spawns.Add(1)
			return nil
		},
		Backoff: proc.Backoff{Base: time.Nanosecond, Cap: time.Nanosecond},
		Log:     discardLog(),
		clock:   newAutoClock(),
	}
	return e, cfg
}

func cancelAfterTicks(cfg *RunConfig, n int, cancel context.CancelFunc, live Liveness) {
	ticks := 0
	cfg.CanonicalAlive = func(context.Context) Liveness {
		ticks++
		if ticks >= n {
			cancel()
		}
		return live
	}
}

func TestRunYieldsIdleAndExitsAtZero(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	var fence *fakeFence
	e.res.seize = func(Key) (Fence, error) {
		fence = &fakeFence{held: true}
		return fence, nil
	}
	if err := Run(context.Background(), *cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"keys *", "seize k1", "attest k1", "yield k1"}
	got := e.res.calls()
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if !fence.released {
		t.Error("fence not released after journal advance")
	}
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("generation dir not removed at zero: %v", err)
	}
}

func TestRunBusyAttestAbortsAndRestores(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	e.res.attest = func(Key) (IdleVerdict, error) { return IdleBusy, nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 2, cancel, Alive)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	log := e.res.calls()
	indexOf(t, log, "restore k1", 0)
	for _, call := range log {
		if call == "yield k1" {
			t.Error("busy resource was yielded")
		}
	}
	row := mustRows(t, e.gen.journal())["k1"]
	if row.State != RowPending || row.Seq != 1 {
		t.Errorf("row advanced despite busy attest: %+v", row)
	}
}

func TestRunCanceledAttestRestoresWithLiveContext(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	ctx, cancel := context.WithCancel(context.Background())
	fence := &fakeFence{held: true}
	e.res.seize = func(Key) (Fence, error) { return fence, nil }
	e.res.attest = func(Key) (IdleVerdict, error) {
		cancel()
		return IdleUndetermined, ctx.Err()
	}
	e.res.restore = func(ctx context.Context, _ Key, fence Fence) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		fence.Release()
		return nil
	}

	yielded, err := cfg.sweepKey(ctx, "k1", Row{Key: "k1", Seq: 1, State: RowPending})
	if err != nil {
		t.Fatalf("sweepKey: %v", err)
	}
	if yielded {
		t.Error("sweepKey yielded after canceled idle attestation")
	}
	if !fence.released {
		t.Error("fence not released by Restore after sweep context cancellation")
	}
	if row := mustRows(t, e.gen.journal())["k1"]; row != (Row{Key: "k1", Seq: 1, State: RowPending}) {
		t.Errorf("row = %+v, want pending seq 1", row)
	}
}

func TestRunLostFenceRestoresWithoutAdvance(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	e.res.seize = func(Key) (Fence, error) { return &fakeFence{held: false}, nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 2, cancel, Alive)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	log := e.res.calls()
	indexOf(t, log, "restore k1", 0)
	for _, call := range log {
		if call == "yield k1" {
			t.Error("lost fence still yielded")
		}
	}
	row := mustRows(t, e.gen.journal())["k1"]
	if row.State != RowPending || row.Seq != 1 {
		t.Errorf("row advanced despite lost fence: %+v", row)
	}
}

func TestRunWedgedYieldRestoresBeforeNextAttempt(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	yields := 0
	e.res.yield = func(Key, Fence) error {
		yields++
		if yields == 1 {
			return errors.New("teardown wedged")
		}
		return nil
	}
	if err := Run(context.Background(), *cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	log := e.res.calls()
	y1 := indexOf(t, log, "yield k1", 0)
	r1 := indexOf(t, log, "restore k1", y1)
	indexOf(t, log, "seize k1", r1)
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("generation dir not removed after retry succeeded: %v", err)
	}
}

func TestRunCanceledAfterYieldRecordsAndReleases(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	ctx, cancel := context.WithCancel(context.Background())
	fence := &fakeFence{held: true}
	e.res.seize = func(Key) (Fence, error) { return fence, nil }

	journal := e.gen.journal()
	lock, err := (proc.FileLockSpec{
		Path:     journal.lockPath(),
		Mode:     proc.FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatalf("hold journal lock: %v", err)
	}
	released := make(chan struct{})
	e.res.yield = func(Key, Fence) error {
		cancel()
		go func() {
			time.Sleep(50 * time.Millisecond)
			lock.Close()
			close(released)
		}()
		return nil
	}

	yielded, err := cfg.sweepKey(ctx, "k1", Row{Key: "k1", Seq: 1, State: RowPending})
	<-released
	if err != nil {
		t.Fatalf("sweepKey: %v", err)
	}
	if !yielded {
		t.Error("sweepKey did not report the committed handoff")
	}
	if !fence.released {
		t.Error("fence not released after committed handoff")
	}
	want := Row{Key: "k1", Seq: 2, State: RowYielded}
	if row := mustRows(t, journal)["k1"]; row != want {
		t.Errorf("row = %+v, want %+v", row, want)
	}
}

func TestRunPostYieldJournalFailureReleasesFence(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	fence := &fakeFence{held: true}
	e.res.seize = func(Key) (Fence, error) { return fence, nil }

	journal := e.gen.journal()
	if err := os.Remove(journal.lockPath()); err != nil {
		t.Fatalf("remove journal lock: %v", err)
	}
	if err := os.Mkdir(journal.lockPath(), 0o700); err != nil {
		t.Fatalf("wedge journal lock: %v", err)
	}

	yielded, err := cfg.sweepKey(context.Background(), "k1", Row{Key: "k1", Seq: 1, State: RowPending})
	if err == nil {
		t.Fatal("sweepKey succeeded after the journal write was wedged, want error")
	}
	if yielded {
		t.Error("sweepKey reported yielded after the journal write failed")
	}
	if !fence.released {
		t.Error("fence not released after committed handoff and journal failure")
	}
	if fence.Held() {
		t.Error("fence still held after release on the post-yield journal-failure path")
	}
	if err := os.Remove(journal.lockPath()); err != nil {
		t.Fatalf("remove wedged journal lock: %v", err)
	}
	if row := mustRows(t, journal)["k1"]; row != (Row{Key: "k1", Seq: 1, State: RowPending}) {
		t.Errorf("row = %+v, want recoverable pending seq 1", row)
	}
}

func TestRunSuccessorNotReadyNoHandoff(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	var ready atomic.Bool
	cfg.Ready = func(context.Context) bool { return ready.Load() }
	ticks := 0
	cfg.CanonicalAlive = func(context.Context) Liveness {
		ticks++
		if ticks == 3 {
			ready.Store(true)
		}
		return Alive
	}
	if err := Run(context.Background(), *cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ticks < 3 {
		t.Errorf("Run exited after %d ticks, before the successor became ready", ticks)
	}
	seizes, keyScans := 0, 0
	for _, call := range e.res.calls() {
		switch call {
		case "seize k1":
			seizes++
		case "keys *":
			keyScans++
		}
	}
	if seizes != 1 || keyScans != 1 {
		t.Errorf("seizes=%d keyScans=%d; want 1 each (no handoff while not ready)", seizes, keyScans)
	}
}

func TestRunKeysErrorNeverZeroCandidates(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	e.res.keysErr = errors.New("process enumeration failed")
	e.res.seize = func(Key) (Fence, error) { return nil, errors.New("no such resource") }
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 2, cancel, Alive)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	row := mustRows(t, e.gen.journal())["k1"]
	if row.State != RowPending {
		t.Errorf("enumeration error treated as zero candidates: %+v", row)
	}
}

func TestRunProvenAbsentKeyIsTerminal(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	e.res.keys = nil
	if err := Run(context.Background(), *cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, call := range e.res.calls() {
		if call == "seize k1" || call == "yield k1" {
			t.Errorf("absent key swept: %v", e.res.calls())
		}
	}
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("generation dir not removed: %v", err)
	}
}

func TestRunRespawnStrikeGatedThenParks(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	cfg.Ready = func(context.Context) bool { return false }
	e.strikes.Ladder = []time.Duration{time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 6, cancel, Dead)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if got := e.spawns.Load(); got != 3 {
		t.Errorf("spawns = %d, want exactly 3 before the breaker parks", got)
	}
}

func TestRunRecordsStrikeBeforeSpawn(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	cfg.CanonicalAlive = func(context.Context) Liveness { return Dead }
	launched := false
	cfg.Spawn = func(ctx context.Context) error {
		if err := e.strikes.SpawnGate()(ctx); err != nil {
			return err
		}
		launched = true
		state, err := e.strikes.load()
		if err != nil {
			t.Fatalf("load strikes at launch: %v", err)
		}
		if len(state.Times) != 1 {
			t.Fatalf("durable strikes at launch = %d, want 1", len(state.Times))
		}
		return nil
	}
	cfg.babysit(context.Background())
	if !launched {
		t.Fatal("launch never happened")
	}
}

func TestSweepSkipsCanonicalOwnedRows(t *testing.T) {
	e, cfg := newRunEnv(
		t,
		Row{Key: "stale", Seq: 5, State: RowPending},
		Row{Key: "mine", Seq: 3, State: RowPending},
	)
	mustApply(t, cfg.Canonical, Row{Key: "stale", Seq: 9, State: RowPending})
	if _, err := cfg.sweep(context.Background(), NewBreakers(cfg.perKeyBackoff()), cfg.clock); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	for _, call := range e.res.calls() {
		if call == "seize stale" || call == "yield stale" {
			t.Fatalf("canonical-owned key swept: %v", e.res.calls())
		}
	}
	rows := mustRows(t, e.gen.journal())
	if rows["stale"].State != RowYielded {
		t.Errorf("stale row = %+v, want terminalized", rows["stale"])
	}
	if canonical := mustRows(t, cfg.Canonical)["stale"]; canonical != (Row{Key: "stale", Seq: 9, State: RowPending}) {
		t.Errorf("canonical row disturbed: %+v", canonical)
	}
	if got := indexOf(t, e.res.calls(), "yield mine", 0); got < 0 {
		t.Errorf("generation-owned key not swept: %v", e.res.calls())
	}
}

func TestRunUndeterminedCanonicalNeverRespawns(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	cfg.Ready = func(context.Context) bool { return false }
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 4, cancel, Undetermined)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if got := e.spawns.Load(); got != 0 {
		t.Errorf("spawns = %d, want 0 on Undetermined", got)
	}
	st, err := e.strikes.load()
	if err != nil {
		t.Fatalf("load strikes: %v", err)
	}
	if len(st.Times) != 0 {
		t.Errorf("strikes recorded on Undetermined: %+v", st)
	}
}

func TestStrikeStorePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strikes.json")
	ctx := context.Background()
	t0 := time.Unix(1_700_000_000, 0)
	s1 := StrikeStore{Path: path, Limit: 3, Window: 10 * time.Minute, Ladder: []time.Duration{time.Minute, 5 * time.Minute}}

	for i := range 2 {
		allowed, until, err := s1.Gate(ctx, t0.Add(time.Duration(i)*time.Second))
		if err != nil || !allowed || !until.IsZero() {
			t.Fatalf("gate %d: allowed=%v until=%v err=%v", i, allowed, until, err)
		}
	}
	allowed, until, err := s1.Gate(ctx, t0.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed || !until.Equal(t0.Add(2*time.Second).Add(time.Minute)) {
		t.Fatalf("third gate: allowed=%v until=%v, want the threshold attempt admitted with park to +1m", allowed, until)
	}

	s2 := StrikeStore{Path: path, Limit: 3, Window: 10 * time.Minute, Ladder: []time.Duration{time.Minute, 5 * time.Minute}}
	parked, until2, err := s2.Parked(ctx, t0.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !parked || !until2.Equal(until) {
		t.Errorf("restart lost the park: parked=%v until=%v want %v", parked, until2, until)
	}
	allowed, until3, err := s2.Gate(ctx, t0.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !allowed || !until3.Equal(t0.Add(2*time.Minute).Add(5*time.Minute)) {
		t.Errorf("ladder level lost across restart: allowed=%v until=%v, want +5m step", allowed, until3)
	}
}

func TestStrikeGateRefusesWhileParkedWithoutRecording(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strikes.json")
	ctx := context.Background()
	t0 := time.Unix(1_700_000_000, 0)
	s := StrikeStore{Path: path, Limit: 1, Window: 10 * time.Minute, Ladder: []time.Duration{time.Hour}}
	allowed, until, err := s.Gate(ctx, t0)
	if err != nil || !allowed || !until.Equal(t0.Add(time.Hour)) {
		t.Fatalf("threshold gate: allowed=%v until=%v err=%v, want admitted with park to +1h", allowed, until, err)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	recorded := len(st.Times)
	allowed, until2, err := s.Gate(ctx, t0.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if allowed || !until2.Equal(until) {
		t.Fatalf("parked gate: allowed=%v until=%v, want refusal until %v", allowed, until2, until)
	}
	st, err = s.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Times) != recorded {
		t.Errorf("refused attempt recorded a strike: %d -> %d", recorded, len(st.Times))
	}
}

func TestStrikeGateSerializesConcurrentCallers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strikes.json")
	s := StrikeStore{Path: path, Limit: 1, Window: 10 * time.Minute, Ladder: []time.Duration{time.Hour}}
	t0 := time.Unix(1_700_000_000, 0)
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, _, err := s.Gate(context.Background(), t0)
			if err != nil {
				t.Error(err)
				return
			}
			results <- allowed
		}()
	}
	wg.Wait()
	close(results)
	admitted := 0
	for a := range results {
		if a {
			admitted++
		}
	}
	if admitted != 1 {
		t.Errorf("admitted = %d, want exactly 1: concurrent gates must serialize on the parked state", admitted)
	}
}

func TestBreakersPerPeerIsolation(t *testing.T) {
	b := NewBreakers(proc.Backoff{Base: time.Minute, Cap: time.Hour})
	now := time.Unix(1_700_000_000, 0)

	b.Fail("a", now)
	if b.Allow("a", now.Add(30*time.Second)) {
		t.Error("failed peer a allowed inside backoff")
	}
	if !b.Allow("b", now.Add(30*time.Second)) {
		t.Error("healthy peer b suppressed by a's failures")
	}
	if !b.Allow("a", now.Add(2*time.Minute)) {
		t.Error("peer a not allowed after backoff elapsed")
	}
	b.Fail("a", now)
	b.Fail("a", now)
	if b.Allow("a", now.Add(3*time.Minute)) {
		t.Error("peer a allowed inside doubled backoff")
	}
	b.OK("a")
	if !b.Allow("a", now) {
		t.Error("peer a not allowed after OK reset")
	}
}

func TestSweepRevalidatesOwnershipBeforeYield(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 2, State: RowPending})
	snap := map[Key]Row{"k1": {Key: "k1", Seq: 2, State: RowPending}}
	mustApply(t, cfg.Canonical, snap["k1"])
	if err := cfg.Canonical.Truncate(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	registered := false
	e.res.attest = func(Key) (IdleVerdict, error) {
		if !registered {
			registered = true
			if _, err := cfg.Canonical.Bump(context.Background(), "k1", RowPending); err != nil {
				t.Errorf("Bump: %v", err)
			}
		}
		return IdleConfirmed, nil
	}
	if err := Run(context.Background(), *cfg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls := e.res.calls()
	for _, c := range calls {
		if c == "yield k1" {
			t.Fatalf("superseded row was yielded; calls = %v", calls)
		}
	}
	if calls[len(calls)-1] != "restore k1" {
		t.Errorf("calls = %v, want trailing restore", calls)
	}
	row := mustRows(t, cfg.Canonical)["k1"]
	if row.Seq <= 2 || row.State != RowPending {
		t.Errorf("canonical row = %+v, want the post-classification registration intact", row)
	}
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("generation dir not removed after supersession: %v", err)
	}
}

func TestSweepPropagatesSupersededRestoreFailure(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 2, State: RowPending})
	snap := map[Key]Row{"k1": {Key: "k1", Seq: 2, State: RowPending}}
	mustApply(t, cfg.Canonical, snap["k1"])
	if err := cfg.Canonical.Truncate(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	registered := false
	e.res.attest = func(Key) (IdleVerdict, error) {
		if !registered {
			registered = true
			if _, err := cfg.Canonical.Bump(context.Background(), "k1", RowPending); err != nil {
				t.Errorf("Bump: %v", err)
			}
		}
		return IdleConfirmed, nil
	}
	restoreFailed := errors.New("restore down")
	e.res.restore = func(context.Context, Key, Fence) error { return restoreFailed }
	_, err := cfg.sweep(context.Background(), NewBreakers(cfg.perKeyBackoff()), newAutoClock())
	if !errors.Is(err, restoreFailed) {
		t.Fatalf("sweep error = %v, want the wrapped restore failure", err)
	}
	if row := mustRows(t, e.gen.journal())["k1"]; row.State != RowYielded {
		t.Errorf("superseded row not terminalized: %+v", row)
	}
}
