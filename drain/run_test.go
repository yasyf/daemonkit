package drain

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func discardLog() *slog.Logger { return slog.New(slog.DiscardHandler) }

type runEnv struct {
	dotdir string
	gen    Generation
	res    *fakeResources
	spawns atomic.Int32
}

func newRunEnv(t *testing.T, rows ...Row) (*runEnv, *RunConfig) {
	t.Helper()
	e := &runEnv{dotdir: t.TempDir()}
	e.gen = NewGeneration(e.dotdir, "g1")
	keys := make([]Key, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, r.Key)
	}
	if len(rows) > 0 {
		mustApply(t, e.gen.Journal(), rows...)
	}
	e.res = &fakeResources{keys: keys}
	cfg := &RunConfig{
		Generation:     e.gen,
		Resources:      e.res,
		CanonicalAlive: func(context.Context) Liveness { return Alive },
		Ready:          func(context.Context) bool { return true },
		Spawn:          func(context.Context) error { e.spawns.Add(1); return nil },
		Strikes:        &StrikeStore{Path: filepath.Join(e.dotdir, "strikes.json")},
		Backoff:        proc.Backoff{Base: time.Nanosecond, Cap: time.Nanosecond},
		Log:            discardLog(),
		clock:          newAutoClock(),
	}
	return e, cfg
}

// cancelAfterTicks wires CanonicalAlive to cancel ctx on the nth tick.
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
	row := mustRows(t, e.gen.Journal())["k1"]
	if row.State != RowPending || row.Seq != 1 {
		t.Errorf("row advanced despite busy attest: %+v", row)
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
	row := mustRows(t, e.gen.Journal())["k1"]
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
	indexOf(t, log, "seize k1", r1) // the retry seizes only after Restore ran
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("generation dir not removed after retry succeeded: %v", err)
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
	row := mustRows(t, e.gen.Journal())["k1"]
	if row.State != RowPending {
		t.Errorf("enumeration error treated as zero candidates: %+v", row)
	}
}

func TestRunProvenAbsentKeyIsTerminal(t *testing.T) {
	e, cfg := newRunEnv(t, Row{Key: "k1", Seq: 1, State: RowPending})
	e.res.keys = nil // a COMPLETE successful scan with zero live resources
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
	cfg.Strikes.Ladder = []time.Duration{time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancelAfterTicks(cfg, 6, cancel, Dead)
	if err := Run(ctx, *cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if got := e.spawns.Load(); got != 3 {
		t.Errorf("spawns = %d, want exactly 3 before the breaker parks", got)
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
	st, err := cfg.Strikes.load()
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
		parked, _, err := s1.Strike(ctx, t0.Add(time.Duration(i)*time.Second))
		if err != nil || parked {
			t.Fatalf("strike %d: parked=%v err=%v", i, parked, err)
		}
	}
	parked, until, err := s1.Strike(ctx, t0.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !parked || !until.Equal(t0.Add(2*time.Second).Add(time.Minute)) {
		t.Fatalf("third strike: parked=%v until=%v, want park to +1m", parked, until)
	}

	// A fresh store on the same path sees the park and the ladder level.
	s2 := StrikeStore{Path: path, Limit: 3, Window: 10 * time.Minute, Ladder: []time.Duration{time.Minute, 5 * time.Minute}}
	parked, until2, err := s2.Parked(ctx, t0.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !parked || !until2.Equal(until) {
		t.Errorf("restart lost the park: parked=%v until=%v want %v", parked, until2, until)
	}
	parked, until3, err := s2.Strike(ctx, t0.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !parked || !until3.Equal(t0.Add(2*time.Minute).Add(5*time.Minute)) {
		t.Errorf("ladder level lost across restart: parked=%v until=%v, want +5m step", parked, until3)
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
