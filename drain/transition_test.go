package drain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

var errCrash = errors.New("injected crash")

type transitionEnv struct {
	dotdir    string
	canonical Journal
	gen       Generation
	strikes   *StrikeStore
	mu        sync.Mutex
	order     []string
}

func (e *transitionEnv) record(what string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.order = append(e.order, what)
}

func (e *transitionEnv) calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.order...)
}

func newTransitionEnv(t *testing.T, seed ...Row) (*transitionEnv, TransitionConfig) {
	t.Helper()
	e := &transitionEnv{dotdir: t.TempDir()}
	e.canonical = NewJournal(e.dotdir + "/canonical.json")
	e.gen = newGen(t, e.dotdir, "g1")
	e.strikes = &StrikeStore{Path: filepath.Join(e.dotdir, "strikes.json"), Limit: 100}
	if len(seed) > 0 {
		mustApply(t, e.canonical, seed...)
	}
	cfg := TransitionConfig{
		Intake:            &Intake{},
		CloseIntake:       func(context.Context) error { e.record("close-intake"); return nil },
		Canonical:         e.canonical,
		Generation:        e.gen,
		Self:              proc.Identity{PID: 4242, StartTime: "111.222", Comm: "old", Boot: "test-boot"},
		BindDrainListener: func(context.Context) error { e.record("bind"); return nil },
		ReleaseLock:       func() error { e.record("release-lock"); return nil },
		SpawnSuccessor: func(ctx context.Context) error {
			if err := e.strikes.SpawnGate()(ctx); err != nil {
				return err
			}
			e.record("spawn")
			return nil
		},
	}
	return e, cfg
}

func seedRows() []Row {
	return []Row{
		{Key: "k1", Seq: 3, State: RowPending},
		{Key: "k2", Seq: 1, State: RowPending},
	}
}

func requireOwnerCoversRows(t *testing.T, g Generation) {
	t.Helper()
	rows := mustRows(t, g.journal())
	if len(rows) == 0 {
		return
	}
	if _, err := g.ReadOwner(); err != nil {
		t.Errorf("generation holds %d rows without a readable owner: %v", len(rows), err)
	}
}

func waitInflight(t *testing.T, in *Intake) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		in.mu.Lock()
		n := in.inflight
		in.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no admission observed")
}

func TestTransitionRunsNormativeOrder(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	steps := make([]Step, 0, 8)
	cfg.afterStep = func(s Step) error {
		steps = append(steps, s)
		return nil
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	wantSteps := []Step{
		StepDrainFlag, StepCloseIntake, StepSettle, StepSnapshot,
		StepTruncate, StepBindListener, StepReleaseLock, StepSpawn,
	}
	if len(steps) != len(wantSteps) {
		t.Fatalf("steps = %v, want %v", steps, wantSteps)
	}
	for i, want := range wantSteps {
		if steps[i] != want {
			t.Errorf("step[%d] = %v, want %v", i, steps[i], want)
		}
	}
	wantCalls := []string{"close-intake", "bind", "release-lock", "spawn"}
	if got := e.calls(); len(got) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", got, wantCalls)
	} else {
		for i, w := range wantCalls {
			if got[i] != w {
				t.Errorf("call[%d] = %q, want %q", i, got[i], w)
			}
		}
	}
	if !cfg.Intake.Draining() {
		t.Error("intake not draining after transition")
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("canonical not truncated: %v", rows)
	}
	gen := mustRows(t, e.gen.journal())
	for _, want := range seedRows() {
		if gen[want.Key] != want {
			t.Errorf("generation row %s = %+v, want %+v", want.Key, gen[want.Key], want)
		}
	}
	owner, err := e.gen.ReadOwner()
	if err != nil || owner.PID != 4242 {
		t.Errorf("generation owner = %+v, %v; want pid 4242", owner, err)
	}
}

func TestTransitionCrashPointsPreserveSingleOwnership(t *testing.T) {
	tests := []struct {
		name      string
		stopAfter Step
		wantOwner OwnedBy
	}{
		{"a drain flag", StepDrainFlag, OwnedByCanonical},
		{"b close intake", StepCloseIntake, OwnedByCanonical},
		{"c settle", StepSettle, OwnedByCanonical},
		{"d snapshot", StepSnapshot, OwnedByGeneration},
		{"e truncate", StepTruncate, OwnedByGeneration},
		{"f bind listener", StepBindListener, OwnedByGeneration},
		{"g release lock", StepReleaseLock, OwnedByGeneration},
		{"h spawn", StepSpawn, OwnedByGeneration},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, cfg := newTransitionEnv(t, seedRows()...)
			cfg.afterStep = func(s Step) error {
				if s == tt.stopAfter {
					return errCrash
				}
				return nil
			}
			if err := Transition(context.Background(), cfg); !errors.Is(err, errCrash) {
				t.Fatalf("Transition err = %v, want injected crash", err)
			}
			canonical := mustRows(t, e.canonical)
			gen := mustRows(t, e.gen.journal())
			requireOwnerCoversRows(t, e.gen)
			for _, seeded := range seedRows() {
				owner := ResolveOwner(canonical, gen, seeded.Key)
				if owner != tt.wantOwner {
					t.Errorf("key %s owner = %v, want %v", seeded.Key, owner, tt.wantOwner)
				}
				if owner == OwnedByNone {
					t.Errorf("key %s lost at crash point", seeded.Key)
				}
			}
			if tt.stopAfter >= StepTruncate && len(canonical) != 0 {
				t.Errorf("canonical still holds rows post-truncate: %v", canonical)
			}
		})
	}
}

func TestTransitionSnapshotRefusesStaleGenerationJournal(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	if err := os.MkdirAll(e.gen.Dir(), 0o700); err != nil {
		t.Fatal(err)
	}
	mustApply(t, e.gen.journal(), Row{Key: "k1", Seq: 9, State: RowYielded})
	err := Transition(context.Background(), cfg)
	if err == nil {
		t.Fatal("Transition succeeded over a stale generation journal, want error")
	}
	if !strings.Contains(err.Error(), "stale journal") {
		t.Errorf("Transition err = %v, want a stale-journal snapshot error", err)
	}
	if _, err := e.gen.ReadOwner(); err == nil {
		t.Error("ownerless stale journal was claimed by the failed transition")
	}
	rows := mustRows(t, e.canonical)
	for _, want := range seedRows() {
		if rows[want.Key] != want {
			t.Errorf("canonical row %s = %+v, want %+v intact (no truncate after a failed snapshot)", want.Key, rows[want.Key], want)
		}
	}
}

func TestTransitionSnapshotRefusesDisjointForeignGenerationJournal(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	foreign := proc.Identity{PID: 999, StartTime: "333.444", Comm: "foreign", Boot: "test-boot"}
	foreignGen := seedOwner(t, e.gen, foreign)
	mustApply(t, foreignGen.journal(), Row{Key: "foreign", Seq: 1, State: RowPending})

	err := Transition(context.Background(), cfg)
	if err == nil {
		t.Error("Transition succeeded over a disjoint foreign generation row, want error")
	}
	owner, ownerErr := e.gen.ReadOwner()
	if ownerErr != nil {
		t.Fatalf("ReadOwner: %v", ownerErr)
	}
	if owner != foreign {
		t.Errorf("generation owner = %+v, want foreign owner %+v unchanged", owner, foreign)
	}
	rows := mustRows(t, e.canonical)
	for _, want := range seedRows() {
		if rows[want.Key] != want {
			t.Errorf("canonical row %s = %+v, want %+v intact", want.Key, rows[want.Key], want)
		}
	}
}

func TestSnapshotFailureBeforeRowsIsRetryable(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	cfg.midSnapshot = func() error {
		if owner, err := e.gen.ReadOwner(); err != nil || owner != cfg.Self {
			t.Errorf("generation owner = %+v, %v; want %+v before rows", owner, err, cfg.Self)
		}
		state, err := e.gen.journal().file.Read()
		if err != nil {
			t.Fatalf("read generation journal: %v", err)
		}
		for key := range state.Rows {
			t.Errorf("generation row %s became durable before the claim completed", key)
		}
		return errCrash
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, errCrash) {
		t.Fatalf("Transition err = %v, want injected crash", err)
	}
	if owner, err := e.gen.ReadOwner(); err != nil || owner != cfg.Self {
		t.Errorf("failed snapshot owner = %+v, %v; want durable %+v", owner, err, cfg.Self)
	}
	if rows := mustRows(t, e.gen.journal()); len(rows) != 0 {
		t.Errorf("failed snapshot wrote generation rows: %v", rows)
	}
	rows := mustRows(t, e.canonical)
	if len(rows) != len(seedRows()) {
		t.Errorf("canonical rows = %v, want the seeds intact", rows)
	}
	cfg.midSnapshot = nil
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry: %v", err)
	}
}

func TestTransitionJoinsInFlightAdoption(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	peerOwner := proc.Identity{PID: 999, StartTime: "1.2", Comm: "peer", Boot: "test-boot"}
	peer := seedOwner(t, newGen(t, e.dotdir, "g0"), peerOwner)
	mustApply(t, peer.journal(), Row{Key: "k3", Seq: 5, State: RowPending})
	scanCfg := ScanConfig{
		Dotdir:    e.dotdir,
		Canonical: e.canonical,
		Intake:    cfg.Intake,
		Log:       discardLog(),
		prober:    &fakeProber{results: map[int]proberResult{peerOwner.PID: {err: proc.ErrNoProcess}}},
	}

	ctx := context.Background()
	held, err := (proc.FileLockSpec{
		Path:     e.canonical.lockPath(),
		Mode:     proc.FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	adoptErr := make(chan error, 1)
	go func() { adoptErr <- AdoptDead(ctx, scanCfg, peer, peerOwner) }()
	waitInflight(t, cfg.Intake)
	transitionErr := make(chan error, 1)
	go func() { transitionErr <- Transition(ctx, cfg) }()
	held.Close()

	if err := <-adoptErr; err != nil {
		t.Fatalf("AdoptDead: %v", err)
	}
	if err := <-transitionErr; err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("canonical not truncated: %v", rows)
	}
	gen := mustRows(t, e.gen.journal())
	want := Row{Key: "k3", Seq: 6, State: RowPending}
	if gen["k3"] != want {
		t.Errorf("adopted row = %+v, want %+v snapshotted into the generation", gen["k3"], want)
	}
	if _, err := os.Stat(peer.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("adopted peer dir not removed: %v", err)
	}
}

func TestTransitionPausedHandlerCannotRegisterPostSnapshot(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	handlerDone, err := cfg.Intake.Admit()
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	steps := make(chan Step)
	cfg.afterStep = func(s Step) error {
		steps <- s
		return nil
	}
	errCh := make(chan error, 1)
	go func() { errCh <- Transition(context.Background(), cfg) }()

	for _, want := range []Step{StepDrainFlag, StepCloseIntake} {
		if got := <-steps; got != want {
			t.Fatalf("step = %v, want %v", got, want)
		}
	}
	if _, err := os.Stat(e.gen.journal().Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot exists while a handler is paused: %v", err)
	}
	handlerDone()
	for _, want := range []Step{StepSettle, StepSnapshot, StepTruncate, StepBindListener, StepReleaseLock, StepSpawn} {
		if got := <-steps; got != want {
			t.Fatalf("step = %v, want %v", got, want)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if _, err := cfg.Intake.Register(context.Background(), e.canonical, "rogue"); !errors.Is(err, ErrDraining) {
		t.Fatalf("Register err = %v, want ErrDraining", err)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("rogue registration reached the canonical journal: %v", rows)
	}
}

func TestIntakeAdmitAndRegister(t *testing.T) {
	in := &Intake{}
	j := NewJournal(t.TempDir() + "/canonical.json")
	ctx := context.Background()

	row, err := in.Register(ctx, j, "k1")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if row.Seq != 1 || row.State != RowPending {
		t.Errorf("registered row = %+v, want seq 1 pending", row)
	}
	if err := in.BeginDrain(); err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}
	if _, err := in.Admit(); !errors.Is(err, ErrDraining) {
		t.Errorf("Admit after Close err = %v, want ErrDraining", err)
	}
	if _, err := in.Register(ctx, j, "k2"); !errors.Is(err, ErrDraining) {
		t.Errorf("Register after Close err = %v, want ErrDraining", err)
	}
	if err := in.Settle(ctx); err != nil {
		t.Errorf("Settle with no inflight: %v", err)
	}
}

func TestIntakeSettleHonorsContext(t *testing.T) {
	in := &Intake{}
	done, err := in.Admit()
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if err := in.BeginDrain(); err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := in.Settle(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Settle err = %v, want context.Canceled", err)
	}
}

func generationHasRows(t *testing.T, g Generation) bool {
	t.Helper()
	rows := mustRows(t, g.journal())
	if len(rows) == 0 {
		return false
	}
	if _, err := g.ReadOwner(); err != nil {
		t.Errorf("generation %s holds %d rows without a readable owner: %v", g.Name(), len(rows), err)
	}
	return true
}

func TestTransitionConcurrentDifferentGenerationsSingleOwner(t *testing.T) {
	e, cfg1 := newTransitionEnv(t, seedRows()...)
	g2 := newGen(t, e.dotdir, "g2")
	cfg2 := TransitionConfig{
		Intake:            cfg1.Intake,
		CloseIntake:       func(context.Context) error { return nil },
		Canonical:         e.canonical,
		Generation:        g2,
		Self:              proc.Identity{PID: 7777, StartTime: "222.333", Comm: "other", Boot: "test-boot"},
		BindDrainListener: func(context.Context) error { return nil },
		ReleaseLock:       func() error { return nil },
		SpawnSuccessor:    func(context.Context) error { return nil },
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = Transition(context.Background(), cfg1) }()
	go func() { defer wg.Done(); errs[1] = Transition(context.Background(), cfg2) }()
	wg.Wait()

	wins, refusals := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrDrainInProgress):
			refusals++
		default:
			t.Fatalf("unexpected Transition error: %v", err)
		}
	}
	if wins != 1 || refusals != 1 {
		t.Fatalf("wins=%d refusals=%d, want exactly one winner and one ErrDrainInProgress", wins, refusals)
	}
	if g1Owned, g2Owned := generationHasRows(t, e.gen), generationHasRows(t, g2); g1Owned == g2Owned {
		t.Fatalf("ownership split g1=%v g2=%v, want exactly one generation to own the rows", g1Owned, g2Owned)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("winner did not truncate canonical: %v", rows)
	}
}

func TestSnapshotHoldsGenerationLockAcrossClaim(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	landed := make(chan struct{})
	cfg.midSnapshot = func() error {
		go func() {
			_, _ = e.gen.journal().apply(context.Background(), Row{Key: "foreign", Seq: 1, State: RowPending})
			close(landed)
		}()
		select {
		case <-landed:
			t.Error("a concurrent apply landed while the snapshot held the journal flock")
		case <-time.After(100 * time.Millisecond):
		}
		return nil
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	<-landed
}

func TestTransitionFailedLockReleaseDoesNotSpawn(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	relErr := errors.New("flock release failed")
	cfg.ReleaseLock = func() error { e.record("release-lock"); return relErr }
	if err := Transition(context.Background(), cfg); !errors.Is(err, relErr) {
		t.Fatalf("Transition err = %v, want the canonical lock release error", err)
	}
	for _, c := range e.calls() {
		if c == "spawn" {
			t.Fatalf("successor spawned after a failed canonical lock release: %v", e.calls())
		}
	}
}

func TestTransitionReleaseFailureRetriesBeforeSpawn(t *testing.T) {
	_, cfg := newTransitionEnv(t, seedRows()...)
	releaseErr := errors.New("flock release failed")
	releases := 0
	spawns := 0
	cfg.ReleaseLock = func() error {
		releases++
		if releases == 1 {
			return releaseErr
		}
		return nil
	}
	cfg.SpawnSuccessor = func(context.Context) error { spawns++; return nil }
	if err := Transition(context.Background(), cfg); !errors.Is(err, releaseErr) {
		t.Fatalf("first Transition err = %v, want release failure", err)
	}
	if spawns != 0 {
		t.Fatalf("spawns after release failure = %d, want 0", spawns)
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry: %v", err)
	}
	if releases != 2 || spawns != 1 {
		t.Errorf("releases=%d spawns=%d, want 2 release attempts and 1 spawn", releases, spawns)
	}
}

func TestTransitionFailureReleasesAttemptWithoutReopeningIntake(t *testing.T) {
	_, cfg := newTransitionEnv(t, seedRows()...)
	bindErr := errors.New("bind failed")
	cfg.BindDrainListener = func(context.Context) error { return bindErr }
	if err := Transition(context.Background(), cfg); !errors.Is(err, bindErr) {
		t.Fatalf("Transition err = %v, want bind failure", err)
	}
	if _, err := cfg.Intake.Admit(); !errors.Is(err, ErrDraining) {
		t.Fatalf("Admit after failed transition err = %v, want ErrDraining", err)
	}
	if err := cfg.Intake.BeginDrain(); err != nil {
		t.Fatalf("BeginDrain retry: %v", err)
	}
}

func TestTransitionPostTruncateFailureRetriesSameGeneration(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	bindErr := errors.New("bind failed")
	attempts := 0
	cfg.BindDrainListener = func(context.Context) error {
		attempts++
		if attempts == 1 {
			return bindErr
		}
		return nil
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, bindErr) {
		t.Fatalf("first Transition err = %v, want bind failure", err)
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry: %v", err)
	}
	if attempts != 2 {
		t.Errorf("bind attempts = %d, want 2", attempts)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("canonical rows after retry = %v, want empty", rows)
	}
	if rows := mustRows(t, e.gen.journal()); len(rows) != len(seedRows()) {
		t.Errorf("generation rows after retry = %v, want the original snapshot", rows)
	}
}

func TestTransitionSpawnFailureResumesAfterReleasedLock(t *testing.T) {
	_, cfg := newTransitionEnv(t, seedRows()...)
	spawnErr := errors.New("spawn failed")
	releases := 0
	spawns := 0
	cfg.ReleaseLock = func() error {
		releases++
		if releases > 1 {
			return errors.New("lock released twice")
		}
		return nil
	}
	cfg.SpawnSuccessor = func(context.Context) error {
		spawns++
		if spawns == 1 {
			return spawnErr
		}
		return nil
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, spawnErr) {
		t.Fatalf("first Transition err = %v, want spawn failure", err)
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry: %v", err)
	}
	if releases != 1 || spawns != 2 {
		t.Errorf("releases=%d spawns=%d, want 1 release and 2 spawn attempts", releases, spawns)
	}
}

func TestConflictReopenedIntakeRowSurvivesTruncate(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	bindErr := errors.New("bind failed")
	cfg.BindDrainListener = func(context.Context) error { return bindErr }
	if err := Transition(context.Background(), cfg); !errors.Is(err, bindErr) {
		t.Fatalf("first Transition err = %v, want bind failure", err)
	}
	cfg2 := cfg
	cfg2.Generation = newGen(t, e.dotdir, "g2")
	if err := Transition(context.Background(), cfg2); !errors.Is(err, ErrDrainInProgress) {
		t.Fatalf("conflicting Transition err = %v, want ErrDrainInProgress", err)
	}
	late, err := cfg.Intake.Register(context.Background(), e.canonical, "k3")
	if err != nil {
		t.Fatalf("Register on the reopened intake: %v", err)
	}
	cfg.BindDrainListener = func(context.Context) error { return nil }
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry on the pinned generation: %v", err)
	}
	rows := mustRows(t, e.canonical)
	if rows["k3"] != late {
		t.Errorf("post-snapshot row = %+v, want %+v surviving the scoped truncate", rows["k3"], late)
	}
	if len(rows) != 1 {
		t.Errorf("canonical rows = %v, want only the post-snapshot row", rows)
	}
	gen := mustRows(t, e.gen.journal())
	if _, ok := gen["k3"]; ok {
		t.Error("post-snapshot row leaked into the generation snapshot")
	}
	for _, want := range seedRows() {
		if gen[want.Key] != want {
			t.Errorf("generation row %s = %+v, want %+v", want.Key, gen[want.Key], want)
		}
	}
}

func TestTransitionSpawnFailuresStrikeAndPark(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	strikes := &StrikeStore{Path: e.dotdir + "/strikes.json", Limit: 2, Ladder: []time.Duration{time.Hour}}
	spawnErr := errors.New("spawn failed")
	launches := 0
	cfg.SpawnSuccessor = func(ctx context.Context) error {
		if err := strikes.SpawnGate()(ctx); err != nil {
			return err
		}
		launches++
		return spawnErr
	}
	for i := range 2 {
		if err := Transition(context.Background(), cfg); !errors.Is(err, spawnErr) {
			t.Fatalf("Transition %d err = %v, want spawn failure", i, err)
		}
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, ErrSpawnParked) {
		t.Fatalf("parked Transition err = %v, want ErrSpawnParked", err)
	}
	if launches != 2 {
		t.Errorf("launches = %d, want 2: the parked attempt must not launch", launches)
	}
}

func TestTransitionRetryResettlesReopenedAdmissions(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	cfg.midSnapshot = func() error { return errCrash }
	if err := Transition(context.Background(), cfg); !errors.Is(err, errCrash) {
		t.Fatalf("first Transition err = %v, want injected crash", err)
	}
	cfg2 := cfg
	cfg2.Generation = newGen(t, e.dotdir, "g2")
	if err := Transition(context.Background(), cfg2); !errors.Is(err, ErrDrainInProgress) {
		t.Fatalf("conflicting Transition err = %v, want ErrDrainInProgress", err)
	}
	done, err := cfg.Intake.Admit()
	if err != nil {
		t.Fatalf("Admit on the reopened intake: %v", err)
	}
	cfg.midSnapshot = nil
	retryErr := make(chan error, 1)
	go func() { retryErr <- Transition(context.Background(), cfg) }()
	select {
	case err := <-retryErr:
		t.Fatalf("retry completed without re-settling the admitted unit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	late, err := e.canonical.Bump(context.Background(), "k3", RowPending)
	if err != nil {
		t.Fatalf("Bump: %v", err)
	}
	done()
	if err := <-retryErr; err != nil {
		t.Fatalf("Transition retry: %v", err)
	}
	if row := mustRows(t, e.gen.journal())["k3"]; row != late {
		t.Errorf("re-settled row = %+v, want %+v in the generation snapshot", row, late)
	}
}

func TestTransitionParkNeverBlocksLiveSuccessorCompletion(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	strikes := &StrikeStore{Path: e.dotdir + "/strikes.json", Limit: 1, Ladder: []time.Duration{time.Hour}}
	spawnErr := errors.New("spawn timed out")
	var alive atomic.Bool
	launches := 0
	spawnCalls := 0
	cfg.SpawnSuccessor = func(ctx context.Context) error {
		spawnCalls++
		if alive.Load() {
			return nil
		}
		if err := strikes.SpawnGate()(ctx); err != nil {
			return err
		}
		launches++
		return spawnErr
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, spawnErr) {
		t.Fatalf("first Transition err = %v, want spawn failure", err)
	}
	alive.Store(true)
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition completion behind a live successor: %v", err)
	}
	if launches != 1 || spawnCalls != 2 {
		t.Errorf("launches = %d, spawnCalls = %d, want 1 launch and 2 idempotent spawn calls", launches, spawnCalls)
	}
}

func TestClaimOwnerRecommitsOwnerDurably(t *testing.T) {
	e, cfg := newTransitionEnv(t)
	ctx := context.Background()
	if _, err := e.gen.claimOwner(ctx, cfg.Self); err != nil {
		t.Fatalf("claimOwner: %v", err)
	}
	before, err := os.Stat(e.gen.ownerPath())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.gen.claimOwner(ctx, cfg.Self); err != nil {
		t.Fatalf("claimOwner retry: %v", err)
	}
	after, err := os.Stat(e.gen.ownerPath())
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Error("retrying claimOwner left the prior owner file untouched, want a durable rewrite")
	}
	owner, err := e.gen.ReadOwner()
	if err != nil || owner != cfg.Self {
		t.Errorf("owner after recommit = %+v, %v; want %+v", owner, err, cfg.Self)
	}
}

func TestTransitionRejectsUnprovenSameOwnerGenerationRows(t *testing.T) {
	e, cfg := newTransitionEnv(t)
	seeded := seedOwner(t, e.gen, cfg.Self)
	mustApply(t, seeded.journal(), Row{Key: "foreign", Seq: 9, State: RowPending})
	if err := Transition(context.Background(), cfg); !errors.Is(err, ErrStaleJournal) {
		t.Fatalf("Transition err = %v, want ErrStaleJournal", err)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("stale generation rows reached canonical: %v", rows)
	}
}

func TestTransitionRestartFencesPreviousGeneration(t *testing.T) {
	e, cfg1 := newTransitionEnv(t, seedRows()...)
	cfg1.afterStep = func(s Step) error {
		if s == StepSnapshot {
			return errCrash
		}
		return nil
	}
	if err := Transition(context.Background(), cfg1); !errors.Is(err, errCrash) {
		t.Fatalf("first Transition err = %v, want injected crash", err)
	}

	g2 := newGen(t, e.dotdir, "g2")
	self2 := proc.Identity{PID: 7777, StartTime: "222.333", Comm: "new", Boot: "test-boot"}
	cfg2 := TransitionConfig{
		Intake:            &Intake{},
		CloseIntake:       func(context.Context) error { return nil },
		Canonical:         e.canonical,
		Generation:        g2,
		Self:              self2,
		BindDrainListener: func(context.Context) error { return nil },
		ReleaseLock:       func() error { return nil },
		SpawnSuccessor:    func(context.Context) error { return nil },
	}
	if err := Transition(context.Background(), cfg2); !errors.Is(err, ErrDrainInProgress) {
		t.Fatalf("second Transition err = %v, want ErrDrainInProgress", err)
	}
	if cfg2.Intake.Draining() {
		t.Fatal("conflicting durable claim closed the restarted intake")
	}
	if rows := mustRows(t, g2.journal()); len(rows) != 0 {
		t.Fatalf("conflicting generation received rows: %v", rows)
	}
	if _, err := cfg2.Intake.Register(context.Background(), e.canonical, "k3"); err != nil {
		t.Fatalf("Register on the reopened restarted intake: %v", err)
	}

	prb := &fakeProber{results: map[int]proberResult{
		cfg1.Self.PID: {err: proc.ErrNoProcess},
		self2.PID:     {id: self2},
	}}
	if err := ScanPeers(context.Background(), ScanConfig{
		Dotdir:    e.dotdir,
		Canonical: e.canonical,
		Intake:    cfg2.Intake,
		Log:       discardLog(),
		prober:    prb,
	}); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if _, err := os.Stat(e.gen.Dir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead first generation retained: %v", err)
	}
	if err := Transition(context.Background(), cfg2); err != nil {
		t.Fatalf("second Transition after fencing: %v", err)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("canonical rows after second transition = %v, want empty", rows)
	}
	rows := mustRows(t, g2.journal())
	for _, original := range seedRows() {
		want := original
		want.Seq = nextSeq(want.Seq)
		if rows[want.Key] != want {
			t.Errorf("second generation row %s = %+v, want %+v", want.Key, rows[want.Key], want)
		}
	}
	if row, ok := rows["k3"]; !ok || row.State != RowPending {
		t.Errorf("post-restart registered row = %+v, %v; want pending in the second generation", row, ok)
	}
}

func TestTransitionCompletedReleaseRetryIsIdempotent(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	first := e.calls()
	cfg.Intake.abortTransition(false)
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition retry after completed release: %v", err)
	}
	if got := e.calls(); len(got) != len(first) {
		t.Errorf("retry re-ran callbacks: %v, want %v", got, first)
	}
	if _, ok, err := e.canonical.activeTransition(context.Background()); err != nil || ok {
		t.Errorf("active transition after idempotent retry = %v, %v; want released", ok, err)
	}
}

func TestTransitionDrainFlagPrecedesCloseIntake(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	cfg.CloseIntake = func(context.Context) error {
		if !cfg.Intake.Draining() {
			t.Error("CloseIntake ran before the drain flag was set")
		}
		e.record("close-intake")
		return nil
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
}

func TestTransitionBindFollowsTruncate(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	cfg.BindDrainListener = func(ctx context.Context) error {
		rows, err := e.canonical.Rows(ctx)
		if err != nil {
			t.Fatalf("read canonical: %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("bind ran before canonical truncate: %v", rows)
		}
		e.record("bind")
		return nil
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
}

func TestTransitionTruncateFollowsOwnerClaim(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	ctx := context.Background()
	canonicalRows := func(at string) map[Key]Row {
		rows, err := e.canonical.Rows(ctx)
		if err != nil {
			t.Fatalf("read canonical at %s: %v", at, err)
		}
		return rows
	}
	cfg.afterStep = func(s Step) error {
		switch s {
		case StepSnapshot:
			if _, err := e.gen.ReadOwner(); err != nil {
				t.Errorf("owner not claimed at snapshot completion: %v", err)
			}
			if len(canonicalRows("snapshot")) == 0 {
				t.Error("canonical truncated before the generation owned the rows")
			}
		case StepTruncate:
			if _, err := e.gen.ReadOwner(); err != nil {
				t.Errorf("owner missing at truncate: %v", err)
			}
			if rows := canonicalRows("truncate"); len(rows) != 0 {
				t.Errorf("canonical not truncated at StepTruncate: %v", rows)
			}
		}
		return nil
	}
	if err := Transition(ctx, cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
}

func TestStaleCompleteMarkerDoesNotShortCircuitReusedName(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	if err := os.MkdirAll(e.gen.Dir(), 0o755); err != nil {
		t.Fatal(err)
	}
	file := journalStateFile(filepath.Join(e.gen.Dir(), "journal.json"))
	if err := file.UpdateUnlocked(func(state *journalState) error {
		state.Complete = "deadbeefdeadbeefdeadbeefdeadbeef"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	spawned := false
	for _, c := range e.calls() {
		if c == "spawn" {
			spawned = true
		}
	}
	if !spawned {
		t.Fatalf("stale marker short-circuited the transition; calls = %v", e.calls())
	}
}
