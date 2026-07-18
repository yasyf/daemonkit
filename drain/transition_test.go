package drain

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

var errCrash = errors.New("injected crash")

type transitionEnv struct {
	dotdir    string
	canonical Journal
	gen       Generation
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
	e.gen = NewGeneration(e.dotdir, "g1")
	if len(seed) > 0 {
		mustApply(t, e.canonical, seed...)
	}
	cfg := TransitionConfig{
		Intake:            &Intake{},
		CloseIntake:       func(context.Context) error { e.record("close-intake"); return nil },
		Canonical:         e.canonical,
		Generation:        e.gen,
		Self:              proc.Identity{PID: 4242, StartTime: "111.222", Comm: "old"},
		BindDrainListener: func(context.Context) error { e.record("bind"); return nil },
		ReleaseLock:       func() error { e.record("release-lock"); return nil },
		SpawnSuccessor:    func(context.Context) error { e.record("spawn"); return nil },
	}
	return e, cfg
}

func seedRows() []Row {
	return []Row{
		{Key: "k1", Seq: 3, State: RowPending},
		{Key: "k2", Seq: 1, State: RowPending},
	}
}

// requireOwnerCoversRows pins the snapshot ordering invariant: generation rows
// exist only under a readable owner record.
func requireOwnerCoversRows(t *testing.T, g Generation) {
	t.Helper()
	rows := mustRows(t, g.Journal())
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

func waitDraining(t *testing.T, in *Intake) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if in.Draining() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("intake never closed")
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
	gen := mustRows(t, e.gen.Journal())
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
			gen := mustRows(t, e.gen.Journal())
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
	// A reused generation name left a stale higher-seq row: snapshot must fail
	// loud before Truncate, never silently CAS-suppress the pending claim.
	mustApply(t, e.gen.Journal(), Row{Key: "k1", Seq: 9, State: RowYielded})
	err := Transition(context.Background(), cfg)
	if err == nil {
		t.Fatal("Transition succeeded over a stale generation journal, want error")
	}
	if !strings.Contains(err.Error(), "stale journal") {
		t.Errorf("Transition err = %v, want a stale-journal snapshot error", err)
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
	foreign := proc.Identity{PID: 999, StartTime: "333.444", Comm: "foreign"}
	if err := e.gen.WriteOwner(foreign); err != nil {
		t.Fatalf("WriteOwner: %v", err)
	}
	mustApply(t, e.gen.Journal(), Row{Key: "foreign", Seq: 1, State: RowPending})

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

func TestSnapshotCrashBetweenRowsAndOwner(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	cfg.midSnapshot = func() error {
		if _, err := e.gen.ReadOwner(); err == nil {
			t.Error("owner written before the row snapshot completed")
		}
		// The snapshot holds the generation journal flock here; read the file
		// directly rather than through Rows, which would re-enter the
		// non-reentrant flock and deadlock.
		state, err := readState(e.gen.Journal().Path())
		if err != nil {
			t.Fatalf("read generation journal: %v", err)
		}
		for _, want := range seedRows() {
			row, err := decodeRow(state[string(want.Key)])
			if err != nil {
				t.Fatalf("decode row %s: %v", want.Key, err)
			}
			if row != want {
				t.Errorf("generation row %s = %+v, want %+v", want.Key, row, want)
			}
		}
		return errCrash
	}
	if err := Transition(context.Background(), cfg); !errors.Is(err, errCrash) {
		t.Fatalf("Transition err = %v, want injected crash", err)
	}
	if _, err := e.gen.ReadOwner(); err == nil {
		t.Error("failed snapshot wrote the generation owner")
	}
	rows := mustRows(t, e.canonical)
	if len(rows) != len(seedRows()) {
		t.Errorf("canonical rows = %v, want the seeds intact", rows)
	}
}

func TestTransitionSettleJoinsInFlightAdoption(t *testing.T) {
	e, cfg := newTransitionEnv(t, seedRows()...)
	peer := NewGeneration(e.dotdir, "g0")
	if err := peer.WriteOwner(proc.Identity{PID: 999, StartTime: "1.2", Comm: "peer"}); err != nil {
		t.Fatal(err)
	}
	mustApply(t, peer.Journal(), Row{Key: "k3", Seq: 5, State: RowPending})

	ctx := context.Background()
	held, err := proc.Flock(ctx, e.canonical.lockPath())
	if err != nil {
		t.Fatal(err)
	}
	adoptErr := make(chan error, 1)
	go func() { adoptErr <- AdoptDead(ctx, cfg.Intake, e.canonical, peer) }()
	// The adoption is admitted and blocked on the canonical flock when the
	// intake closes: Settle must join it before the snapshot.
	waitInflight(t, cfg.Intake)
	transitionErr := make(chan error, 1)
	go func() { transitionErr <- Transition(ctx, cfg) }()
	waitDraining(t, cfg.Intake)
	held.Release()

	if err := <-adoptErr; err != nil {
		t.Fatalf("AdoptDead: %v", err)
	}
	if err := <-transitionErr; err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if rows := mustRows(t, e.canonical); len(rows) != 0 {
		t.Errorf("canonical not truncated: %v", rows)
	}
	gen := mustRows(t, e.gen.Journal())
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
	// The paused handler still holds admission: the barrier blocks pre-snapshot.
	if _, err := os.Stat(e.gen.Journal().Path()); !errors.Is(err, os.ErrNotExist) {
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
	// The handler wakes post-snapshot and tries to register: it must be refused.
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

// generationHasRows reports whether g's journal holds rows, and fails the test
// if it does without a readable owner (a snapshot must claim ownership atomically).
func generationHasRows(t *testing.T, g Generation) bool {
	t.Helper()
	rows := mustRows(t, g.Journal())
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
	g2 := NewGeneration(e.dotdir, "g2")
	cfg2 := TransitionConfig{
		Intake:            cfg1.Intake, // shared: BeginDrain's CAS serializes the two
		CloseIntake:       func(context.Context) error { return nil },
		Canonical:         e.canonical,
		Generation:        g2,
		Self:              proc.Identity{PID: 7777, StartTime: "222.333", Comm: "other"},
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
		// The generation journal flock is held across the empty-check, the apply,
		// and this claim: a concurrent journal writer must block until it releases.
		go func() {
			_, _ = e.gen.Journal().Apply(context.Background(), Row{Key: "foreign", Seq: 1, State: RowPending})
			close(landed)
		}()
		select {
		case <-landed:
			t.Error("a concurrent Apply landed while the snapshot held the journal flock")
		case <-time.After(100 * time.Millisecond):
		}
		return nil
	}
	if err := Transition(context.Background(), cfg); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	<-landed // the foreign Apply lands only once the snapshot released the flock
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
