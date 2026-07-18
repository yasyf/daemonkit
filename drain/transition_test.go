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
		ReleaseLock:       func() { e.record("release-lock") },
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
		gen := mustRows(t, e.gen.Journal())
		for _, want := range seedRows() {
			if gen[want.Key] != want {
				t.Errorf("generation row %s = %+v, want %+v", want.Key, gen[want.Key], want)
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
	in.Close()
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
	in.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := in.Settle(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Settle err = %v, want context.Canceled", err)
	}
}
