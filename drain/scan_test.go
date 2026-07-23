package drain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

const (
	deadPID   = 4242
	deadStart = "111.222"
)

func deadOwner() proc.Identity {
	return proc.Identity{PID: deadPID, StartTime: deadStart, Comm: "old-daemon", Boot: "test-boot"}
}

func newScanEnv(t *testing.T, prb prober, rows ...Row) (ScanConfig, Generation) {
	t.Helper()
	dotdir := t.TempDir()
	cfg := ScanConfig{
		Dotdir:    dotdir,
		Canonical: NewJournal(filepath.Join(dotdir, "canonical.json")),
		Intake:    &Intake{},
		Log:       discardLog(),
		prober:    prb,
	}
	g := seedOwner(t, newGen(t, dotdir, "g1"), deadOwner())
	if len(rows) > 0 {
		mustApply(t, g.journal(), rows...)
	}
	return cfg, g
}

func dirExists(t *testing.T, dir string) bool {
	t.Helper()
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func TestScanColdStartAdoptsOrphan(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(
		t, prb,
		Row{Key: "k1", Seq: 3, State: RowPending},
		Row{Key: "k2", Seq: 2, State: RowYielded},
	)
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	rows := mustRows(t, cfg.Canonical)
	want := Row{Key: "k1", Seq: 4, State: RowPending}
	if rows["k1"] != want {
		t.Errorf("adopted row = %+v, want %+v", rows["k1"], want)
	}
	if _, ok := rows["k2"]; ok {
		t.Error("terminal yielded row was resurrected")
	}
	if dirExists(t, g.Dir()) {
		t.Error("dead generation dir not removed after adoption")
	}
}

func TestScanUnreadablePeerStaysUnadopted(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := os.WriteFile(filepath.Join(g.Dir(), "owner.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("unreadable peer adopted: %v", rows)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("unreadable peer's generation removed")
	}
}

func TestScanProbeFailureIsUndetermined(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: errors.New("stat timed out")}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("undetermined peer adopted: %v", rows)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("undetermined peer's generation removed")
	}
}

func TestScanAlivePeerUntouched(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {id: deadOwner()}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("live peer adopted: %v", rows)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("live peer's generation removed")
	}
}

func TestScanReusedPIDIsDead(t *testing.T) {
	reused := proc.Identity{PID: deadPID, StartTime: "999.000", Comm: "newcomer", Boot: "test-boot"}
	prb := &fakeProber{results: map[int]proberResult{deadPID: {id: reused}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	rows := mustRows(t, cfg.Canonical)
	if rows["k1"].Seq != 2 || rows["k1"].State != RowPending {
		t.Errorf("reused-pid peer not adopted: %v", rows)
	}
	if dirExists(t, g.Dir()) {
		t.Error("dead generation dir not removed")
	}
}

func TestScanEnumerationErrorAdoptsNothing(t *testing.T) {
	dotdir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dotdir, "drain"), []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := ScanConfig{
		Dotdir:    dotdir,
		Canonical: NewJournal(filepath.Join(dotdir, "canonical.json")),
		Intake:    &Intake{},
		Log:       discardLog(),
		prober:    &fakeProber{},
	}
	if err := ScanPeers(context.Background(), cfg); err == nil {
		t.Fatal("ScanPeers succeeded on unreadable layout, want error")
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("enumeration failure adopted rows: %v", rows)
	}
}

func TestScanAdoptStaleReplayNoOps(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	mustApply(t, cfg.Canonical, Row{Key: "k1", Seq: 9, State: RowYielded})
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	row := mustRows(t, cfg.Canonical)["k1"]
	if row.Seq != 9 || row.State != RowYielded {
		t.Errorf("stale replay overwrote canonical: %+v", row)
	}
	if dirExists(t, g.Dir()) {
		t.Error("generation dir not removed after stale replay")
	}
}

func TestAdoptDeadRefusedWhileDraining(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	if err := cfg.Intake.BeginDrain(); err != nil {
		t.Fatalf("BeginDrain: %v", err)
	}
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); !errors.Is(err, ErrDraining) {
		t.Fatalf("AdoptDead err = %v, want ErrDraining", err)
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("draining adoption reached canonical: %v", rows)
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("refused adoption removed the generation")
	}
	want := Row{Key: "k1", Seq: 3, State: RowPending}
	if rows := mustRows(t, g.journal()); rows["k1"] != want {
		t.Errorf("generation row disturbed: %+v, want %+v", rows["k1"], want)
	}
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers while draining: %v", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("draining scan removed the generation")
	}
}

func TestScanRetainsGenerationWhenTransitionOwnerMismatches(t *testing.T) {
	owner1 := proc.Identity{PID: 3131, StartTime: "111.222", Comm: "live", Boot: "test-boot"}
	owner2 := proc.Identity{PID: 4242, StartTime: "333.444", Comm: "dead", Boot: "test-boot"}
	prb := &fakeProber{results: map[int]proberResult{
		owner1.PID: {id: owner1},
		owner2.PID: {err: proc.ErrNoProcess},
	}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	seedOwner(t, g, owner1)
	if _, err := cfg.Canonical.claimTransition(context.Background(), g.Name(), owner1); err != nil {
		t.Fatalf("claimTransition: %v", err)
	}
	seedOwner(t, g, owner2)
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("owner-mismatched generation was removed")
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("owner-mismatched generation was adopted: %v", rows)
	}
	active, ok, err := cfg.Canonical.activeTransition(context.Background())
	if err != nil || !ok || active.Owner.identity() != owner1 {
		t.Errorf("active transition = %+v, %v, %v; want owner1 retained", active, ok, err)
	}
}

func TestAdoptDeadRequiresStrictlyNewerCanonical(t *testing.T) {
	generation := Row{Key: "k1", Seq: 7, State: RowPending}
	if provesAdoption(generation, generation) {
		t.Fatal("equal ordinary seq treated as proven newer")
	}
	canonical := generation
	canonical.Seq++
	if !provesAdoption(canonical, generation) {
		t.Fatal("strictly newer canonical row was not recognized")
	}
}

func TestAdoptDeadSecondAdoptionDoesNotResurrect(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); err != nil {
		t.Fatalf("AdoptDead: %v", err)
	}
	if dirExists(t, g.Dir()) {
		t.Fatal("adopted generation not removed")
	}
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("second AdoptDead err = %v, want os.ErrNotExist", err)
	}
	if dirExists(t, g.Dir()) {
		t.Error("late adoption resurrected the removed generation directory")
	}
}

func TestScanReclaimsOwnerlessGeneration(t *testing.T) {
	cfg, g := newScanEnv(t, &fakeProber{}, Row{Key: "k1", Seq: 4, State: RowYielded})
	if err := os.Remove(filepath.Join(g.Dir(), "owner.json")); err != nil {
		t.Fatal(err)
	}
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers: %v", err)
	}
	if dirExists(t, g.Dir()) {
		t.Fatal("ownerless generation not reclaimed")
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("reclaim adopted rows: %v", rows)
	}
	if _, err := g.claimOwner(context.Background(), deadOwner()); err != nil {
		t.Fatalf("claimOwner on reclaimed name: %v", err)
	}
	prb := cfg.prober.(*fakeProber)
	prb.results = map[int]proberResult{deadPID: {id: deadOwner()}}
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers after re-claim: %v", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("re-claimed generation was reclaimed out from under its owner")
	}
}

func TestScanReclaimSerializesOnRootLock(t *testing.T) {
	cfg, g := newScanEnv(t, &fakeProber{})
	if err := os.Remove(filepath.Join(g.Dir(), "owner.json")); err != nil {
		t.Fatal(err)
	}
	lock, err := (proc.FileLockSpec{
		Path:     g.rootLock(),
		Mode:     proc.FileLockExclusive,
		Deadline: time.Second,
	}).Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := ScanPeers(ctx, cfg); err == nil {
		t.Fatal("ScanPeers succeeded while the root lock was held, want a lock timeout error")
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("locked-out reclaim removed the generation")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers after release: %v", err)
	}
	if dirExists(t, g.Dir()) {
		t.Error("released ownerless generation not reclaimed")
	}
}

func TestScanReclaimFailurePropagates(t *testing.T) {
	cfg, g := newScanEnv(t, &fakeProber{})
	if err := os.Remove(filepath.Join(g.Dir(), "owner.json")); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(g.Dir(), "wedge")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sub, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) })
	if err := ScanPeers(context.Background(), cfg); err == nil {
		t.Fatal("ScanPeers swallowed a failed reclamation, want error")
	}
}

func TestAdoptDeadReprovesDeathUnderLock(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {id: deadOwner()}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); !errors.Is(err, errAdoptionRaced) {
		t.Fatalf("AdoptDead err = %v, want errAdoptionRaced", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("raced adoption removed a live owner's generation")
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("raced adoption reached canonical: %v", rows)
	}
}

func TestJournalWriteCannotResurrectRemovedGeneration(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); err != nil {
		t.Fatalf("AdoptDead: %v", err)
	}
	if dirExists(t, g.Dir()) {
		t.Fatal("adopted generation not removed")
	}
	if _, err := g.journal().apply(context.Background(), Row{Key: "k9", Seq: 1, State: RowPending}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late Apply err = %v, want os.ErrNotExist", err)
	}
	if dirExists(t, g.Dir()) {
		t.Error("late journal write resurrected the removed generation")
	}
}

func TestScanBreakerSpacesFailingPeer(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	if err := os.RemoveAll(g.journal().Path()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(g.journal().Path(), "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	brk := NewBreakers(proc.Backoff{Base: time.Minute, Cap: time.Hour})
	now := time.Unix(1_700_000_000, 0)
	if err := scanOnce(context.Background(), cfg, brk, now); err == nil {
		t.Fatal("scanOnce succeeded on wedged adoption, want error")
	}
	if err := scanOnce(context.Background(), cfg, brk, now.Add(30*time.Second)); err != nil {
		t.Fatalf("failing peer not spaced by its breaker: %v", err)
	}
	if err := scanOnce(context.Background(), cfg, brk, now.Add(2*time.Minute)); err == nil {
		t.Fatal("peer not retried after backoff elapsed")
	}
}

func TestAdoptDeadRefusesChangedOwnerIdentity(t *testing.T) {
	reuser := proc.Identity{PID: 5353, StartTime: "555.666", Comm: "new-daemon", Boot: "test-boot"}
	prb := &fakeProber{results: map[int]proberResult{
		deadPID:    {err: proc.ErrNoProcess},
		reuser.PID: {err: proc.ErrNoProcess},
	}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 3, State: RowPending})
	seedOwner(t, g, reuser)
	if err := AdoptDead(context.Background(), cfg, g, deadOwner()); !errors.Is(err, errAdoptionRaced) {
		t.Fatalf("AdoptDead err = %v, want errAdoptionRaced", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("identity-mismatched adoption removed the generation")
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("identity-mismatched adoption reached canonical: %v", rows)
	}
}

func TestAdoptedRowSurvivesRetriedTruncate(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	ctx := context.Background()
	scope := map[Key]Row{"k1": {Key: "k1", Seq: 2, State: RowPending}}
	mustApply(t, cfg.Canonical, scope["k1"])
	if err := cfg.Canonical.Truncate(ctx, scope); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := AdoptDead(ctx, cfg, g, deadOwner()); err != nil {
		t.Fatalf("AdoptDead: %v", err)
	}
	want := Row{Key: "k1", Seq: 3, State: RowPending}
	if rows := mustRows(t, cfg.Canonical); rows["k1"] != want {
		t.Fatalf("adopted row = %+v, want %+v", rows["k1"], want)
	}
	if err := cfg.Canonical.Truncate(ctx, scope); err != nil {
		t.Fatalf("retried Truncate: %v", err)
	}
	if rows := mustRows(t, cfg.Canonical); rows["k1"] != want {
		t.Errorf("retried truncate deleted the adopted row: got %+v", rows["k1"])
	}
}
