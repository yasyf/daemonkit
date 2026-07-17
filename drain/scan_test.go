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
	return proc.Identity{PID: deadPID, StartTime: deadStart, Comm: "old-daemon"}
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
	g := NewGeneration(dotdir, "g1")
	if err := g.WriteOwner(deadOwner()); err != nil {
		t.Fatal(err)
	}
	if len(rows) > 0 {
		mustApply(t, g.Journal(), rows...)
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
	cfg, g := newScanEnv(t, prb,
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
	reused := proc.Identity{PID: deadPID, StartTime: "999.000", Comm: "newcomer"}
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
	cfg.Intake.Close()
	if err := AdoptDead(context.Background(), cfg.Intake, cfg.Canonical, g); !errors.Is(err, ErrDraining) {
		t.Fatalf("AdoptDead err = %v, want ErrDraining", err)
	}
	if rows := mustRows(t, cfg.Canonical); len(rows) != 0 {
		t.Errorf("draining adoption reached canonical: %v", rows)
	}
	if !dirExists(t, g.Dir()) {
		t.Fatal("refused adoption removed the generation")
	}
	want := Row{Key: "k1", Seq: 3, State: RowPending}
	if rows := mustRows(t, g.Journal()); rows["k1"] != want {
		t.Errorf("generation row disturbed: %+v, want %+v", rows["k1"], want)
	}
	// ScanPeers treats the refusal as a skip, not a scan failure.
	if err := ScanPeers(context.Background(), cfg); err != nil {
		t.Fatalf("ScanPeers while draining: %v", err)
	}
	if !dirExists(t, g.Dir()) {
		t.Error("draining scan removed the generation")
	}
}

func TestScanBreakerSpacesFailingPeer(t *testing.T) {
	prb := &fakeProber{results: map[int]proberResult{deadPID: {err: proc.ErrNoProcess}}}
	cfg, g := newScanEnv(t, prb, Row{Key: "k1", Seq: 1, State: RowPending})
	// Wedge adoption: an unreadable generation journal fails the adopt.
	if err := os.RemoveAll(g.Journal().Path()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(g.Journal().Path(), "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	brk := NewBreakers(proc.Backoff{Base: time.Minute, Cap: time.Hour})
	now := time.Unix(1_700_000_000, 0)
	if err := scanOnce(context.Background(), cfg, brk, now); err == nil {
		t.Fatal("scanOnce succeeded on wedged adoption, want error")
	}
	// Inside the backoff the failing peer is skipped, so the pass is clean.
	if err := scanOnce(context.Background(), cfg, brk, now.Add(30*time.Second)); err != nil {
		t.Fatalf("failing peer not spaced by its breaker: %v", err)
	}
	// After the backoff it is retried and fails again.
	if err := scanOnce(context.Background(), cfg, brk, now.Add(2*time.Minute)); err == nil {
		t.Fatal("peer not retried after backoff elapsed")
	}
}
