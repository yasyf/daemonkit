package drain

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestJournalCASRejectsStaleReplay(t *testing.T) {
	j := NewJournal(filepath.Join(t.TempDir(), "journal.json"))
	ctx := context.Background()

	mustApply(t, j, Row{Key: "k1", Seq: 2, State: RowPending})

	tests := []struct {
		name    string
		row     Row
		want    int
		wantSeq uint64
	}{
		{"equal seq replay is a no-op", Row{Key: "k1", Seq: 2, State: RowYielded}, 0, 2},
		{"lower seq replay is a no-op", Row{Key: "k1", Seq: 1, State: RowYielded}, 0, 2},
		{"higher seq applies", Row{Key: "k1", Seq: 3, State: RowYielded}, 1, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := j.Apply(ctx, tt.row)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if n != tt.want {
				t.Errorf("applied = %d, want %d", n, tt.want)
			}
			rows, err := j.Rows(ctx)
			if err != nil {
				t.Fatalf("Rows: %v", err)
			}
			if got := rows["k1"].Seq; got != tt.wantSeq {
				t.Errorf("stored seq = %d, want %d", got, tt.wantSeq)
			}
		})
	}
}

func TestJournalApplyCountsPerRow(t *testing.T) {
	j := NewJournal(filepath.Join(t.TempDir(), "journal.json"))
	mustApply(t, j, Row{Key: "a", Seq: 5, State: RowPending})

	n, err := j.Apply(context.Background(),
		Row{Key: "a", Seq: 4, State: RowYielded}, // stale
		Row{Key: "b", Seq: 1, State: RowPending}, // fresh
	)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if n != 1 {
		t.Errorf("applied = %d, want 1", n)
	}
	rows := mustRows(t, j)
	if rows["a"].State != RowPending || rows["a"].Seq != 5 {
		t.Errorf("row a mutated by stale replay: %+v", rows["a"])
	}
	if rows["b"].Seq != 1 {
		t.Errorf("row b = %+v, want seq 1", rows["b"])
	}
}

func TestJournalPreservesForeignRowBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.json")
	foreign := `{"key":"a","seq":7,"state":"pending","x": [1,  2]}`
	if err := os.WriteFile(path, []byte(`{"a":`+foreign+`}`), 0o600); err != nil {
		t.Fatal(err)
	}
	j := NewJournal(path)
	mustApply(t, j, Row{Key: "b", Seq: 1, State: RowPending})

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), foreign) {
		t.Errorf("foreign row bytes not preserved verbatim:\n%s", data)
	}
}

func TestJournalBumpAdvancesSeq(t *testing.T) {
	j := NewJournal(filepath.Join(t.TempDir(), "journal.json"))
	ctx := context.Background()

	r1, err := j.Bump(ctx, "k1", RowPending)
	if err != nil {
		t.Fatalf("Bump: %v", err)
	}
	if r1.Seq != 1 || r1.State != RowPending {
		t.Errorf("first bump = %+v, want seq 1 pending", r1)
	}
	r2, err := j.Bump(ctx, "k1", RowYielded)
	if err != nil {
		t.Fatalf("Bump: %v", err)
	}
	if r2.Seq != 2 || r2.State != RowYielded {
		t.Errorf("second bump = %+v, want seq 2 yielded", r2)
	}
}

func TestJournalTruncate(t *testing.T) {
	j := NewJournal(filepath.Join(t.TempDir(), "journal.json"))
	ctx := context.Background()
	mustApply(t, j,
		Row{Key: "a", Seq: 1, State: RowPending},
		Row{Key: "b", Seq: 2, State: RowPending},
	)
	if err := j.Truncate(ctx); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if rows := mustRows(t, j); len(rows) != 0 {
		t.Errorf("rows after truncate = %v, want empty", rows)
	}
}

func TestJournalTruncatePreservesSeqEpoch(t *testing.T) {
	j := NewJournal(filepath.Join(t.TempDir(), "journal.json"))
	ctx := context.Background()
	mustApply(t, j,
		Row{Key: "a", Seq: 5, State: RowPending},
		Row{Key: "b", Seq: 2, State: RowPending},
	)
	if err := j.Truncate(ctx); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	fresh, err := j.Bump(ctx, "a", RowPending)
	if err != nil {
		t.Fatalf("Bump: %v", err)
	}
	if fresh.Seq != 6 {
		t.Errorf("post-truncate seq = %d, want 6 (above the pre-truncate max 5)", fresh.Seq)
	}
	if err := j.Truncate(ctx); err != nil {
		t.Fatalf("second Truncate: %v", err)
	}
	again, err := j.Bump(ctx, "b", RowPending)
	if err != nil {
		t.Fatalf("Bump: %v", err)
	}
	if again.Seq != 7 {
		t.Errorf("second-epoch seq = %d, want 7 (above %d)", again.Seq, fresh.Seq)
	}
}

func TestResolveOwner(t *testing.T) {
	tests := []struct {
		name      string
		canonical map[Key]Row
		gen       map[Key]Row
		want      OwnedBy
	}{
		{"neither owns", nil, nil, OwnedByNone},
		{"canonical only", map[Key]Row{"k": {Seq: 1}}, nil, OwnedByCanonical},
		{"generation only", nil, map[Key]Row{"k": {Seq: 1}}, OwnedByGeneration},
		{"equal seq snapshot copy is generation-owned", map[Key]Row{"k": {Seq: 3}}, map[Key]Row{"k": {Seq: 3}}, OwnedByGeneration},
		{"proven-newer canonical wins", map[Key]Row{"k": {Seq: 4}}, map[Key]Row{"k": {Seq: 3}}, OwnedByCanonical},
		{"stale canonical loses", map[Key]Row{"k": {Seq: 2}}, map[Key]Row{"k": {Seq: 3}}, OwnedByGeneration},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveOwner(tt.canonical, tt.gen, "k"); got != tt.want {
				t.Errorf("ResolveOwner = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGenerationOwnerRoundTrip(t *testing.T) {
	g := NewGeneration(t.TempDir(), "g1")
	id := proc.Identity{PID: 4242, StartTime: "111.222", Comm: "daemon"}
	if err := g.WriteOwner(id); err != nil {
		t.Fatalf("WriteOwner: %v", err)
	}
	got, err := g.ReadOwner()
	if err != nil {
		t.Fatalf("ReadOwner: %v", err)
	}
	if got != id {
		t.Errorf("owner = %+v, want %+v", got, id)
	}
}

func TestGenerationOwnerUnreadable(t *testing.T) {
	dotdir := t.TempDir()
	tests := []struct {
		name string
		prep func(t *testing.T, g Generation)
	}{
		{"missing file", func(*testing.T, Generation) {}},
		{"corrupt json", func(t *testing.T, g Generation) {
			if err := os.MkdirAll(g.Dir(), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(g.Dir(), "owner.json"), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"incomplete identity", func(t *testing.T, g Generation) {
			if err := os.MkdirAll(g.Dir(), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(g.Dir(), "owner.json"), []byte(`{"pid":0,"start_time":""}`), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGeneration(dotdir, "g"+string(rune('a'+i)))
			tt.prep(t, g)
			if _, err := g.ReadOwner(); err == nil {
				t.Error("ReadOwner succeeded, want error")
			}
		})
	}
}

func TestGenerations(t *testing.T) {
	dotdir := t.TempDir()
	if got, err := Generations(dotdir); err != nil || len(got) != 0 {
		t.Fatalf("missing layout: got %v, %v; want empty, nil", got, err)
	}
	for _, gen := range []string{"g1", "g2"} {
		if err := os.MkdirAll(filepath.Join(dotdir, "drain", gen), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dotdir, "drain", "stray.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := Generations(dotdir)
	if err != nil {
		t.Fatalf("Generations: %v", err)
	}
	if len(got) != 2 || got[0].Name() != "g1" || got[1].Name() != "g2" {
		t.Errorf("generations = %v, want [g1 g2]", got)
	}
}
