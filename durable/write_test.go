package durable

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteFilePublishes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		perm os.FileMode
	}{
		{"payload", []byte("published"), 0o600},
		{"empty", []byte{}, 0o600},
		{"nil is a valid ordering barrier", nil, 0o644},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			if err := WriteFile(path, test.data, test.perm); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(test.data) {
				t.Fatalf("contents = %q, want %q", got, test.data)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != test.perm {
				t.Fatalf("mode = %#o, want %#o", info.Mode().Perm(), test.perm)
			}
		})
	}
}

func TestWriteFileLeavesNoResidue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Fatalf("directory holds %v, want only state.json", entries)
	}
}

// TestWriteFileRefusesAnAbsentDirectory pins the deleted implicit mkdir: a
// missing directory is a loud os error every caller can branch on, not a
// directory this package invents at a permission it chose.
func TestWriteFileRefusesAnAbsentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent", "state.json")
	err := WriteFile(path, []byte("x"), 0o600)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("WriteFile into an absent directory = %v, want os.ErrNotExist", err)
	}
	if _, statErr := os.Stat(filepath.Dir(path)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("WriteFile created the directory: %v", statErr)
	}
}

// TestTempNameDerivesFromTheTarget pins the attribution property that deleted
// the temp-prefix knob: a stump in a scanned directory names the target it
// belongs to.
func TestTempNameDerivesFromTheTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "owner.json")
	w, err := Create(path, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	name := filepath.Base(w.file.Name())
	if !strings.HasPrefix(name, ".owner.json.") {
		t.Fatalf("temp name = %q, want a %q prefix", name, ".owner.json.")
	}
}

func TestWriterCommitsAndDiscards(t *testing.T) {
	t.Run("commit publishes the stream", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "stream.bin")
		w, err := Create(path, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		for _, chunk := range []string{"one", "two", "three"} {
			if _, err := w.Write([]byte(chunk)); err != nil {
				t.Fatal(err)
			}
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("target visible before Commit: %v", statErr)
			}
		}
		if err := w.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "onetwothree" {
			t.Fatalf("contents = %q, want %q", got, "onetwothree")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close after Commit = %v, want a no-op", err)
		}
	})

	t.Run("close discards an uncommitted write", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "stream.bin")
		w, err := Create(path, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("abandoned")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("discarded write left %v behind", entries)
		}
	})
}

// TestSweepSparesAConcurrentWritersTemp pins D12. Two writers racing one path
// is legal — rename is atomic and last committed wins — so an unbounded sweep
// by name would unlink the slower writer's in-flight temp out from under it.
func TestSweepSparesAConcurrentWritersTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	slow, err := Create(path, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if _, err := slow.Write([]byte("slow")); err != nil {
		t.Fatal(err)
	}
	inFlight := slow.file.Name()

	if err := WriteFile(path, []byte("fast"), 0o600); err != nil {
		t.Fatalf("concurrent WriteFile: %v", err)
	}

	if _, err := os.Stat(inFlight); err != nil {
		t.Fatalf("the sweep unlinked a live writer's temp %q: %v", inFlight, err)
	}
	if err := slow.Commit(); err != nil {
		t.Fatalf("Commit after a concurrent publication: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "slow" {
		t.Fatalf("contents = %q, want the last committed writer's %q", got, "slow")
	}
}

// TestSweepCollectsAStaleStump is the other half of D12: a stump a crashed
// writer left behind neither survives forever nor masquerades as foreign state
// to a directory scanner. Same-target is a stricter test than same-prefix — a
// sibling target's stump shares this target's prefix byte for byte, and only
// the temp's own dot-free random component separates them.
func TestSweepCollectsAStaleStump(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	stump := filepath.Join(dir, ".state.json.crashed")
	foreign := filepath.Join(dir, ".other.json.crashed")
	sibling := filepath.Join(dir, ".state.json.bak.crashed")
	for _, name := range []string{stump, foreign, sibling} {
		if err := os.WriteFile(name, []byte("torn"), 0o600); err != nil {
			t.Fatal(err)
		}
		stale := time.Now().Add(-2 * stumpStaleAfter)
		if err := os.Chtimes(name, stale, stale); err != nil {
			t.Fatal(err)
		}
	}

	if err := WriteFile(path, []byte("fresh"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(stump); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale stump for this target survived: %v", err)
	}
	for _, spared := range []string{foreign, sibling} {
		if _, err := os.Stat(spared); err != nil {
			t.Fatalf("the sweep took another target's stump %q: %v", filepath.Base(spared), err)
		}
	}
}
