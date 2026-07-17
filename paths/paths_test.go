package paths

import (
	"path/filepath"
	"regexp"
	"testing"
)

func TestRepoTurnsDir(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	p := Paths{App: ".cc-test"}

	tests := []struct {
		name     string
		repoRoot string
	}{
		{name: "simple root", repoRoot: "/Users/alice/code/project"},
		{name: "root with spaces", repoRoot: "/Users/alice/my project"},
	}
	hashed := regexp.MustCompile(`^[0-9a-f]{16}$`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.RepoTurnsDir(tt.repoRoot)
			if again := p.RepoTurnsDir(tt.repoRoot); again != got {
				t.Fatalf("RepoTurnsDir not deterministic: %q vs %q", got, again)
			}
			if dir := filepath.Dir(got); dir != p.TurnsDir() {
				t.Fatalf("parent = %q, want %q", dir, p.TurnsDir())
			}
			if base := filepath.Base(got); !hashed.MatchString(base) {
				t.Fatalf("base = %q, want 16 hex chars", base)
			}
		})
	}

	if p.RepoTurnsDir("/repo/a") == p.RepoTurnsDir("/repo/b") {
		t.Fatal("distinct repo roots mapped to the same turns dir")
	}
}
