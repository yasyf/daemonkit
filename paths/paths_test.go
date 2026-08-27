package paths

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
)

// agentLayout is the golden the Swift half is pinned to as well, so the two
// derivations of ~/.daemonkit/a/<label> cannot drift.
type agentLayout struct {
	Home     string `json:"home"`
	Label    string `json:"label"`
	StateDir string `json:"stateDir"`
	Socket   string `json:"socket"`
}

func readAgentLayout(t *testing.T) agentLayout {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "agent-layout.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden agentLayout
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	return golden
}

func TestAgentMatchesTheSharedSwiftGolden(t *testing.T) {
	golden := readAgentLayout(t)
	t.Setenv(realhome.EnvOverride, golden.Home)
	agent := Agent(golden.Label)
	socket, err := Socket(golden.Label)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"state dir", agent.StateDir(), golden.StateDir},
		{"socket", agent.SocketPath(), golden.Socket},
		{"Socket", socket, golden.Socket},
		{"log", agent.LogPath(), filepath.Join(golden.StateDir, "daemon.log")},
		{"start lock", agent.StartLockPath(), filepath.Join(golden.StateDir, "locks", "start.lock")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestRepoTurnsDir(t *testing.T) {
	t.Setenv(realhome.EnvOverride, t.TempDir())
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
