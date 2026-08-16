package paths

import (
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/internal/realhome"
)

func TestSocket(t *testing.T) {
	t.Setenv(realhome.EnvOverride, "/tmp")
	tests := []struct {
		name     string
		app      string
		wantLong bool
	}{
		{"short", ".cc-test", false},
		{"at the 103-byte limit", strings.Repeat("a", 68), false},
		{"one past the limit", strings.Repeat("a", 69), true},
		{"far past the limit", strings.Repeat("a", 200), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := "/tmp/.daemonkit/agents/" + tt.app + "/daemon.sock"
			got, err := Socket(tt.app)
			if tt.wantLong {
				var overlong *SocketPathError
				if !errors.As(err, &overlong) {
					t.Fatalf("Socket() error = %v, want *SocketPathError", err)
				}
				if overlong.Path != path {
					t.Errorf("Path = %q, want %q", overlong.Path, path)
				}
				return
			}
			if err != nil {
				t.Fatalf("Socket() error = %v", err)
			}
			if got != path {
				t.Errorf("Socket() = %q, want %q", got, path)
			}
		})
	}
}
