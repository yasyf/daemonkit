package daemonkit

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/proc"
)

func TestRunForwardsToStore(t *testing.T) {
	store, err := proc.OpenStore(filepath.Join(t.TempDir(), "records.dkstate"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := Run(ctx, store, proc.Cmd{Path: "/bin/echo", Args: []string{"hi"}, Stdin: []byte("ignored")})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Exit.Code != 0 {
		t.Fatalf("Exit.Code = %d, want 0", result.Exit.Code)
	}
	if got := bytes.TrimSpace(result.Stdout); !bytes.Equal(got, []byte("hi")) {
		t.Fatalf("Stdout = %q, want %q", got, "hi")
	}
}
