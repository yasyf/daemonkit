package daemonkit

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestStopRequiresDeadline(t *testing.T) {
	client := openClient(t, Daemon{Label: "com.example.stop"})
	if err := client.Stop(context.Background()); err == nil {
		t.Fatal("Stop() without a deadline succeeded")
	}
}

// neverPlacedDaemon declares a daemon on Stable's own policy under the home
// the test relocated, so the program path the policy names has never been
// written to: the shape a launcher presents to Stop when it uninstalls before
// any install ran.
func neverPlacedDaemon(t *testing.T, label Label) (Daemon, string) {
	t.Helper()
	program, err := Stable()
	if err != nil {
		t.Fatalf("Stable() error = %v", err)
	}
	d := Daemon{Label: label, Program: program}
	return d, programPath(t, d)
}

// stopTwiceWithoutPlacing is the never-installed contract: Stop succeeds, a
// repeated Stop succeeds, and neither puts the program anywhere.
func stopTwiceWithoutPlacing(t *testing.T, client *Client, program string) {
	t.Helper()
	for pass := 1; pass <= 2; pass++ {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		err := client.Stop(ctx)
		cancel()
		if err != nil {
			t.Fatalf("Stop() #%d = %v, want success with the program never placed", pass, err)
		}
		if _, err := os.Lstat(program); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Lstat(%q) after Stop #%d = %v, want the program still never placed", program, pass, err)
		}
	}
}
