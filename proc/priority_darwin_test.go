//go:build darwin

package proc

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestNice(t *testing.T) {
	pre, err := unix.Getpriority(unix.PRIO_PROCESS, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pre >= 5 {
		t.Skipf("pre-state: nice already %d; cannot lower priority to observe Nice(5)", pre)
	}

	if err := Nice(5); err != nil {
		t.Fatal(err)
	}
	got, err := unix.Getpriority(unix.PRIO_PROCESS, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("getpriority after Nice(5) = %d; want 5", got)
	}

	if band, err := unix.Getpriority(4, 0); err != nil || band != 0 {
		t.Fatalf("darwin band after Nice(5) = %#x, %v; want foreground (0)", band, err)
	}

	if unix.Getuid() != 0 {
		if err := unix.Setpriority(unix.PRIO_PROCESS, 0, pre); err == nil {
			t.Fatalf("renice %d -> %d unexpectedly succeeded for an unprivileged process", got, pre)
		}
	}
}
