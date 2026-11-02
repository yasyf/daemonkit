package proc

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestBootSessionUsesKernelUUID(t *testing.T) {
	raw, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		t.Fatal(err)
	}
	want, err := parseBootSession(raw, "kern.bootsessionuuid")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := bootSession()
		if err != nil || got != want {
			t.Fatalf("bootSession() = %d, %v, want kernel UUID stamp %d and no error", got, err, want)
		}
	}
}
