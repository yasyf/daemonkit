package proc

import (
	"testing"
	"time"
)

func TestMonotonicUptimeAdvancesWithinOneBoot(t *testing.T) {
	first, err := MonotonicUptime()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := MonotonicUptime()
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("monotonic uptime did not advance: %s -> %s", first, second)
	}
}
