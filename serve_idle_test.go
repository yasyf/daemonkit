package daemonkit

import (
	"testing"
	"time"
)

func TestIdleOrDefaultReachesTheServer(t *testing.T) {
	if got := idleOrDefault(0); got != time.Duration(defaultIdle) {
		t.Fatalf("idleOrDefault(0) = %v, want the %v default", got, defaultIdle)
	}
	if got := idleOrDefault(Grace(90 * time.Second)); got != 90*time.Second {
		t.Fatalf("idleOrDefault(90s) = %v, want the stated override", got)
	}
}
