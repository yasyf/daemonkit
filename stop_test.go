package daemonkit

import (
	"context"
	"testing"
)

func TestStopRequiresDeadline(t *testing.T) {
	client := openClient(t, Daemon{Label: "com.example.stop"})
	if err := client.Stop(context.Background()); err == nil {
		t.Fatal("Stop() without a deadline succeeded")
	}
}
