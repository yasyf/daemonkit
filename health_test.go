package daemonkit

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
	"github.com/yasyf/daemonkit/paths"
)

func TestPhaseFromWire(t *testing.T) {
	tests := []struct {
		name string
		wire wire.Phase
		want Phase
	}{
		{"starting", wire.PhaseStarting, PhaseStarting},
		{"ready", wire.PhaseReady, PhaseReady},
		{"draining", wire.PhaseDraining, PhaseDraining},
		{"failed", wire.PhaseFailed, PhaseFailed},
		{"unknown", wire.Phase("runtime_from_a_later_era"), phaseInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := phaseFromWire(tt.wire); got != tt.want {
				t.Fatalf("phaseFromWire(%q) = %d, want %d", tt.wire, got, tt.want)
			}
		})
	}
}

func TestMaxDetail(t *testing.T) {
	tests := []struct {
		name     string
		maxFrame Bytes
		want     Bytes
	}{
		{"default frame", 0, (wire.DefaultMaxFrame - detailEnvelopeReserve) * 3 / 4},
		{"explicit frame", 16 << 20, ((16 << 20) - detailEnvelopeReserve) * 3 / 4},
		{"frame smaller than the envelope", 1 << 10, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaxDetail(tt.maxFrame); got != tt.want {
				t.Fatalf("MaxDetail(%d) = %d, want %d", tt.maxFrame, got, tt.want)
			}
		})
	}
}

// TestServeRefusesOversizedReport proves an oversized Report cannot kill the
// health session: the report never reaches the wire, and the verb keeps
// answering on the same session with the last detail that fit.
func TestServeRefusesOversizedReport(t *testing.T) {
	shortHome(t)
	d := Daemon{Label: "dkdetail", Schemas: []Schema{"test.v1"}, Shutdown: Grace(5 * time.Second)}
	small := bytes.Repeat([]byte("d"), 1024)
	limit := int(MaxDetail(d.MaxFrame))
	reported := make(chan func([]byte), 1)
	done := serveInBackground(context.Background(), t, d, func(c Ctx) (Product, error) {
		c.Report(small)
		reported <- c.Report
		return &stubProduct{}, nil
	})

	socket, err := paths.Socket("dkdetail")
	if err != nil {
		t.Fatalf("Socket: %v", err)
	}
	session := awaitControlSession(t, socket)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := session.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() = %v", err)
	}
	publish := <-reported
	reports := []struct {
		name    string
		publish int
		want    int
	}{
		{"the starting report", 0, len(small)},
		{"exactly the bound", limit, limit},
		{"one byte past the bound", limit + 1, limit},
		{"a report no frame could carry", limit + (1 << 20), limit},
	}
	for _, tt := range reports {
		if tt.publish > 0 {
			publish(bytes.Repeat([]byte("D"), tt.publish))
		}
		report, err := session.Health(ctx)
		if err != nil {
			t.Fatalf("Health() after %s = %v, want a live session", tt.name, err)
		}
		if len(report.Detail) != tt.want {
			t.Fatalf("Detail after %s = %d bytes, want %d", tt.name, len(report.Detail), tt.want)
		}
	}

	if _, err := session.Drain(ctx); err != nil {
		t.Fatalf("Drain() = %v", err)
	}
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatalf("Serve() = %v", out.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Serve did not return after the drain verb")
	}
}
