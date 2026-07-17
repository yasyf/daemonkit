package daemon

import "testing"

// TestHealthHasFeature: capability is read from Features bits alone.
func TestHealthHasFeature(t *testing.T) {
	tests := []struct {
		name     string
		features []string
		query    string
		want     bool
	}{
		{"present", []string{"handoff", "drain"}, "handoff", true},
		{"present second", []string{"handoff", "drain"}, "drain", true},
		{"absent", []string{"handoff"}, "drain", false},
		{"empty", nil, "handoff", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := Health{Features: tt.features}
			if got := h.HasFeature(tt.query); got != tt.want {
				t.Errorf("HasFeature(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

// TestStateStrings pins the State enum's wire strings; the Swift peer decodes the
// same values.
func TestStateStrings(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateHealthy, "healthy"},
		{StateDegraded, "degraded"},
		{StateFailed, "failed"},
	}
	for _, tt := range tests {
		if string(tt.state) != tt.want {
			t.Errorf("State = %q, want %q", tt.state, tt.want)
		}
	}
}
