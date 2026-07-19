package daemon

import "testing"

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
