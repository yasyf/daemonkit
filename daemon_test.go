package daemonkit

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestDaemonValidateForServe(t *testing.T) {
	tests := []struct {
		name    string
		d       Daemon
		wantErr string
	}{
		{"zero graces default", Daemon{Label: "x"}, ""},
		{"no label", Daemon{}, "not canonical"},
		{"label escaping the state root", Daemon{Label: "../../evil"}, "not canonical"},
		{"label naming two path elements", Daemon{Label: "bin/daemon"}, "not canonical"},
		{"hidden label", Daemon{Label: ".hidden"}, "not canonical"},
		{"bounds inclusive", Daemon{Label: "x", Shutdown: Grace(24 * time.Hour), Handshake: Grace(time.Millisecond)}, ""},
		{"saturated shutdown", Daemon{Label: "x", Shutdown: Grace(math.MaxInt64)}, "Shutdown"},
		{"negative shutdown", Daemon{Label: "x", Shutdown: Grace(-time.Second)}, "Shutdown"},
		{"overlong shutdown", Daemon{Label: "x", Shutdown: Grace(24*time.Hour + 1)}, "Shutdown"},
		{"saturated handshake", Daemon{Label: "x", Handshake: Grace(math.MaxInt64)}, "Handshake"},
		{"negative handshake", Daemon{Label: "x", Handshake: Grace(-1)}, "Handshake"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.d.ValidateForServe()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateForServe() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateForServe() error = %v, want it to name %s", err, tt.wantErr)
			}
		})
	}
}
