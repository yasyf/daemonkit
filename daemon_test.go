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
		{"nil business set", Daemon{Label: "x"}, ""},
		{
			"business set naming two bundles",
			Daemon{Label: "x", Trust: Trust{Business: Requirements{{TeamID: "T", SigningIdentifier: "a"}, {TeamID: "T", SigningIdentifier: "b"}}}},
			"",
		},
		{"business set stated but empty", Daemon{Label: "x", Trust: Trust{Business: Requirements{}}}, "Trust.Business"},
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

// TestDaemonValidateForClient is Open's whole config boundary. The unstated
// Trust.Serving is the one this type exists to delete: a nil requirement used
// to read as the floor alone, so a caller that never decided got the posture a
// socket squatter clears, silently.
func TestDaemonValidateForClient(t *testing.T) {
	stated := Trust{Serving: ServingSameUser()}
	tests := []struct {
		name    string
		d       Daemon
		wantErr string
	}{
		{"same-user waiver", Daemon{Label: "x", Trust: stated}, ""},
		{
			"signed posture",
			Daemon{Label: "x", Trust: Trust{Serving: ServingSigned(Requirement{TeamID: "T", SigningIdentifier: "com.example.app"})}},
			"",
		},
		{"unstated serving", Daemon{Label: "x"}, "Trust.Serving"},
		{"no label", Daemon{Trust: stated}, "not canonical"},
		{"label escaping the state root", Daemon{Label: "../../evil", Trust: stated}, "not canonical"},
		{"overlong shutdown", Daemon{Label: "x", Trust: stated, Shutdown: Grace(24*time.Hour + 1)}, "Shutdown"},
		{"negative shutdown", Daemon{Label: "x", Trust: stated, Shutdown: Grace(-time.Second)}, "Shutdown"},
		{"handshake is the serving half", Daemon{Label: "x", Trust: stated, Handshake: Grace(math.MaxInt64)}, ""},
		{
			"serving requirement that admits nobody",
			Daemon{Label: "x", Trust: Trust{Serving: ServingSigned(Requirement{TeamID: "T"})}},
			"Trust.Serving",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.d.ValidateForClient()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateForClient() error = %v", err)
				}
				if _, err := Open(tt.d); err != nil {
					t.Fatalf("Open() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateForClient() error = %v, want it to name %s", err, tt.wantErr)
			}
			if _, err := Open(tt.d); err == nil {
				t.Fatal("Open() accepted a Daemon ValidateForClient refuses")
			}
		})
	}
}
