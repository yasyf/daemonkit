package daemonkit

import (
	"strings"
	"testing"
)

func TestDaemonValidateForServeAdmitsABusinessSetNamingTwoBundles(t *testing.T) {
	d := Daemon{Label: "x", Trust: Trust{Business: Requirements{{TeamID: "T", SigningIdentifier: "a"}, {TeamID: "T", SigningIdentifier: "b"}}}}
	if err := d.ValidateForServe(); err != nil {
		t.Fatalf("ValidateForServe() error = %v", err)
	}
}

func TestDaemonValidateForClientJudgesASignedPosture(t *testing.T) {
	tests := []struct {
		name    string
		d       Daemon
		wantErr string
	}{
		{
			"signed posture",
			Daemon{Label: "x", Trust: Trust{Serving: ServingSigned(Requirement{TeamID: "T", SigningIdentifier: "com.example.app"})}},
			"",
		},
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
				t.Fatal("Open() accepted a Daemon ValidateForClient refused")
			}
		})
	}
}

func TestCmdAcceptsTheSignedPosture(t *testing.T) {
	cmd := Cmd{Path: "/bin/echo", Exec: ServingSigned(Requirement{TeamID: "T", SigningIdentifier: "id"})}
	if err := cmd.validate("Spawn", ChannelNone); err != nil {
		t.Fatalf("validate(Spawn) = %v", err)
	}
}
