package daemonkit

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// TestASignedRequirementIsRefusedAtEveryConfigBoundary is the no-downgrade rule
// where a consumer meets it. Linux can verify no code identity, so a Daemon or
// a Cmd that states one is refused by name, with ErrNoVerifier, before anything
// serves, attaches, or spawns — never read as the same-user floor.
func TestASignedRequirementIsRefusedAtEveryConfigBoundary(t *testing.T) {
	signed := Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.example.app"}
	tests := []struct {
		name  string
		field string
		check func() error
	}{
		{
			"a client pinning the serving process", "Trust.Serving",
			func() error {
				_, err := Open(Daemon{Label: "x", Trust: Trust{Serving: ServingSigned(signed)}})
				return err
			},
		},
		{
			"a client validated directly", "Trust.Serving",
			Daemon{Label: "x", Trust: Trust{Serving: ServingSigned(signed)}}.ValidateForClient,
		},
		{
			"a daemon gating its control lane", "Trust.Control",
			Daemon{Label: "x", Trust: Trust{Control: &signed}}.ValidateForServe,
		},
		{
			"a daemon gating its business lane", "Trust.Business",
			Daemon{Label: "x", Trust: Trust{Business: Requirements{signed}}}.ValidateForServe,
		},
		{
			"a served daemon", "Trust.Control",
			func() error {
				_, err := Serve(t.Context(), Daemon{Label: "x", Trust: Trust{Control: &signed}}, func(Ctx) (Product, error) {
					t.Error("Serve started a daemon whose requirement nothing can verify")
					return &stubProduct{}, nil
				})
				return err
			},
		},
		{
			"a spawned command", "Cmd.Exec",
			func() error {
				return Cmd{Path: "/bin/echo", Exec: ServingSigned(signed)}.validate("Spawn", ChannelNone)
			},
		},
		{
			"a run command", "Cmd.Exec",
			func() error { return Cmd{Path: "/bin/echo", Exec: ServingSigned(signed)}.validate("Run", ChannelNone) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.check()
			if !errors.Is(err, ErrNoVerifier) {
				t.Fatalf("error = %v, want ErrNoVerifier", err)
			}
			if !strings.Contains(err.Error(), tt.field) {
				t.Fatalf("error = %v, want it to name %s", err, tt.field)
			}
		})
	}
}

// TestASignedSpawnNeverRunsItsChild drives the refusal through the verbs
// themselves: the posture is refused before a process exists.
func TestASignedSpawnNeverRunsItsChild(t *testing.T) {
	owned := ownedScope(t)
	signed := ServingSigned(Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.example.app"})
	marker := t.TempDir() + "/ran"
	cmd := Cmd{Path: "/bin/sh", Args: []string{"-c", "touch " + marker}, Exec: signed}
	if _, err := owned.Spawn(bounded(t, 20*time.Second), cmd, ChannelNone, nil); !errors.Is(err, ErrNoVerifier) {
		t.Fatalf("Spawn() = %v, want ErrNoVerifier", err)
	}
	if _, err := owned.Run(bounded(t, 20*time.Second), cmd); !errors.Is(err, ErrNoVerifier) {
		t.Fatalf("Run() = %v, want ErrNoVerifier", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("the refused command ran: %v", err)
	}
}
