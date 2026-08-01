package daemonkit

import (
	"strings"
	"testing"
)

func TestCmdBoundaryRefusals(t *testing.T) {
	tests := []struct {
		name    string
		verb    string
		channel Channel
		cmd     Cmd
		want    string
	}{
		{
			name: "empty path",
			verb: "Run",
			cmd:  Cmd{Exec: ServingSameUser()},
			want: "Cmd.Path is required",
		},
		{
			name: "relative path",
			verb: "Run",
			cmd:  Cmd{Path: "bin/echo", Exec: ServingSameUser()},
			want: "is not absolute and clean",
		},
		{
			name: "non-clean path",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/../bin/echo", Exec: ServingSameUser()},
			want: "is not absolute and clean",
		},
		{
			name: "relative dir",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Dir: "rel", Exec: ServingSameUser()},
			want: "Cmd.Dir",
		},
		{
			name: "unstated exec posture",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo"},
			want: "requires a stated Cmd.Exec posture",
		},
		{
			name:    "channel out of range",
			verb:    "Spawn",
			channel: channelLimit,
			cmd:     Cmd{Path: "/bin/echo", Exec: ServingSameUser()},
			want:    "is not one of ChannelNone, ChannelHandoff, ChannelStdio",
		},
		{
			name:    "stdin on the stdio channel",
			verb:    "Spawn",
			channel: ChannelStdio,
			cmd:     Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Stdin: []byte("x")},
			want:    "Cmd.Stdin is refused on ChannelStdio",
		},
		{
			name:    "limits on a stdio channel",
			verb:    "Spawn",
			channel: ChannelStdio,
			cmd:     Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Limits: Limits{Concurrency: 2}},
			want:    "Cmd.Limits is the handoff session's declaration",
		},
		{
			name: "limits on run",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Limits: Limits{MaxFrame: 1}},
			want: "Cmd.Limits is the handoff session's declaration",
		},
		{
			name:    "max output on spawn",
			verb:    "Spawn",
			channel: ChannelNone,
			cmd:     Cmd{Path: "/bin/echo", Exec: ServingSameUser(), MaxOutput: 16},
			want:    "Cmd.MaxOutput bounds a Run's retained streams",
		},
		{
			name: "session on run",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Session: true},
			want: "Cmd.Session is Spawn's posture",
		},
		{
			name: "negative max output",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Exec: ServingSameUser(), MaxOutput: -1},
			want: "is negative",
		},
		{
			name: "duplicate env key",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Env: []string{"PATH=/bin", "TERM=x", "PATH=/usr/bin"}},
			want: "Cmd.Env repeats PATH",
		},
		{
			name: "caller-supplied spawn namespace key",
			verb: "Run",
			cmd:  Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Env: []string{spawnLimitsEnv + "=4,8"}},
			want: "daemonkit's own spawn namespace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cmd.validate(tt.verb, tt.channel)
			if err == nil {
				t.Fatalf("validate(%s) accepted %+v", tt.verb, tt.cmd)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate(%s) = %v, want it to name %q", tt.verb, err, tt.want)
			}
		})
	}
}

func TestCmdAcceptsTheShapesEachVerbOwns(t *testing.T) {
	tests := []struct {
		name    string
		verb    string
		channel Channel
		cmd     Cmd
	}{
		{"run with a cap", "Run", ChannelNone, Cmd{Path: "/bin/echo", Exec: ServingSameUser(), MaxOutput: 16}},
		{"handoff with limits", "Spawn", ChannelHandoff, Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Limits: Limits{MaxFrame: 1 << 20, Concurrency: 4}}},
		{"stdio with stderr only", "Spawn", ChannelStdio, Cmd{Path: "/bin/echo", Exec: ServingSameUser()}},
		{"signed posture", "Spawn", ChannelNone, Cmd{Path: "/bin/echo", Exec: ServingSigned(Requirement{TeamID: "T", SigningIdentifier: "id"})}},
		{"session on spawn", "Spawn", ChannelNone, Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Session: true}},
		{"nil env inherits", "Run", ChannelNone, Cmd{Path: "/bin/echo", Exec: ServingSameUser(), Env: nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cmd.validate(tt.verb, tt.channel); err != nil {
				t.Fatalf("validate(%s) = %v", tt.verb, err)
			}
		})
	}
}

// TestChildEnvStripsAnInheritedSpawnConveyance is D2's other half: a caller
// cannot name the spawn namespace, and an inherited value never rides a second
// exec, so the variables Spawn appends cannot collide or mislead.
func TestChildEnvStripsAnInheritedSpawnConveyance(t *testing.T) {
	t.Setenv(spawnLimitsEnv, "1,1")
	t.Setenv("DAEMONKIT_SPAWNED_NONCE", "deadbeef")
	t.Setenv("DAEMONKIT_HOME", "/tmp/kept")

	none := childEnv(Cmd{}, ChannelNone, nil)
	for _, entry := range none {
		if strings.HasPrefix(entry, spawnEnvPrefix) {
			t.Fatalf("ChannelNone env carries %q", entry)
		}
	}
	if !hasEnv(none, "DAEMONKIT_HOME=/tmp/kept") {
		t.Fatal("the strip took DAEMONKIT_HOME, which is not daemonkit's spawn namespace")
	}

	handoff := childEnv(Cmd{Limits: Limits{MaxFrame: 4096, Concurrency: 3}}, ChannelHandoff, []byte{0xaa, 0xbb})
	if !hasEnv(handoff, "DAEMONKIT_SPAWNED_NONCE=aabb") {
		t.Fatalf("handoff env = %v, want the freshly minted nonce", spawnScoped(handoff))
	}
	if !hasEnv(handoff, spawnLimitsEnv+"=4096,3") {
		t.Fatalf("handoff env = %v, want the conveyed limits", spawnScoped(handoff))
	}
	if got := len(spawnScoped(handoff)); got != 2 {
		t.Fatalf("handoff env carries %d spawn-namespace entries, want exactly the two daemonkit owns", got)
	}
}

func TestParseSpawnLimitsRoundTrips(t *testing.T) {
	env := childEnv(Cmd{Limits: Limits{MaxFrame: 1 << 20, Concurrency: 9}}, ChannelHandoff, nil)
	var value string
	for _, entry := range env {
		if key, rest, _ := strings.Cut(entry, "="); key == spawnLimitsEnv {
			value = rest
		}
	}
	limits, err := parseSpawnLimits(value)
	if err != nil {
		t.Fatalf("parseSpawnLimits(%q) = %v", value, err)
	}
	if limits != (Limits{MaxFrame: 1 << 20, Concurrency: 9}) {
		t.Fatalf("parseSpawnLimits(%q) = %+v", value, limits)
	}
	if _, err := parseSpawnLimits("4096"); err == nil {
		t.Fatal("parseSpawnLimits accepted a value with no concurrency")
	}
}

func TestExitErrorNamesSignalAndStatusApart(t *testing.T) {
	status := (&ExitError{Exit: Exit{Code: 3}}).Error()
	if !strings.Contains(status, "status 3") {
		t.Fatalf("ExitError(code 3) = %q", status)
	}
	signal := (&ExitError{Exit: Exit{Code: -1, Signal: 9}}).Error()
	if !strings.Contains(signal, "signal") {
		t.Fatalf("ExitError(signal 9) = %q", signal)
	}
}

func hasEnv(env []string, want string) bool {
	for _, entry := range env {
		if entry == want {
			return true
		}
	}
	return false
}

func spawnScoped(env []string) []string {
	var scoped []string
	for _, entry := range env {
		if strings.HasPrefix(entry, spawnEnvPrefix) {
			scoped = append(scoped, entry)
		}
	}
	return scoped
}
