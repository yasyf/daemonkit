package service

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testAgent(t *testing.T) Agent {
	t.Helper()
	return Agent{
		Label:         "com.example.worker",
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), "worker.log"),
		RestartPolicy: RestartAlways,
	}
}

func TestAgentPlistIsPureAndEscaped(t *testing.T) {
	agent := testAgent(t)
	agent.Args = []string{"daemon"}
	agent.Env = map[string]string{"PATH": "/usr/bin", "AMPERSAND": "a&b<c"}
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"<string>com.example.worker</string>",
		"<string>/usr/bin/true</string>",
		"<string>daemon</string>",
		"<key>PATH</key>",
		"<key>KeepAlive</key>",
		"a&amp;b&lt;c",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered plist missing %q\n%s", want, text)
		}
	}
	if strings.Contains(text, "a&b<c") {
		t.Fatalf("rendered plist contains unescaped bytes\n%s", text)
	}
	path, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("Plist mutated filesystem: %v", matches)
	}
}

func TestAgentPlistRequiresCanonicalIdentityAndPaths(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Agent)
		want string
	}{
		{name: "label", edit: func(agent *Agent) { agent.Label = "../worker" }, want: "not canonical"},
		{name: "program", edit: func(agent *Agent) { agent.Program = "usr/bin/true" }, want: "program path"},
		{name: "log", edit: func(agent *Agent) { agent.LogPath = "worker.log" }, want: "log path"},
		{name: "restart", edit: func(agent *Agent) { agent.RestartPolicy = 0 }, want: "restart policy is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent := testAgent(t)
			test.edit(&agent)
			if _, err := agent.Plist(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Plist error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAgentRestartPolicies(t *testing.T) {
	tests := []struct {
		policy RestartPolicy
		want   string
	}{
		{RestartAlways, "<key>KeepAlive</key>\n    <true/>"},
		{RestartOnFailure, "<key>SuccessfulExit</key>\n        <false/>"},
		{NoRestart, "<key>KeepAlive</key>\n    <false/>"},
	}
	for _, test := range tests {
		agent := testAgent(t)
		agent.RestartPolicy = test.policy
		body, err := agent.Plist()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), test.want) {
			t.Fatalf("plist missing %q\n%s", test.want, body)
		}
	}
}

func TestAgentOptionalLaunchPolicy(t *testing.T) {
	agent := testAgent(t)
	agent.StartInterval = 15 * time.Minute
	agent.ProcessType = ProcessTypeBackground
	agent.LimitLoadToSessionType = SessionTypeAqua
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"<key>StartInterval</key>\n    <integer>900</integer>",
		"<key>ProcessType</key>\n    <string>Background</string>",
		"<key>LimitLoadToSessionType</key>\n    <string>Aqua</string>",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("plist missing %q\n%s", want, text)
		}
	}
}

func TestAgentOptionalLaunchPolicyRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		edit func(*Agent)
		want string
	}{
		{edit: func(agent *Agent) { agent.StartInterval = 500 * time.Millisecond }, want: "positive whole number of seconds"},
		{edit: func(agent *Agent) { agent.ProcessType = ProcessType(99) }, want: "invalid process type 99"},
		{edit: func(agent *Agent) { agent.LimitLoadToSessionType = SessionType(99) }, want: "invalid session type 99"},
	}
	for _, test := range tests {
		agent := testAgent(t)
		test.edit(&agent)
		if _, err := agent.Plist(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Plist error = %v, want %q", err, test.want)
		}
	}
}

func TestParseSessionType(t *testing.T) {
	for value, want := range map[string]SessionType{
		"Aqua": SessionTypeAqua, "Background": SessionTypeBackground,
		"LoginWindow": SessionTypeLoginWindow, "StandardIO": SessionTypeStandardIO,
		"System": SessionTypeSystem,
	} {
		got, err := ParseSessionType("\n" + value + " \n")
		if err != nil || got != want {
			t.Fatalf("ParseSessionType(%q) = %d, %v; want %d", value, got, err, want)
		}
	}
	if _, err := ParseSessionType("unknown"); err == nil {
		t.Fatal("ParseSessionType accepted an unknown manager")
	}
}
