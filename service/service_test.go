package service

import (
	"bytes"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/realhome"
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
		{name: "empty program", edit: func(agent *Agent) { agent.Program = "" }, want: "program path"},
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
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"<key>StartInterval</key>\n    <integer>900</integer>",
		"<key>ProcessType</key>\n    <string>Background</string>",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("plist missing %q\n%s", want, text)
		}
	}
}

func TestAgentAssociatedBundleIdentifiersAreCanonicalAndDeterministic(t *testing.T) {
	agent := testAgent(t)
	plain, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "<key>AssociatedBundleIdentifiers</key>") {
		t.Fatalf("plist rendered absent association\n%s", plain)
	}
	agent.AssociatedBundleIdentifiers = []string{
		"com.yasyf.product.helper",
		"com.yasyf.product",
	}
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	want := `<key>AssociatedBundleIdentifiers</key>
    <array>
        <string>com.yasyf.product</string>
        <string>com.yasyf.product.helper</string>
    </array>`
	if !strings.Contains(text, want) {
		t.Fatalf("plist missing sorted associated bundle identifiers\n%s", text)
	}

	for _, values := range [][]string{
		{"product"},
		{".com.yasyf.product"},
		{"com..product"},
		{"com.yasyf.product_1"},
		{"com.yasyf.product", "com.yasyf.product"},
	} {
		agent.AssociatedBundleIdentifiers = values
		if _, err := agent.Plist(); err == nil {
			t.Fatalf("Plist() accepted associated bundle identifiers %q", values)
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
	}
	for _, test := range tests {
		agent := testAgent(t)
		test.edit(&agent)
		if _, err := agent.Plist(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("Plist error = %v, want %q", err, test.want)
		}
	}
}

func TestAgentRendersEverySessionTypeIdentically(t *testing.T) {
	agent := testAgent(t)
	unset, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []SessionType{
		SessionTypeAqua, SessionTypeBackground, SessionTypeLoginWindow,
		SessionTypeStandardIO, SessionTypeSystem, SessionType(99),
	} {
		agent.LimitLoadToSessionType = session
		body, err := agent.Plist()
		if err != nil {
			t.Fatalf("Plist() with session type %d error = %v", session, err)
		}
		if bytes.Contains(body, []byte("<key>LimitLoadToSessionType</key>")) {
			t.Fatalf("session type %d rendered the launchd key\n%s", session, body)
		}
		if !bytes.Equal(body, unset) {
			t.Fatalf("session type %d changed the rendered plist\n%s", session, body)
		}
	}
}

func TestDesiredAgentsWarnsOnlyForPermanentlyRefusedSessionTypes(t *testing.T) {
	tests := []struct {
		name    string
		session SessionType
		want    bool
	}{
		{name: "unset", session: sessionTypeUnset},
		{name: "aqua", session: SessionTypeAqua},
		{name: "background", session: SessionTypeBackground, want: true},
		{name: "login window", session: SessionTypeLoginWindow, want: true},
		{name: "standard io", session: SessionTypeStandardIO, want: true},
		{name: "system", session: SessionTypeSystem, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logs := captureDefaultLog(t)
			agent := testAgent(t)
			agent.LimitLoadToSessionType = test.session
			if _, err := desiredAgents([]Agent{agent}); err != nil {
				t.Fatal(err)
			}
			logged := logs.String()
			if got := strings.Contains(
				logged, "LimitLoadToSessionType is accepted and ignored",
			); got != test.want {
				t.Fatalf("warned = %t, want %t\n%s", got, test.want, logged)
			}
			if !test.want {
				return
			}
			for _, want := range []string{"label=" + agent.Label, "session_type=" + test.session.name()} {
				if !strings.Contains(logged, want) {
					t.Fatalf("warning missing %q\n%s", want, logged)
				}
			}
		})
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

func TestPlistPathIgnoresCallerHome(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv(realhome.EnvOverride, "")
	agent := Agent{Label: "com.example.real-home"}

	got, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(current.HomeDir, "Library", "LaunchAgents", "com.example.real-home.plist")
	if got != want {
		t.Fatalf("PlistPath() = %q, want passwd-home path %q", got, want)
	}
	if strings.HasPrefix(got, os.Getenv("HOME")) {
		t.Fatalf("PlistPath() = %q resolved under the caller HOME", got)
	}
}

func TestPlistPathHonorsOverrideSeam(t *testing.T) {
	override := t.TempDir()
	t.Setenv("HOME", t.TempDir())
	t.Setenv(realhome.EnvOverride, override)
	agent := Agent{Label: "com.example.override"}

	got, err := agent.PlistPath()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(override, "Library", "LaunchAgents", "com.example.override.plist")
	if got != want {
		t.Fatalf("PlistPath() = %q, want override path %q", got, want)
	}
}
