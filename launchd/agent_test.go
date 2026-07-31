package launchd

import (
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

const goldenPlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.example.worker</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/worker</string>
        <string>daemon</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>

    <key>StartInterval</key>
    <integer>900</integer>

    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ExitTimeOut</key>
    <integer>45</integer>

    <key>ProcessType</key>
    <string>Background</string>

    <key>StandardOutPath</key>
    <string>/Users/example/Library/Logs/worker.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/example/Library/Logs/worker.log</string>
    <key>AssociatedBundleIdentifiers</key>
    <array>
        <string>com.example.worker</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>DAEMONKIT_AGENT_OWNER</key>
        <string>daemonkit</string>
        <key>PATH</key>
        <string>/usr/bin</string>
    </dict>
</dict>
</plist>
`

// TestAgentPlistGoldenCarriesOwnerMarker freezes the exact rendered plist,
// including the DAEMONKIT_AGENT_OWNER ownership marker sorted into
// EnvironmentVariables. A drift here is a wire-format change consumers see.
func TestAgentPlistGoldenCarriesOwnerMarker(t *testing.T) {
	agent := Agent{
		Label:                       "com.example.worker",
		Program:                     "/usr/local/bin/worker",
		Args:                        []string{"daemon"},
		LogPath:                     "/Users/example/Library/Logs/worker.log",
		Env:                         map[string]string{"PATH": "/usr/bin"},
		AssociatedBundleIdentifiers: []string{"com.example.worker"},
		RestartPolicy:               RestartOnFailure,
		ProcessType:                 ProcessTypeBackground,
		StartInterval:               15 * time.Minute,
		ExitTimeOut:                 45 * time.Second,
	}
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != goldenPlist {
		t.Fatalf("rendered plist drifted from golden\n--- got ---\n%s\n--- want ---\n%s", body, goldenPlist)
	}
	if !strings.Contains(goldenPlist, "<key>"+OwnerEnvKey+"</key>") {
		t.Fatalf("golden plist lacks the ownership marker %q", OwnerEnvKey)
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
		"<key>" + OwnerEnvKey + "</key>",
		"a&amp;b&lt;c",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered plist missing %q\n%s", want, text)
		}
	}
	if strings.Contains(text, "a&b<c") {
		t.Fatalf("rendered plist contains unescaped bytes\n%s", text)
	}
}

// TestAgentPlistOmitsExitTimeOutWhenUnset holds the launchd default: an agent
// that names no drain grace renders no ExitTimeOut key, so launchd's own
// 20-second SIGKILL backstop applies and the plist bytes of every agent written
// before the field existed are unchanged.
func TestAgentPlistOmitsExitTimeOutWhenUnset(t *testing.T) {
	agent := testAgent(t)
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "<key>ExitTimeOut</key>") {
		t.Fatalf("plist names ExitTimeOut without one set\n%s", body)
	}
}

func TestAgentPlistRendersExitTimeOutAsWholeSeconds(t *testing.T) {
	agent := testAgent(t)
	agent.ExitTimeOut = 90 * time.Second
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if want := "<key>ExitTimeOut</key>\n    <integer>90</integer>"; !strings.Contains(string(body), want) {
		t.Fatalf("plist missing %q\n%s", want, body)
	}
}

func TestAgentPlistRejectsReservedOwnerEnvKey(t *testing.T) {
	agent := testAgent(t)
	agent.Env = map[string]string{OwnerEnvKey: "someone-else"}
	if _, err := agent.Plist(); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Plist error = %v, want a reserved-key refusal", err)
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
		{
			name: "sub-second exit timeout",
			edit: func(agent *Agent) { agent.ExitTimeOut = 500 * time.Millisecond },
			want: "exit timeout must be a positive whole number of seconds",
		},
		{
			name: "fractional exit timeout",
			edit: func(agent *Agent) { agent.ExitTimeOut = 1500 * time.Millisecond },
			want: "exit timeout must be a positive whole number of seconds",
		},
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
