package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

var errFakeLaunch = errors.New("fake launchctl failure")

type fakeLauncher struct {
	failOn string
	verbs  []string
}

func (f *fakeLauncher) Run(_ context.Context, args ...string) (string, error) {
	f.verbs = append(f.verbs, args[0])
	if args[0] == f.failOn {
		return "boom", errFakeLaunch
	}
	return "", nil
}

func TestAgentInstall(t *testing.T) {
	cases := []struct {
		name      string
		failOn    string
		wantVerbs []string
		wantErr   bool
	}{
		{"runs the donor order and succeeds", "", []string{"bootout", "bootstrap", "enable", "kickstart"}, false},
		{"bootstrap failure aborts after bootstrap", "bootstrap", []string{"bootout", "bootstrap"}, true},
		{"kickstart failure runs all four then errors", "kickstart", []string{"bootout", "bootstrap", "enable", "kickstart"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			f := &fakeLauncher{failOn: tc.failOn}
			a := Agent{
				Label:         "com.yasyf.cc-pool",
				Program:       "/opt/homebrew/bin/cc-pool",
				Args:          []string{"daemon"},
				LogPath:       filepath.Join(t.TempDir(), "daemon.log"),
				RestartPolicy: RestartAlways,
				Launcher:      f,
			}
			err := a.Install(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("Install() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, errFakeLaunch) {
				t.Errorf("Install() err = %v, want errors.Is-wrapped %v", err, errFakeLaunch)
			}
			if !slices.Equal(f.verbs, tc.wantVerbs) {
				t.Errorf("launchctl verbs = %q, want %q", f.verbs, tc.wantVerbs)
			}
		})
	}
}

func TestStatusLines(t *testing.T) {
	cases := []struct {
		id   string
		got  []string
		want []string
	}{
		{
			id:   "brew managed with info",
			got:  brewStatus("cc-pool (homebrew.mxcl.cc-pool)\nRunning: ✔", true),
			want: []string{"Management: Homebrew (brew services)", "cc-pool (homebrew.mxcl.cc-pool)\nRunning: ✔"},
		},
		{
			id:   "brew managed but info unavailable",
			got:  brewStatus("", false),
			want: []string{"Management: Homebrew (brew services)"},
		},
		{
			id:   "self managed and loaded",
			got:  []string{selfStatus(true)},
			want: []string{"Management: self-managed LaunchAgent (loaded: true)"},
		},
		{
			id:   "self managed and not loaded",
			got:  []string{selfStatus(false)},
			want: []string{"Management: self-managed LaunchAgent (loaded: false)"},
		},
	}
	for _, tc := range cases {
		if !slices.Equal(tc.got, tc.want) {
			t.Errorf("%s: got %q, want %q", tc.id, tc.got, tc.want)
		}
	}
}

func TestAgentPathIsBrewManaged(t *testing.T) {
	t.Setenv("HOMEBREW_PREFIX", "/opt/homebrew")
	a := Agent{Formula: "cc-pool"}
	cases := []struct {
		path string
		want bool
	}{
		{path: "/opt/homebrew/Cellar/cc-pool/1.2.3/bin/cc-pool", want: true},
		{path: "/opt/homebrew/opt/cc-pool/bin/cc-pool", want: true},
		{path: "/opt/homebrew/bin/cc-pool", want: true},
		{path: "/Users/x/go/bin/cc-pool", want: false},
		{path: "/usr/local/bin/other-tool", want: false},
	}
	for _, tc := range cases {
		if got := a.pathIsBrewManaged(tc.path); got != tc.want {
			t.Errorf("pathIsBrewManaged(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestBrewPrefixesHonorsEnv(t *testing.T) {
	t.Setenv("HOMEBREW_PREFIX", "/custom/brew")
	if got := brewPrefixes(); len(got) != 1 || got[0] != "/custom/brew" {
		t.Errorf("brewPrefixes() = %v, want [/custom/brew]", got)
	}
}

func TestWritePlistRendersAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	logPath := filepath.Join(home, ".cc-pool", "daemon.log")
	a := Agent{
		Label:         "com.yasyf.cc-pool",
		Formula:       "cc-pool",
		Program:       "/opt/homebrew/bin/cc-pool",
		Args:          []string{"daemon"},
		LogPath:       logPath,
		RestartPolicy: RestartAlways,
		Env: map[string]string{
			"PATH":      "/usr/bin",
			"AMPERSAND": "a&b<c",
		},
	}
	path, err := a.WritePlist()
	if err != nil {
		t.Fatalf("WritePlist() = %v", err)
	}
	if want := filepath.Join(home, "Library", "LaunchAgents", "com.yasyf.cc-pool.plist"); path != want {
		t.Errorf("plist path = %q, want %q", path, want)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		"<string>com.yasyf.cc-pool</string>",
		"<string>/opt/homebrew/bin/cc-pool</string>",
		"<string>daemon</string>",
		"<string>" + logPath + "</string>",
		"<key>PATH</key>",
		"<string>/usr/bin</string>",
		"<key>KeepAlive</key>",
		"a&amp;b&lt;c",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered plist missing %q\n---\n%s", want, s)
		}
	}
	if strings.Contains(s, "a&b<c") {
		t.Errorf("rendered plist contains an unescaped env value\n---\n%s", s)
	}
	if _, err := os.Stat(filepath.Dir(logPath)); err != nil {
		t.Errorf("log dir was not created: %v", err)
	}
}

func TestAgentRestartPolicyRequired(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	a := Agent{
		Label:   "com.example.worker",
		Program: "/usr/bin/true",
		LogPath: filepath.Join(t.TempDir(), "worker.log"),
	}
	if _, err := a.WritePlist(); err == nil || !strings.Contains(err.Error(), "restart policy is required") {
		t.Fatalf("WritePlist() err = %v, want required restart policy", err)
	}
}

func TestAgentRestartPolicyRejectsUnknownValue(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	a := Agent{
		Label:         "com.example.worker",
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), "worker.log"),
		RestartPolicy: RestartPolicy(99),
	}
	if _, err := a.WritePlist(); err == nil || !strings.Contains(err.Error(), "invalid restart policy 99") {
		t.Fatalf("WritePlist() err = %v, want invalid restart policy", err)
	}
}

func TestAgentRestartPolicies(t *testing.T) {
	cases := []struct {
		name   string
		policy RestartPolicy
		want   string
	}{
		{"always", RestartAlways, "<key>KeepAlive</key>\n    <true/>"},
		{"failure", RestartOnFailure, "<key>SuccessfulExit</key>\n        <false/>"},
		{"never", NoRestart, "<key>KeepAlive</key>\n    <false/>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			a := Agent{
				Label:         "com.example.worker",
				Program:       "/usr/bin/true",
				LogPath:       filepath.Join(home, "worker.log"),
				RestartPolicy: tc.policy,
			}
			path, err := a.WritePlist()
			if err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), tc.want) {
				t.Errorf("plist missing %q:\n%s", tc.want, body)
			}
		})
	}
}
