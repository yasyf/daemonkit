package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/worker"
)

const keepAliveGolden = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yasyf.fusekit-holder</string>
    <key>Program</key>
    <string>/usr/bin/open</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/open</string>
        <string>-g</string>
        <string>-W</string>
        <string>/Applications/fusekit-holder.app</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
`

func TestAppKeepAliveGoldenPlist(t *testing.T) {
	k := AppKeepAlive{
		Label:         "com.yasyf.fusekit-holder",
		AppPath:       "/Applications/fusekit-holder.app",
		RestartPolicy: RestartAlways,
	}
	body, err := k.plist()
	if err != nil {
		t.Fatalf("plist() = %v", err)
	}
	if string(body) != keepAliveGolden {
		t.Fatalf("rendered plist drifted from the golden artifact:\n--- got ---\n%s\n--- want ---\n%s", body, keepAliveGolden)
	}
}

const keepAliveGoldenWithBundle = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yasyf.fusekit-holder</string>
    <key>Program</key>
    <string>/usr/bin/open</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/open</string>
        <string>-g</string>
        <string>-W</string>
        <string>/Applications/fusekit-holder.app</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>AssociatedBundleIdentifiers</key>
    <array>
        <string>com.yasyf.FusekitHolder</string>
    </array>
</dict>
</plist>
`

func TestAppKeepAliveGoldenPlistWithBundleID(t *testing.T) {
	k := AppKeepAlive{
		Label:         "com.yasyf.fusekit-holder",
		AppPath:       "/Applications/fusekit-holder.app",
		BundleID:      "com.yasyf.FusekitHolder",
		RestartPolicy: RestartAlways,
	}
	body, err := k.plist()
	if err != nil {
		t.Fatalf("plist() = %v", err)
	}
	if string(body) != keepAliveGoldenWithBundle {
		t.Fatalf("rendered plist drifted from the golden artifact:\n--- got ---\n%s\n--- want ---\n%s", body, keepAliveGoldenWithBundle)
	}
}

func TestAppKeepAlivePlistEscapesAppPath(t *testing.T) {
	k := AppKeepAlive{
		Label:         "com.example.holder",
		AppPath:       "/Apps/a&b<c>.app",
		RestartPolicy: RestartAlways,
	}
	body, err := k.plist()
	if err != nil {
		t.Fatalf("plist() = %v", err)
	}
	s := string(body)
	if !strings.Contains(s, "<string>/Apps/a&amp;b&lt;c&gt;.app</string>") {
		t.Errorf("app path not XML-escaped:\n%s", s)
	}
	if strings.Contains(s, "a&b<c>") {
		t.Errorf("raw unescaped app path leaked into the plist:\n%s", s)
	}
}

func TestAppKeepAliveValidation(t *testing.T) {
	cases := []struct {
		name    string
		agent   AppKeepAlive
		wantErr string
	}{
		{"empty label", AppKeepAlive{AppPath: "/Applications/x.app"}, "Label is required"},
		{"relative app path", AppKeepAlive{Label: "com.example.x", AppPath: "x.app"}, "must be an absolute"},
		{"empty app path", AppKeepAlive{Label: "com.example.x"}, "must be an absolute"},
		{
			"empty restart policy",
			AppKeepAlive{Label: "com.example.x", AppPath: "/Applications/x.app"},
			"restart policy is required",
		},
		{
			"invalid restart policy",
			AppKeepAlive{
				Label:         "com.example.x",
				AppPath:       "/Applications/x.app",
				RestartPolicy: RestartPolicy(99),
			},
			"invalid restart policy 99",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.agent.plist(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("plist() err = %v, want it to contain %q", err, tc.wantErr)
			}
			if _, err := tc.agent.WritePlist(); err == nil {
				t.Fatal("WritePlist() accepted an invalid agent")
			}
		})
	}
}

type taskRunnerFunc func(context.Context, worker.CommandRequest) (worker.CommandResult, error)

func (f taskRunnerFunc) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	return f(ctx, task)
}

func launchctlRunner(fn func(context.Context, ...string) (string, error)) taskRunner {
	return taskRunnerFunc(func(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
		output, err := fn(ctx, task.Args...)
		return worker.CommandResult{Stdout: []byte(output)}, err
	})
}

func shExit(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("/bin/sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != code {
		t.Fatalf("fabricate exit %d: %v", code, err)
	}
	return err
}

func TestAppKeepAliveUninstallBootout(t *testing.T) {
	errDenied := errors.New("bootout: Operation not permitted")
	cases := []struct {
		name       string
		bootoutErr error
		wantGone   bool
	}{
		{"exit 3 not loaded succeeds and removes plist", shExit(t, 3), true},
		{"other exit code fails and keeps plist", shExit(t, 5), false},
		{"non-exit failure fails and keeps plist", errDenied, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			k := AppKeepAlive{
				Label:         "com.example.holder",
				AppPath:       "/Applications/x.app",
				RestartPolicy: RestartAlways,
			}
			plist, err := k.WritePlist()
			if err != nil {
				t.Fatalf("WritePlist() = %v", err)
			}
			var gotArgs []string
			k.runner = launchctlRunner(func(_ context.Context, args ...string) (string, error) {
				gotArgs = args
				return "launchctl output", tc.bootoutErr
			})
			err = k.Uninstall(context.Background())
			if want := []string{"bootout", serviceTarget(k.Label)}; !slices.Equal(gotArgs, want) {
				t.Errorf("launchctl args = %q, want %q", gotArgs, want)
			}
			_, statErr := os.Stat(plist)
			if tc.wantGone {
				if err != nil {
					t.Fatalf("Uninstall() = %v, want nil", err)
				}
				if !os.IsNotExist(statErr) {
					t.Errorf("plist not removed: stat err = %v", statErr)
				}
				return
			}
			if !errors.Is(err, tc.bootoutErr) {
				t.Fatalf("Uninstall() = %v, want errors.Is-wrapped %v", err, tc.bootoutErr)
			}
			if statErr != nil {
				t.Errorf("plist removed despite bootout failure: stat err = %v", statErr)
			}
		})
	}
}

func TestAppKeepAliveInstallEnableBeforeBootstrap(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	k := AppKeepAlive{
		Label:         "com.example.holder",
		AppPath:       "/Applications/x.app",
		RestartPolicy: RestartAlways,
	}

	var verbs []string
	k.runner = launchctlRunner(func(_ context.Context, args ...string) (string, error) {
		verbs = append(verbs, args[0])
		return "", nil
	})
	if err := k.Install(context.Background()); err != nil {
		t.Fatalf("Install() = %v", err)
	}
	if want := []string{"bootout", "enable", "bootstrap", "kickstart"}; !slices.Equal(verbs, want) {
		t.Errorf("launchctl verbs = %q, want %q", verbs, want)
	}

	errDisabled := errors.New("enable: Input/output error")
	verbs = nil
	k.runner = launchctlRunner(func(_ context.Context, args ...string) (string, error) {
		verbs = append(verbs, args[0])
		if args[0] == "enable" {
			return "", errDisabled
		}
		return "", nil
	})
	if err := k.Install(context.Background()); !errors.Is(err, errDisabled) {
		t.Fatalf("Install() = %v, want errors.Is-wrapped %v", err, errDisabled)
	}
	if want := []string{"bootout", "enable"}; !slices.Equal(verbs, want) {
		t.Errorf("launchctl verbs after enable failure = %q, want %q", verbs, want)
	}
}

func TestAppKeepAliveWritePlist(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	k := AppKeepAlive{
		Label:         "com.yasyf.fusekit-holder",
		AppPath:       "/Applications/fusekit-holder.app",
		RestartPolicy: RestartAlways,
	}
	path, err := k.WritePlist()
	if err != nil {
		t.Fatalf("WritePlist() = %v", err)
	}
	if want := filepath.Join(home, "Library", "LaunchAgents", "com.yasyf.fusekit-holder.plist"); path != want {
		t.Errorf("plist path = %q, want %q", path, want)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if string(body) != keepAliveGolden {
		t.Fatalf("written plist differs from the golden artifact:\n%s", body)
	}
}
