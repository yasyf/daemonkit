package launchd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/realhome"
)

func okRunner(context.Context, string, ...string) (string, int, error) { return "", 0, nil }

func noWait(context.Context, time.Duration) error { return nil }

func TestReloadRetriesOnlyInFlux(t *testing.T) {
	tests := []struct {
		name            string
		bootstrap       func(attempt int) (string, int, error)
		wantErr         bool
		wantErrContains string
		wantAttempts    int
	}{
		{
			name: "gives up after retrying persistent in-flux",
			bootstrap: func(int) (string, int, error) {
				return "Bootstrap failed: 37: Operation already in progress", launchctlAlreadyExit, launchctlErr(37)
			},
			wantErr: true, wantErrContains: "gave up after 3 attempts", wantAttempts: bootstrapAttempts,
		},
		{
			name: "succeeds after transient in-flux",
			bootstrap: func(attempt int) (string, int, error) {
				if attempt < bootstrapAttempts {
					return "Boot-out failed: 36: Operation now in progress", launchctlInProgressExit, launchctlErr(36)
				}
				return "", 0, nil
			},
			wantErr: false, wantAttempts: bootstrapAttempts,
		},
		{
			name: "does not retry a refusal",
			bootstrap: func(int) (string, int, error) {
				return "Bootstrap failed: 1: Operation not permitted", 1, launchctlErr(1)
			},
			wantErr: true, wantErrContains: "launchd refused", wantAttempts: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempts := 0
			run := func(_ context.Context, _ string, args ...string) (string, int, error) {
				if args[0] == "bootstrap" {
					attempts++
					return test.bootstrap(attempts)
				}
				return "", 0, nil
			}
			c := converger{run: run, wait: noWait}
			err := c.reload(context.Background(), Agent{Label: "com.example.worker"}, "/x.plist")
			if (err != nil) != test.wantErr {
				t.Fatalf("reload err = %v, want error=%t", err, test.wantErr)
			}
			if test.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), test.wantErrContains)) {
				t.Fatalf("reload err = %v, want it to contain %q", err, test.wantErrContains)
			}
			if attempts != test.wantAttempts {
				t.Fatalf("bootstrap attempts = %d, want %d", attempts, test.wantAttempts)
			}
		})
	}
}

func launchAgentsDir(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv(realhome.EnvOverride, home)
	dir := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func convergeAgent(t *testing.T, label string) Agent {
	t.Helper()
	return Agent{
		Label:         label,
		Program:       "/usr/bin/true",
		LogPath:       filepath.Join(t.TempDir(), label+".log"),
		RestartPolicy: RestartAlways,
	}
}

func TestConvergeAdoptsPreCutMarkerlessAgent(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := convergeAgent(t, "com.example.worker")
	plistFile := filepath.Join(dir, agent.Label+".plist")
	preCut := "<?xml version=\"1.0\"?>\n<plist><dict><key>Label</key><string>com.example.worker</string></dict></plist>\n"
	if err := os.WriteFile(plistFile, []byte(preCut), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Converge(context.Background(), okRunner, []Agent{agent}); err != nil {
		t.Fatalf("Converge: %v", err)
	}

	got, err := os.ReadFile(plistFile)
	if err != nil {
		t.Fatal(err)
	}
	want, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("adopted plist was not rewritten with the marker\n%s", got)
	}
	backups, err := filepath.Glob(plistFile + ".daemonkit-archived-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 1 {
		t.Fatalf("markerless plist not archived aside: %v", backups)
	}
	archived, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(archived) != preCut {
		t.Fatalf("archived backup does not preserve the pre-cut plist\n%s", archived)
	}
}

func TestConvergeInstallsFreshAgentWithoutArchiving(t *testing.T) {
	dir := launchAgentsDir(t)
	agent := convergeAgent(t, "com.example.fresh")
	plistFile := filepath.Join(dir, agent.Label+".plist")

	if err := Converge(context.Background(), okRunner, []Agent{agent}); err != nil {
		t.Fatalf("Converge: %v", err)
	}

	got, err := os.ReadFile(plistFile)
	if err != nil {
		t.Fatalf("fresh agent plist not installed: %v", err)
	}
	if !plistHasOwnerMarker(got) {
		t.Fatalf("installed plist lacks the ownership marker\n%s", got)
	}
	backups, err := filepath.Glob(plistFile + ".daemonkit-archived-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 0 {
		t.Fatalf("fresh install archived a nonexistent plist: %v", backups)
	}
}

func TestConvergeRemovesStaleMarkedAgent(t *testing.T) {
	dir := launchAgentsDir(t)
	desired := convergeAgent(t, "com.example.keep")
	stale := convergeAgent(t, "com.example.stale")
	stalePlist, err := stale.Plist()
	if err != nil {
		t.Fatal(err)
	}
	staleFile := filepath.Join(dir, stale.Label+".plist")
	if err := os.WriteFile(staleFile, stalePlist, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Converge(context.Background(), okRunner, []Agent{desired}); err != nil {
		t.Fatalf("Converge: %v", err)
	}

	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Fatalf("stale marked agent plist survived convergence: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, desired.Label+".plist")); err != nil {
		t.Fatalf("desired agent was not installed: %v", err)
	}
}

func TestConvergeRequiresRunner(t *testing.T) {
	if err := Converge(context.Background(), nil, nil); err == nil || !strings.Contains(err.Error(), "runner is required") {
		t.Fatalf("Converge with nil runner = %v, want a runner-required error", err)
	}
}
