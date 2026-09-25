//go:build darwin

package launchd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceDisableBlocksRespawn(t *testing.T) {
	if os.Getenv("DAEMONKIT_MAINTENANCE_E2E") != "1" {
		t.Skip("disposable launchd fixture runs only in its CI step")
	}
	directory := t.TempDir()
	events := filepath.Join(directory, "events")
	label := fmt.Sprintf("com.daemonkit.ci.maintenance.%d.%d", os.Getpid(), time.Now().UnixNano())
	launchAgentsDir(t)
	agent := Agent{Label: label, Program: "/bin/sh", Args: []string{"-c", `trap 'printf "signal\n" >> "$1/events"' HUP INT TERM CONT; printf 'started\n' >> "$1/events"; while [ ! -f "$1/exit" ]; do /bin/sleep 0.05; done; exit 1`, "maintenance-fixture", directory}, LogPath: filepath.Join(directory, "service.log"), RestartPolicy: RestartAlways}
	run := func(ctx context.Context, path string, args ...string) (string, int, error) {
		command := exec.CommandContext(ctx, path, args...)
		output, err := command.CombinedOutput()
		code := 0
		if command.ProcessState != nil {
			code = command.ProcessState.ExitCode()
		}
		return string(output), code, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := Remove(cleanup, run, label); err != nil {
			t.Error(err)
		}
	})
	if err := Apply(ctx, run, agent); err != nil {
		t.Fatal(err)
	}
	client := applier{run: run}
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		pid, err = client.loadedIdentity(ctx, label)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if pid <= 0 {
		t.Fatal("fixture never acquired a loaded process identity")
	}
	for {
		data, readErr := os.ReadFile(events)
		if readErr == nil && string(data) == "started\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture signal handlers did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	lease, err := PauseRestarts(ctx, run, label, pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.unchanged(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "exit"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(12 * time.Second)
	data, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "started\n" {
		t.Fatalf("disable signalled or permitted restart: %q", data)
	}
	printed, code, err := run(ctx, launchctlPath, "print", serviceTarget(label))
	if err != nil || code != 0 {
		t.Fatalf("disabled loaded job disappeared unexpectedly: %d %v", code, err)
	}
	if strings.Contains(printed, "\tpid = ") {
		t.Fatalf("disabled job has a live process: %s", printed)
	}
	if !strings.Contains(printed, "runs = 1") {
		t.Fatalf("restart count not proven: pid=%s %s", strconv.Itoa(pid), printed)
	}
}
