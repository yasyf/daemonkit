//go:build mixedera

// Package mixedera proves a daemon and client of different daemonkit releases
// share a session when only their build strings differ (RC3, ccn 0771ea54).
package mixedera

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	readyLine   = "READY"
	readyWait   = 60 * time.Second
	dialWait    = 90 * time.Second
	settleWait  = 60 * time.Second
	buildWait   = 5 * time.Minute
	tagOverride = "MIXED_ERA_TAG"
)

var releaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

type healthReport struct {
	WireBuild string `json:"wire_build"`
	Protocol  int    `json:"protocol"`
	PID       int    `json:"pid"`
}

type dialReport struct {
	SelfBuild string       `json:"self_build"`
	PeerBuild string       `json:"peer_build"`
	Protocol  uint16       `json:"protocol"`
	Health    healthReport `json:"health"`
	StopAcked bool         `json:"stop_acked"`
}

type syncBuffer struct {
	mu    sync.Mutex
	bytes []byte
}

func (b *syncBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bytes = append(b.bytes, payload...)
	return len(payload), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.bytes)
}

func TestMixedEraSessionSurvivesBuildSkew(t *testing.T) {
	root := repoRoot(t)
	tag := previousRelease(t, root)
	t.Logf("mixed-era boundary: working tree vs %s", tag)

	oldPeer := buildPeer(t, "old", "require github.com/yasyf/daemonkit "+tag)
	newPeer := buildPeer(t, "new", fmt.Sprintf(
		"require github.com/yasyf/daemonkit v0.0.0\n\nreplace github.com/yasyf/daemonkit => %s", root,
	))

	oldBuild := "mixedera.consumer." + tag
	newBuild := "mixedera.consumer.tree"

	tests := []struct {
		name        string
		daemon      string
		daemonBuild string
		client      string
		clientBuild string
	}{
		{"control/old-daemon-new-client", oldPeer, oldBuild, newPeer, oldBuild},
		{"control/new-daemon-old-client", newPeer, newBuild, oldPeer, newBuild},
		{"skew/new-daemon-old-client", newPeer, newBuild, oldPeer, oldBuild},
		{"skew/old-daemon-new-client", oldPeer, oldBuild, newPeer, newBuild},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := exchange(t, tt.daemon, tt.daemonBuild, tt.client, tt.clientBuild)
			if report.PeerBuild != tt.daemonBuild {
				t.Errorf("handshake peer build = %q, want %q", report.PeerBuild, tt.daemonBuild)
			}
			if report.SelfBuild != tt.clientBuild {
				t.Errorf("handshake self build = %q, want %q", report.SelfBuild, tt.clientBuild)
			}
			if report.Protocol != 1 {
				t.Errorf("handshake protocol = %d, want 1", report.Protocol)
			}
			if report.Health.WireBuild != tt.daemonBuild {
				t.Errorf("health build = %q, want %q", report.Health.WireBuild, tt.daemonBuild)
			}
			if report.Health.Protocol != 1 {
				t.Errorf("health protocol = %d, want 1", report.Health.Protocol)
			}
			if !report.StopAcked {
				t.Error("stop was not acknowledged")
			}
		})
	}
}

func exchange(t *testing.T, daemonBin, daemonBuild, clientBin, clientBuild string) dialReport {
	t.Helper()
	dir := socketDir(t)
	socket := filepath.Join(dir, "d.sock")

	daemon := exec.CommandContext(t.Context(), daemonBin,
		"serve", "-socket", socket, "-build", daemonBuild, "-state", dir)
	daemon.WaitDelay = 5 * time.Second
	stdout, err := daemon.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var daemonLog syncBuffer
	daemon.Stderr = &daemonLog
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		if joined {
			return
		}
		_ = daemon.Process.Kill()
		<-exited
	})
	awaitReady(t, stdout, exited, daemon, &daemonLog)

	client := exec.CommandContext(t.Context(), clientBin,
		"dial", "-socket", socket, "-build", clientBuild)
	client.WaitDelay = 5 * time.Second
	clientOut, clientErr := run(t, client, dialWait)
	if clientErr != nil {
		t.Fatalf("client %s dialing daemon %s: %v\ndaemon stderr:\n%s",
			clientBuild, daemonBuild, clientErr, daemonLog.String())
	}

	var report dialReport
	if err := json.Unmarshal([]byte(clientOut), &report); err != nil {
		t.Fatalf("decode client report %q: %v", clientOut, err)
	}

	select {
	case err := <-exited:
		joined = true
		if err != nil {
			t.Fatalf("daemon %s exited %v\nstderr:\n%s", daemonBuild, err, daemonLog.String())
		}
	case <-time.After(settleWait):
		t.Fatalf("daemon %s did not settle after stop\nstderr:\n%s", daemonBuild, daemonLog.String())
	}
	return report
}

func awaitReady(t *testing.T, stdout io.Reader, exited chan error, daemon *exec.Cmd, log *syncBuffer) {
	t.Helper()
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) == readyLine {
				close(ready)
				break
			}
		}
		_, _ = io.Copy(io.Discard, stdout)
		exited <- daemon.Wait()
	}()
	select {
	case <-ready:
	case err := <-exited:
		exited <- err
		t.Fatalf("daemon exited before %s: %v\nstderr:\n%s", readyLine, err, log.String())
	case <-time.After(readyWait):
		t.Fatalf("daemon did not report %s within %s\nstderr:\n%s", readyLine, readyWait, log.String())
	}
}

func run(t *testing.T, cmd *exec.Cmd, wait time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	var stdout, stderr syncBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return stdout.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return stdout.String(), nil
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-done
		return stdout.String(), fmt.Errorf("timed out after %s: %s", wait, strings.TrimSpace(stderr.String()))
	}
}

func buildPeer(t *testing.T, era, directive string) string {
	t.Helper()
	dir := t.TempDir()
	source, err := os.ReadFile(filepath.Join("testdata", "peer", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), source, 0o644); err != nil {
		t.Fatal(err)
	}
	manifest := fmt.Sprintf("module mixedera/peer/%s\n\ngo %s\n\n%s\n", era, goDirective(t), directive)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "peer")
	for _, args := range [][]string{{"mod", "tidy"}, {"build", "-o", binary, "."}} {
		cmd := exec.CommandContext(t.Context(), "go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off")
		if out, err := run(t, cmd, buildWait); err != nil {
			t.Fatalf("%s era: go %s: %v\n%s", era, strings.Join(args, " "), err, out)
		}
	}
	return binary
}

func previousRelease(t *testing.T, root string) string {
	t.Helper()
	if tag := os.Getenv(tagOverride); tag != "" {
		return tag
	}
	head := git(t, root, "rev-parse", "HEAD^{commit}")
	for _, tag := range strings.Split(git(t, root, "tag", "--sort=-creatordate"), "\n") {
		if !releaseTag.MatchString(tag) {
			continue
		}
		if gitRun(t, root, "merge-base", "--is-ancestor", tag, "HEAD") != nil {
			continue
		}
		if git(t, root, "rev-list", "-n", "1", tag) == head {
			continue
		}
		return tag
	}
	t.Fatalf("no released tag precedes HEAD; set %s to name one", tagOverride)
	return ""
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func goDirective(t *testing.T) string {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(repoRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(manifest)) {
		if directive, ok := strings.CutPrefix(strings.TrimSpace(line), "go "); ok {
			return strings.TrimSpace(directive)
		}
	}
	t.Fatal("go.mod declares no go directive")
	return ""
}

// socketDir mirrors wiretest.SocketDir: macOS caps sun_path at 104 bytes,
// which t.TempDir routinely exceeds.
func socketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", fmt.Sprintf("dk-mixedera-%d-", os.Getpid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func git(t *testing.T, root string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, exit.Stderr)
		}
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}

func gitRun(t *testing.T, root string, args ...string) error {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = root
	return cmd.Run()
}
