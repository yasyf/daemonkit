// Package service installs a consumer's daemon as a macOS user LaunchAgent —
// per-user, not root, so it can reach the login Keychain — and reconciles
// with a Homebrew-managed install. Generic launchctl/brew choreography.
package service

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/yasyf/daemonkit/supervise"
)

const plistTemplateText = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
{{range .Args}}        <string>{{.}}</string>
{{end}}    </array>
    <key>RunAtLoad</key>
    <true/>
{{.Restart}}
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>{{.Log}}</string>
    <key>StandardErrorPath</key>
    <string>{{.Log}}</string>
    <key>EnvironmentVariables</key>
    <dict>
{{range .Env}}        <key>{{.Key}}</key>
        <string>{{.Value}}</string>
{{end}}    </dict>
</dict>
</plist>
`

var plistTemplate = template.Must(template.New("plist").Parse(plistTemplateText))

// <, >, and & are legal in APFS paths and would produce a plist launchctl rejects.
func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

type plistKV struct{ Key, Value string }

type plistData struct {
	Label   string
	Args    []string
	Log     string
	Env     []plistKV
	Restart string
}

// Agent is a consumer's background daemon as a macOS user LaunchAgent; the
// launchctl/brew mechanics are generic, the fields are what varies per
// consumer.
type Agent struct {
	// Label is the LaunchAgent label / reverse-DNS identifier naming the plist
	// and the launchctl service target. Required.
	Label string
	// Formula is the Homebrew formula name used to detect a brew-managed install.
	// Required for the brew methods; the launchctl methods ignore it.
	Formula string
	// Program is the absolute path launchd execs; empty means os.Executable,
	// deliberately WITHOUT EvalSymlinks so a Homebrew symlink stays a constant
	// launchd program path across `brew upgrade`.
	Program string
	// Args are the arguments passed after Program (e.g. {"daemon"}).
	Args []string
	// LogPath is where launchd points StandardOut/StandardError; its parent directory is created 0700.
	LogPath string
	// Env are EnvironmentVariables entries written into the plist. Keys are
	// emitted in sorted order so the rendered plist is reproducible.
	Env map[string]string
	// RestartPolicy defines when launchd restarts the daemon. Required.
	RestartPolicy RestartPolicy
	// Runner owns every external service command as a disposable process group.
	Runner supervise.TaskRunner
}

// PlistPath is the LaunchAgent plist location (~/Library/LaunchAgents/<Label>.plist).
func (a Agent) PlistPath() (string, error) {
	return plistPath(a.Label)
}

func plistPath(label string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist"), nil
}

// WritePlist renders and writes the LaunchAgent plist, returning the path
// written; every interpolated value is XML-escaped.
func (a Agent) WritePlist() (string, error) {
	restart, err := a.RestartPolicy.plist()
	if err != nil {
		return "", fmt.Errorf("render restart policy: %w", err)
	}
	bin := a.Program
	if bin == "" {
		exe, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("resolve executable: %w", err)
		}
		bin = exe
	}
	path, err := a.PlistPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("ensure LaunchAgents dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(a.LogPath), 0o700); err != nil {
		return "", fmt.Errorf("ensure log dir: %w", err)
	}
	args := make([]string, 0, len(a.Args)+1)
	args = append(args, xmlEscape(bin))
	for _, arg := range a.Args {
		args = append(args, xmlEscape(arg))
	}
	env := make([]plistKV, 0, len(a.Env))
	for _, k := range slices.Sorted(maps.Keys(a.Env)) {
		env = append(env, plistKV{Key: xmlEscape(k), Value: xmlEscape(a.Env[k])})
	}
	var buf bytes.Buffer
	if err := plistTemplate.Execute(&buf, plistData{
		Label:   xmlEscape(a.Label),
		Args:    args,
		Log:     xmlEscape(a.LogPath),
		Env:     env,
		Restart: restart,
	}); err != nil {
		return "", fmt.Errorf("render plist: %w", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}
	return path, nil
}

func domainTarget() string { return "gui/" + strconv.Itoa(os.Getuid()) }

func serviceTarget(label string) string { return domainTarget() + "/" + label }

func (a Agent) serviceTarget() string { return serviceTarget(a.Label) }

func (a Agent) launchctl(ctx context.Context, args ...string) (string, error) {
	return runCombined(ctx, a.Runner, "/bin/launchctl", args...)
}

// Install writes the plist and (re)bootstraps the agent so it runs now and at
// every login. Idempotent: an existing instance is booted out first.
func (a Agent) Install(ctx context.Context) error {
	if a.Runner == nil {
		return errors.New("service: disposable task runner is required")
	}
	plist, err := a.WritePlist()
	if err != nil {
		return err
	}
	_, _ = a.launchctl(ctx, "bootout", a.serviceTarget())
	if out, err := a.launchctl(ctx, "bootstrap", domainTarget(), plist); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	_, _ = a.launchctl(ctx, "enable", a.serviceTarget())
	// Plain kickstart (no -k) covers the loaded-but-not-running race and no-ops when already running.
	if out, err := a.launchctl(ctx, "kickstart", a.serviceTarget()); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// Uninstall boots out the agent and removes its plist. A missing plist is not
// an error.
func (a Agent) Uninstall(ctx context.Context) error {
	if a.Runner == nil {
		return errors.New("service: disposable task runner is required")
	}
	_, _ = a.launchctl(ctx, "bootout", a.serviceTarget())
	path, err := a.PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Loaded reports whether launchd currently knows about the agent.
func (a Agent) Loaded(ctx context.Context) bool {
	_, err := a.launchctl(ctx, "print", a.serviceTarget())
	return err == nil
}

// IsBrewManaged reports whether the running binary was installed via Homebrew. It
// inspects the executable path only (no shelling out).
func (a Agent) IsBrewManaged() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	if a.pathIsBrewManaged(exe) {
		return true
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		return a.pathIsBrewManaged(resolved)
	}
	return false
}

func (a Agent) pathIsBrewManaged(p string) bool {
	if strings.Contains(p, "/Cellar/"+a.Formula+"/") {
		return true
	}
	for _, prefix := range brewPrefixes() {
		if strings.HasPrefix(p, prefix+"/opt/"+a.Formula+"/") || p == filepath.Join(prefix, "bin", a.Formula) {
			return true
		}
	}
	return false
}

func brewPrefixes() []string {
	if v := os.Getenv("HOMEBREW_PREFIX"); v != "" {
		return []string{v}
	}
	return []string{"/opt/homebrew", "/usr/local"}
}

func (a Agent) brewLabel() string { return "homebrew.mxcl." + a.Formula }

func (a Agent) brewServices(ctx context.Context, action string) error {
	return runSplit(ctx, a.Runner, "brew", os.Stdout, os.Stderr, "services", action, a.Formula)
}

// BrewStart starts the daemon via `brew services` (installs the user agent).
func (a Agent) BrewStart(ctx context.Context) error { return a.brewServices(ctx, "start") }

// BrewStop stops and unloads the brew-managed agent.
func (a Agent) BrewStop(ctx context.Context) error { return a.brewServices(ctx, "stop") }

// BrewKickstart ensures the brew-managed daemon is running: `brew services
// start` only bootstraps the job, and a stop/start race can leave it
// loaded-but-never-running, so kick it explicitly.
func (a Agent) BrewKickstart(ctx context.Context) error {
	target := domainTarget() + "/" + a.brewLabel()
	if out, err := a.launchctl(ctx, "kickstart", target); err != nil {
		return fmt.Errorf("launchctl kickstart %s: %w: %s", target, err, out)
	}
	return nil
}

// BrewInfo returns `brew services info <formula>` output for status display.
func (a Agent) BrewInfo(ctx context.Context) (string, error) {
	out, err := runCombined(ctx, a.Runner, "brew", "services", "info", a.Formula)
	return strings.TrimSpace(out), err
}

// StatusLines is the management block a consumer's `service status` command
// prints: whether the daemon is Homebrew- or self-managed, plus detail when available.
func (a Agent) StatusLines(ctx context.Context) []string {
	if a.IsBrewManaged() {
		info, err := a.BrewInfo(ctx)
		return brewStatus(info, err == nil)
	}
	return []string{selfStatus(a.Loaded(ctx))}
}

func brewStatus(info string, infoOK bool) []string {
	lines := []string{"Management: Homebrew (brew services)"}
	if infoOK {
		lines = append(lines, info)
	}
	return lines
}

func selfStatus(loaded bool) string {
	return fmt.Sprintf("Management: self-managed LaunchAgent (loaded: %v)", loaded)
}

// BrewReinstall runs `brew reinstall <formula>`, streaming brew's output to out
// and errOut. Errors when Homebrew is absent or the reinstall fails.
func (a Agent) BrewReinstall(ctx context.Context, out, errOut io.Writer) error {
	return brewStream(ctx, a.Runner, out, errOut, "reinstall", a.Formula)
}

// InstallCask runs `brew install --cask <ref>`, streaming brew's output to out
// and errOut. ref may carry a tap, which brew auto-taps. Errors on failure.
func InstallCask(
	ctx context.Context,
	runner supervise.TaskRunner,
	ref string,
	out, errOut io.Writer,
) error {
	return brewStream(ctx, runner, out, errOut, "install", "-y", "--cask", ref)
}

func brewStream(
	ctx context.Context,
	runner supervise.TaskRunner,
	out, errOut io.Writer,
	args ...string,
) error {
	return runSplit(ctx, runner, "brew", out, errOut, args...)
}
