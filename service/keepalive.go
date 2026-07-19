package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const openPath = "/usr/bin/open"

// AppKeepAlive is a per-user KeepAlive LaunchAgent whose Program is
// `/usr/bin/open -g -W <app>`: -W blocks until the app exits AND attaches to
// a running instance, so launchd relaunches only on a real exit and never
// spins against a live holder.
type AppKeepAlive struct {
	// Label is the LaunchAgent label / reverse-DNS identifier naming the plist
	// and the launchctl service target. Required.
	Label string
	// AppPath is the absolute .app bundle path open launches. Required.
	AppPath string
	// BundleID, when set, adds an AssociatedBundleIdentifiers entry so launchd
	// attributes this agent to the app bundle; empty omits the key.
	BundleID string
	// RestartPolicy defines when launchd restarts the app waiter. Required.
	RestartPolicy RestartPolicy
}

const keepAlivePlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%s</string>
    <key>Program</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>-g</string>
        <string>-W</string>
        <string>%s</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
%s%s</dict>
</plist>
`

// Rendered only when BundleID is set, so the no-bundle plist stays byte-identical to the donor.
const assocBundleBlock = `    <key>AssociatedBundleIdentifiers</key>
    <array>
        <string>%s</string>
    </array>
`

func (k AppKeepAlive) validate() error {
	if k.Label == "" {
		return errors.New("keepalive agent: Label is required")
	}
	if !filepath.IsAbs(k.AppPath) {
		return fmt.Errorf("keepalive agent: AppPath %q must be an absolute .app bundle path", k.AppPath)
	}
	if _, err := k.RestartPolicy.plist(); err != nil {
		return fmt.Errorf("keepalive agent: %w", err)
	}
	return nil
}

func (k AppKeepAlive) plist() ([]byte, error) {
	if err := k.validate(); err != nil {
		return nil, err
	}
	label, app := xmlEscape(k.Label), xmlEscape(k.AppPath)
	restart, err := k.RestartPolicy.plist()
	if err != nil {
		return nil, fmt.Errorf("keepalive agent: %w", err)
	}
	assoc := ""
	if k.BundleID != "" {
		assoc = fmt.Sprintf(assocBundleBlock, xmlEscape(k.BundleID))
	}
	return fmt.Appendf(nil, keepAlivePlist, label, openPath, openPath, app, restart, assoc), nil
}

// PlistPath is the LaunchAgent plist location (~/Library/LaunchAgents/<Label>.plist).
func (k AppKeepAlive) PlistPath() (string, error) {
	return plistPath(k.Label)
}

// WritePlist renders and writes the LaunchAgent plist, returning the path written.
func (k AppKeepAlive) WritePlist() (string, error) {
	body, err := k.plist()
	if err != nil {
		return "", err
	}
	path, err := k.PlistPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("ensure LaunchAgents dir: %w", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}
	return path, nil
}

// Install writes the plist and (re)bootstraps the agent so the app runs now
// and at every login. Bootout kills only the blocked open waiter, and the
// fresh open attaches via -W instead of starting a second copy.
func (k AppKeepAlive) Install(ctx context.Context) error {
	plist, err := k.WritePlist()
	if err != nil {
		return err
	}
	_, _ = launchctl(ctx, "bootout", serviceTarget(k.Label))
	// enable before bootstrap: it clears a user/MDM disable, and a disabled label fails bootstrap.
	if out, err := launchctl(ctx, "enable", serviceTarget(k.Label)); err != nil {
		return fmt.Errorf("launchctl enable: %w: %s", err, out)
	}
	if out, err := launchctl(ctx, "bootstrap", domainTarget(), plist); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	// Plain kickstart (no -k) covers the loaded-but-not-running race and no-ops when already running.
	if out, err := launchctl(ctx, "kickstart", serviceTarget(k.Label)); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// Uninstall boots out the agent and removes its plist; the app keeps running
// (bootout kills the open waiter, not the app). A bootout failure other than
// "not loaded" aborts before the plist is removed.
func (k AppKeepAlive) Uninstall(ctx context.Context) error {
	if err := k.validate(); err != nil {
		return err
	}
	if out, err := launchctl(ctx, "bootout", serviceTarget(k.Label)); err != nil && !notLoaded(err) {
		return fmt.Errorf("launchctl bootout: %w: %s", err, out)
	}
	path, err := k.PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// bootout exits 3 ("No such process") for an unloaded service target.
func notLoaded(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 3
}

// Loaded reports whether launchd currently knows about the agent.
func (k AppKeepAlive) Loaded(ctx context.Context) bool {
	_, err := launchctl(ctx, "print", serviceTarget(k.Label))
	return err == nil
}
