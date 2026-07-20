package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
)

const openPath = "/usr/bin/open"

const (
	appStopDeadline = 5 * time.Second
	appStopQuiet    = 250 * time.Millisecond
	appStopPoll     = 25 * time.Millisecond
)

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

// AppProcessReaper durably owns one exact app process through bounded termination.
type AppProcessReaper interface {
	Reap(context.Context) (proc.ReapResult, error)
	TrackIdentity(context.Context, proc.Identity) (proc.Record, error)
	Terminate(context.Context, proc.Record) error
}

// AppOwnedProcessRecovery settles durable child and process-group records left by the app.
type AppOwnedProcessRecovery interface {
	Reap(context.Context) (proc.ReapResult, error)
}

// AppStopSpec identifies the fixed signed app endpoint and durable process owner.
type AppStopSpec struct {
	Dial           wire.Dialer
	ExecutableName string
	CodeIdentity   trust.CodeIdentity
	// EntitlementPolicyDigest is the opaque digest bound by the prior
	// signed-side accepted session.
	EntitlementPolicyDigest [32]byte
	Reaper                  AppProcessReaper
	Dependents              AppOwnedProcessRecovery

	peerFromConn func(net.Conn) (wire.Peer, error)
	processes    func(string) ([]proc.Identity, error)
	checkPeer    func(wire.Peer, trust.CodeIdentity) error
	now          func() time.Time
	pause        func(context.Context, time.Duration) error
	deadline     time.Duration
	quiet        time.Duration
}

// AuthenticatedAppPeer is the exact kernel and signed-policy identity proven
// by a prior authenticated app session.
type AuthenticatedAppPeer struct {
	PID                     int
	UID                     int
	StartTime               string
	Boot                    string
	Executable              string
	AuditTokenDigest        [32]byte
	CodeIdentity            trust.CodeIdentity
	EntitlementPolicyDigest [32]byte
}

// NewAuthenticatedAppPeer binds one signed-side accepted identity to an exact
// daemon-facing process proof without exposing its entitlement policy.
func NewAuthenticatedAppPeer(accepted trust.AcceptedIdentity) AuthenticatedAppPeer {
	peer := accepted.Peer()
	return AuthenticatedAppPeer{
		PID: peer.PID, UID: peer.UID, StartTime: peer.StartTime, Boot: peer.Boot,
		Executable: peer.Executable, AuditTokenDigest: sha256.Sum256(peer.Audit),
		CodeIdentity:            accepted.CodeIdentity(),
		EntitlementPolicyDigest: accepted.EntitlementPolicyDigest(),
	}
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

// Stop settles prior durable workers, authenticates and terminates every exact
// fixed-app execution, withdraws launchd ownership, and requires a bounded
// quiet interval with both the service unloaded and the exact executable absent.
func (k AppKeepAlive) Stop(
	ctx context.Context,
	spec AppStopSpec,
	expected AuthenticatedAppPeer,
) (proc.ReapResult, error) {
	executable, err := k.validateStop(spec)
	if err != nil {
		return proc.ReapResult{}, err
	}
	if err := expected.validate(executable); err != nil {
		return proc.ReapResult{}, err
	}
	if expected.CodeIdentity != spec.CodeIdentity {
		return proc.ReapResult{}, errors.New("keepalive agent: authenticated app peer code identity changed")
	}
	if expected.EntitlementPolicyDigest != spec.EntitlementPolicyDigest {
		return proc.ReapResult{}, errors.New("keepalive agent: authenticated app peer trust requirement changed")
	}
	receipts, err := spec.Reaper.Reap(ctx)
	if err != nil {
		return proc.ReapResult{}, fmt.Errorf("reap preexisting fixed app workers: %w", err)
	}
	deadline := spec.timeNow().Add(spec.stopDeadline())
	var quietSince time.Time
	for {
		conn, err := spec.Dial(ctx)
		if err != nil && !appEndpointAbsent(err) {
			return receipts, fmt.Errorf("dial fixed app: %w", err)
		}
		processes, inspectErr := spec.executableProcesses(executable)
		if inspectErr != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return receipts, fmt.Errorf("inventory fixed app executable: %w", inspectErr)
		}
		if conn != nil {
			quietSince = time.Time{}
			peer, err := spec.peer(conn)
			if err != nil {
				_ = conn.Close()
				return receipts, fmt.Errorf("identify fixed app peer: %w", err)
			}
			if err := spec.check(peer); err != nil {
				_ = conn.Close()
				return receipts, fmt.Errorf("authenticate fixed app peer: %w", err)
			}
			if !expected.matches(peer) {
				_ = conn.Close()
				return receipts, errors.New("fixed app peer changed after authenticated proof")
			}
			record, err := spec.Reaper.TrackIdentity(ctx, peer.ProcessIdentity())
			if err != nil {
				_ = conn.Close()
				return receipts, fmt.Errorf("durably track fixed app: %w", err)
			}
			if _, err := k.ensureUnloaded(ctx); err != nil {
				_ = conn.Close()
				return receipts, err
			}
			closeErr := conn.Close()
			if err := spec.Reaper.Terminate(ctx, record); err != nil {
				return receipts, fmt.Errorf("terminate fixed app: %w", err)
			}
			if closeErr != nil {
				return receipts, fmt.Errorf("close fixed app session: %w", closeErr)
			}
			continue
		}

		if len(processes) == 0 {
			reloaded, err := k.ensureUnloaded(ctx)
			if err != nil {
				return receipts, err
			}
			if reloaded {
				quietSince = time.Time{}
			}
			now := spec.timeNow()
			if quietSince.IsZero() {
				quietSince = now
			} else if now.Sub(quietSince) >= spec.stopQuiet() {
				dependentReceipts, err := spec.Dependents.Reap(ctx)
				if err != nil {
					return receipts, fmt.Errorf("reap fixed app dependents: %w", err)
				}
				receipts.Receipts = append(receipts.Receipts, dependentReceipts.Receipts...)
				receipts.More = receipts.More || dependentReceipts.More
				return receipts, nil
			}
		} else {
			quietSince = time.Time{}
		}
		if !spec.timeNow().Before(deadline) {
			return receipts, fmt.Errorf("fixed app did not settle before deadline: %d exact executable process(es) remain", len(processes))
		}
		if err := spec.wait(ctx, appStopPoll); err != nil {
			return receipts, err
		}
	}
}

func (p AuthenticatedAppPeer) validate(executable string) error {
	if p.PID <= 1 || p.UID < 0 || p.StartTime == "" || p.Boot == "" || p.Executable != executable ||
		p.AuditTokenDigest == ([32]byte{}) || p.CodeIdentity == (trust.CodeIdentity{}) ||
		p.EntitlementPolicyDigest == ([32]byte{}) {
		return errors.New("keepalive agent: authenticated app peer proof is incomplete")
	}
	if _, err := p.CodeIdentity.DRString(); err != nil {
		return fmt.Errorf("keepalive agent: authenticated app peer code identity: %w", err)
	}
	return nil
}

func (p AuthenticatedAppPeer) matches(peer wire.Peer) bool {
	return p.PID == peer.PID && p.UID == peer.UID && p.StartTime == peer.StartTime &&
		p.Boot == peer.Boot && p.Executable == peer.Executable &&
		p.AuditTokenDigest == sha256.Sum256(peer.Audit)
}

func (k AppKeepAlive) validateStop(spec AppStopSpec) (string, error) {
	if err := k.validate(); err != nil {
		return "", err
	}
	if spec.Dial == nil || spec.Reaper == nil || spec.Dependents == nil {
		return "", errors.New("keepalive agent: app stop requires a dialer, durable reaper, and dependent recovery")
	}
	if spec.EntitlementPolicyDigest == ([32]byte{}) {
		return "", errors.New("keepalive agent: app stop entitlement policy digest is required")
	}
	if filepath.Base(spec.ExecutableName) != spec.ExecutableName || spec.ExecutableName == "." || spec.ExecutableName == "" {
		return "", errors.New("keepalive agent: app stop executable name is invalid")
	}
	if k.BundleID == "" || spec.CodeIdentity.SigningIdentifier != k.BundleID {
		return "", errors.New("keepalive agent: app stop signing identifier must equal BundleID")
	}
	if _, err := spec.CodeIdentity.DRString(); err != nil {
		return "", fmt.Errorf("keepalive agent: app stop code identity: %w", err)
	}
	executable := filepath.Join(k.AppPath, "Contents", "MacOS", spec.ExecutableName)
	if err := validateDirectAppPath(k.AppPath, executable); err != nil {
		return "", err
	}
	return executable, nil
}

func validateDirectAppPath(appPath, executable string) error {
	clean := filepath.Clean(executable)
	current := string(filepath.Separator)
	for _, element := range strings.Split(strings.TrimPrefix(clean, current), string(filepath.Separator)) {
		current = filepath.Join(current, element)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("keepalive agent: inspect fixed app path %s: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("keepalive agent: fixed app path %s is a symlink", current)
		}
	}
	app, err := os.Lstat(appPath)
	if err != nil {
		return fmt.Errorf("keepalive agent: inspect fixed app path %s: %w", appPath, err)
	}
	if !app.IsDir() {
		return fmt.Errorf("keepalive agent: fixed app path %s is not a directory", appPath)
	}
	info, err := os.Lstat(executable)
	if err != nil {
		return fmt.Errorf("keepalive agent: inspect fixed app executable %s: %w", executable, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("keepalive agent: fixed app executable %s is not a regular file", executable)
	}
	return nil
}

func (s AppStopSpec) peer(conn net.Conn) (wire.Peer, error) {
	if s.peerFromConn != nil {
		return s.peerFromConn(conn)
	}
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return wire.Peer{}, errors.New("fixed app dial did not return a Unix connection")
	}
	return wire.PeerFromConn(unix)
}

func (s AppStopSpec) executableProcesses(executable string) ([]proc.Identity, error) {
	if s.processes != nil {
		return s.processes(executable)
	}
	return proc.ExecutableIdentities(executable)
}

func (s AppStopSpec) check(peer wire.Peer) error {
	if s.checkPeer != nil {
		return s.checkPeer(peer, s.CodeIdentity)
	}
	return (trust.CodePolicy{Identity: s.CodeIdentity}).Check(peer)
}

func (k AppKeepAlive) bootout(ctx context.Context) error {
	if out, err := launchctl(ctx, "bootout", serviceTarget(k.Label)); err != nil && !notLoaded(err) {
		return fmt.Errorf("launchctl bootout: %w: %s", err, out)
	}
	return nil
}

func (k AppKeepAlive) ensureUnloaded(ctx context.Context) (bool, error) {
	loaded, err := k.loaded(ctx)
	if err != nil {
		return false, err
	}
	observedLoaded := loaded
	if loaded {
		if err := k.bootout(ctx); err != nil {
			return false, err
		}
	}
	loaded, err = k.loaded(ctx)
	if err != nil {
		return false, err
	}
	if loaded {
		return false, errors.New("keepalive agent remained loaded after bootout")
	}
	return observedLoaded, nil
}

func (k AppKeepAlive) loaded(ctx context.Context) (bool, error) {
	out, err := launchctl(ctx, "print", serviceTarget(k.Label))
	if err == nil {
		return true, nil
	}
	if notLoaded(err) {
		return false, nil
	}
	return false, fmt.Errorf("launchctl print: %w: %s", err, out)
}

func (s AppStopSpec) timeNow() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s AppStopSpec) wait(ctx context.Context, duration time.Duration) error {
	if s.pause != nil {
		return s.pause(ctx, duration)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s AppStopSpec) stopDeadline() time.Duration {
	if s.deadline > 0 {
		return s.deadline
	}
	return appStopDeadline
}

func (s AppStopSpec) stopQuiet() time.Duration {
	if s.quiet > 0 {
		return s.quiet
	}
	return appStopQuiet
}

func appEndpointAbsent(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED)
}

// Uninstall boots out the agent and removes its plist; the app keeps running
// (bootout kills the open waiter, not the app). A bootout failure other than
// "not loaded" aborts before the plist is removed.
func (k AppKeepAlive) Uninstall(ctx context.Context) error {
	if err := k.validate(); err != nil {
		return err
	}
	if err := k.bootout(ctx); err != nil {
		return err
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
