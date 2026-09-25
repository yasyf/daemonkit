//go:build darwin

package launchd

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/yasyf/daemonkit/internal/proc"
)

// ErrMaintenanceUnproven means a no-signal restart exclusion was not established.
var ErrMaintenanceUnproven = errors.New("launchd: maintenance exclusion is unproven")

// Maintenance retains the original enablement of one exact loaded service.
// Disabling changes no running process; restoration never loads or kickstarts it.
type Maintenance struct {
	client   applier
	label    string
	pid      int
	enabled  bool
	identity proc.Identity
	plist    [32]byte
}

var (
	loadedPID        = regexp.MustCompile(`(?m)^\tpid = ([0-9]+)$`)
	loadedProperties = regexp.MustCompile(`(?m)^\tproperties = (.+)$`)
)

func (c applier) loadedIdentity(ctx context.Context, label string) (int, error) {
	result := c.launchctl(ctx, "print", serviceTarget(label))
	if err := result.fail(); err != nil {
		return 0, err
	}
	if !strings.HasPrefix(result.out, serviceTarget(label)+" = {") {
		return 0, ErrMaintenanceUnproven
	}
	matches := loadedPID.FindAllStringSubmatch(result.out, -1)
	if len(matches) != 1 || len(loadedProperties.FindAllStringSubmatch(result.out, -1)) != 1 {
		return 0, ErrMaintenanceUnproven
	}
	pid, err := strconv.Atoi(matches[0][1])
	if err != nil || pid <= 0 {
		return 0, ErrMaintenanceUnproven
	}
	return pid, nil
}

func (c applier) disabled(ctx context.Context, label string) (bool, error) {
	result := c.launchctl(ctx, "print-disabled", domainTarget())
	if err := result.fail(); err != nil {
		return false, err
	}
	if !strings.Contains(result.out, "disabled services = {") {
		return false, ErrMaintenanceUnproven
	}
	pattern := regexp.MustCompile(`(?m)^\s*"` + regexp.QuoteMeta(label) + `"\s*=>\s*(true|false)\s*$`)
	matches := pattern.FindAllStringSubmatch(result.out, -1)
	if len(matches) > 1 {
		return false, ErrMaintenanceUnproven
	}
	return len(matches) == 1 && matches[0][1] == "true", nil
}

// PauseRestarts disables one owned, currently loaded job without stopping it.
// Its PID is compared before and after the change; callers must also recheck
// their authenticated process-generation pin before committing a daemon drain.
func PauseRestarts(ctx context.Context, run Runner, label string, pid int) (*Maintenance, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("launchd: maintenance requires a deadline")
	}
	if err := ValidateLabel(label); err != nil {
		return nil, err
	}
	path, err := plistPath(label)
	if err != nil {
		return nil, err
	}
	plist, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !plistHasOwnerMarker(plist) || strings.Contains(string(plist), "<key>Disabled</key>") {
		return nil, ErrMaintenanceUnproven
	}
	if run == nil {
		return nil, errors.New("launchd: maintenance runner is required")
	}
	c := applier{run: run}
	actual, err := c.loadedIdentity(ctx, label)
	if err != nil {
		return nil, err
	}
	if pid < 0 || (pid != 0 && actual != pid) {
		return nil, ErrMaintenanceUnproven
	}
	disabled, err := c.disabled(ctx, label)
	if err != nil {
		return nil, err
	}
	pid = actual
	identity, err := proc.ProbeIdentity(pid)
	if err != nil {
		return nil, err
	}
	lease := &Maintenance{client: c, label: label, pid: pid, enabled: !disabled, identity: identity, plist: sha256.Sum256(plist)}
	if !disabled {
		result := c.launchctl(ctx, "disable", serviceTarget(label))
		if err := result.fail(); err != nil {
			return lease, err
		}
	}
	if err := lease.unchanged(ctx); err != nil {
		return lease, err
	}
	disabled, err = c.disabled(ctx, label)
	if err != nil || !disabled {
		return lease, fmt.Errorf("%w: disable state did not converge: %v", ErrMaintenanceUnproven, err)
	}
	actual, err = c.loadedIdentity(ctx, label)
	if err != nil || actual != pid {
		return lease, fmt.Errorf("%w: loaded identity changed: %v", ErrMaintenanceUnproven, err)
	}
	return lease, nil
}

// Restore reinstates the prior enablement only while the same loaded PID remains.
// It sends no stop, bootstrap, or kickstart operation.
func (m *Maintenance) Restore(ctx context.Context) error {
	if err := m.unchanged(ctx); err != nil {
		return err
	}
	if !m.enabled {
		disabled, err := m.client.disabled(ctx, m.label)
		if err != nil || !disabled {
			return fmt.Errorf("%w: original disabled state changed: %v", ErrMaintenanceUnproven, err)
		}
		return nil
	}
	actual, err := m.client.loadedIdentity(ctx, m.label)
	if err != nil || actual != m.pid {
		return fmt.Errorf("%w: restoration identity differs: %v", ErrMaintenanceUnproven, err)
	}
	result := m.client.launchctl(ctx, "enable", serviceTarget(m.label))
	if err := result.fail(); err != nil {
		return err
	}
	disabled, err := m.client.disabled(ctx, m.label)
	if err != nil || disabled {
		return fmt.Errorf("%w: enable state did not converge: %v", ErrMaintenanceUnproven, err)
	}
	return nil
}

func (m *Maintenance) unchanged(ctx context.Context) error {
	_, gone, err := proc.Observe(m.identity)
	if err != nil || gone {
		return fmt.Errorf("%w: loaded process changed: %v", ErrMaintenanceUnproven, err)
	}
	pid, err := m.client.loadedIdentity(ctx, m.label)
	if err != nil || pid != m.pid {
		return fmt.Errorf("%w: loaded label changed: %v", ErrMaintenanceUnproven, err)
	}
	path, err := plistPath(m.label)
	if err != nil {
		return err
	}
	plist, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if sha256.Sum256(plist) != m.plist {
		return fmt.Errorf("%w: owned plist changed", ErrMaintenanceUnproven)
	}
	return nil
}
