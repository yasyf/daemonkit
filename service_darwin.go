package daemonkit

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/yasyf/daemonkit/internal/converge"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/launchd"
)

// service is the specification the platform's service layer runs a daemon
// from: on darwin, one LaunchAgent.
type service = launchd.Agent

// serviceLayer is how a Client reaches launchd.
type serviceLayer struct{ launchctl launchd.Runner }

func openServiceLayer() serviceLayer { return serviceLayer{launchctl: launchctl} }

func validateLabel(label string) error { return launchd.ValidateLabel(label) }

func (l serviceLayer) applyService(ctx context.Context, agent service) error {
	return launchd.Apply(ctx, l.launchctl, agent)
}

func (l serviceLayer) removeService(ctx context.Context, label string) error {
	return launchd.Remove(ctx, l.launchctl, label)
}

func (c *Client) observeWorld(ctx context.Context, agent launchd.Agent) (converge.World, error) {
	record, err := c.record()
	if err != nil {
		return converge.World{}, err
	}
	return converge.Observe(ctx, converge.Sources{
		Serving:    c.servedHealth,
		Recorded:   proc.ReadOwner,
		RecordPath: record,
		Agent:      agent,
		Launchctl:  c.launchctl,
	})
}

// AgentPath is the PATH every daemonkit LaunchAgent runs under. launchd's own
// default omits the Homebrew prefixes, so a daemon that execs `git` reaches the
// Xcode shim at /usr/bin/git, which re-execs Xcode's binary and pays a second
// endpoint-security exec check on every call; a per-machine `launchctl config
// user path` only applies after a reboot, and a job started before it keeps the
// bare default. Rendering the value into the plist makes the daemon's PATH a
// fact of the spec rather than of when the job was bootstrapped.
const AgentPath = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

// agent is the LaunchAgent that runs this daemon. Every field is already on
// the Daemon, so nothing about the job is declared twice; an unset Log sinks
// to the state directory's daemon.log.
func (d Daemon) agent() (launchd.Agent, error) {
	el, err := d.Label.element()
	if err != nil {
		return launchd.Agent{}, err
	}
	program, err := d.Program.path(el)
	if err != nil {
		return launchd.Agent{}, err
	}
	restart, err := d.Restart.launchd()
	if err != nil {
		return launchd.Agent{}, err
	}
	log := d.Log
	if log == "" {
		log = el.state().LogPath()
	}
	return launchd.Agent{
		Label:         el.label,
		Program:       program,
		Args:          d.Args,
		LogPath:       log,
		Env:           map[string]string{"PATH": AgentPath},
		RestartPolicy: restart,
		ExitTimeOut:   d.exitTimeOut(),
	}, nil
}

func (r Restart) launchd() (launchd.RestartPolicy, error) {
	switch r {
	case RestartNever:
		return launchd.NoRestart, nil
	case RestartOnFailure:
		return launchd.RestartOnFailure, nil
	case RestartAlways:
		return launchd.RestartAlways, nil
	default:
		return 0, fmt.Errorf("daemonkit: unknown restart policy %d", r)
	}
}

// launchctl runs one /bin/launchctl invocation to completion. An exit code is
// an answer, not a failure: only a launchctl that could not be run at all is
// an error, which is the boundary launchd's single outcome classifier reads. A
// launchctl that never ran carries no status either: reporting one as zero had
// the classifier prescribe decoding a status launchd never returned.
func launchctl(ctx context.Context, path string, args ...string) (string, int, error) {
	out, err := exec.CommandContext(ctx, path, args...).CombinedOutput()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(out), exit.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}
