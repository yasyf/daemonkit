package daemonkit

import (
	"context"
	"fmt"
	"os/signal"

	"github.com/yasyf/daemonkit/internal/converge"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/supervise"
)

// service is the specification the platform's service layer runs a daemon
// from: on linux, one supervised service.
type service = supervise.Service

// serviceLayer is how a Client reaches the label's supervisor. It holds
// nothing: the supervisor is found at the label's own socket on every verb.
type serviceLayer struct{}

func openServiceLayer() serviceLayer { return serviceLayer{} }

func validateLabel(label string) error { return supervise.ValidateLabel(label) }

func (serviceLayer) applyService(ctx context.Context, agent service) error {
	return supervise.Apply(ctx, agent)
}

func (serviceLayer) removeService(ctx context.Context, label string) error {
	return supervise.Remove(ctx, label)
}

func (c *Client) observeWorld(ctx context.Context, agent service) (converge.World, error) {
	record, err := c.record()
	if err != nil {
		return converge.World{}, err
	}
	return converge.Observe(ctx, converge.Sources{
		Serving:    c.servedHealth,
		Recorded:   proc.ReadOwner,
		RecordPath: record,
		Service:    agent,
	})
}

// agent is the supervised service that runs this daemon. Every field is
// already on the Daemon, so nothing about the service is declared twice; an
// unset Log sinks to the state directory's daemon.log. The service inherits
// its supervisor's environment, PATH included: on linux the workspace that
// owns the supervisor owns that environment too.
func (d Daemon) agent() (service, error) {
	el, err := d.Label.element()
	if err != nil {
		return service{}, err
	}
	program, err := d.Program.path(el)
	if err != nil {
		return service{}, err
	}
	restart, err := d.Restart.supervised()
	if err != nil {
		return service{}, err
	}
	log := d.Log
	if log == "" {
		log = el.state().LogPath()
	}
	return service{
		Label:         el.label,
		Program:       program,
		Args:          d.Args,
		LogPath:       log,
		RestartPolicy: restart,
		ExitTimeOut:   d.exitTimeOut(),
	}, nil
}

func (r Restart) supervised() (supervise.RestartPolicy, error) {
	switch r {
	case RestartNever:
		return supervise.NoRestart, nil
	case RestartOnFailure:
		return supervise.RestartOnFailure, nil
	case RestartAlways:
		return supervise.RestartAlways, nil
	default:
		return 0, fmt.Errorf("daemonkit: unknown restart policy %d", r)
	}
}

// Supervise is the foreground supervisor for one daemon's label, the process
// linux runs in launchd's place. It blocks until ctx ends or a drain signal
// arrives, then stops the daemon within its exit timeout and returns.
//
// The workspace owns this process: it is started wherever the workspace starts
// its long-lived processes, and nothing in daemonkit starts it, daemonizes it,
// or restarts it. While it runs, [Client.Ensure] and [Client.Stop] converge the
// daemon through it exactly as they converge a LaunchAgent on darwin; with no
// supervisor running they refuse with [supervise.ErrNoSupervisor] before they
// touch a live daemon. The service an Ensure applied is persisted, so a
// supervisor started again after a reboot resumes it without another Ensure.
func Supervise(ctx context.Context, label Label) error {
	el, err := label.element()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, drainSignals...)
	defer stop()
	return supervise.Run(ctx, el.label)
}
