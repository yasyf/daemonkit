package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/daemonkit/paths"
)

// ErrMaintenanceIncomplete leaves the deployment unavailable rather than
// restarting an incumbent after an uncertain or partly applied replacement.
var ErrMaintenanceIncomplete = errors.New("deploy: preserving maintenance is incomplete")

type maintenanceRecord struct {
	Identity string      `json:"identity"`
	Schema   int         `json:"schema"`
	Target   string      `json:"target"`
	Prior    *Generation `json:"prior,omitempty"`
}

func (r maintenanceRecord) validate() error {
	if r.Identity != "daemonkit.deploy.maintenance.v1" || r.Schema != recordSchema || !validAppPath(r.Target) {
		return ErrState
	}
	if r.Prior != nil {
		if r.Prior.Path != r.Target {
			return ErrState
		}
		return r.Prior.validate()
	}
	return nil
}

func (d *Deployment) maintenancePath() string {
	return filepath.Join(d.layout.metadata, "maintenance.json")
}

func (d *Deployment) checkMaintenance() error {
	var record maintenanceRecord
	err := readRecord(d.maintenancePath(), &record)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: prior transaction for %s needs explicit recovery", ErrMaintenanceIncomplete, record.Target)
}

func (d *Deployment) requirePristine() error {
	for _, path := range []string{d.layout.canonical, d.layout.prior, d.layout.removed, d.layout.activation, d.layout.removal, d.layout.swap, d.layout.services, d.config.Daemon.RecordPath()} {
		_, err := os.Lstat(path)
		if err == nil {
			return fmt.Errorf("%w: prior installation or ownership state at %s", daemonkit.ErrDrainBusy, path)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := d.requireEmpty(); err != nil {
		return fmt.Errorf("%w: pristine executable inventory is unproven: %v", daemonkit.ErrDrainBusy, err)
	}
	return nil
}

func (d *Deployment) holdStarts(ctx context.Context) (func(), error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("deploy: maintenance requires a deadline")
	}
	state := paths.Agent(string(d.config.Daemon.Label))
	if err := state.EnsureLockDir(); err != nil {
		return nil, err
	}
	lock, err := (flock.Spec{Path: state.StartLockPath(), Mode: flock.Exclusive, Deadline: time.Until(deadline)}).Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return func() { _ = lock.Close() }, nil
}

type stoppedMaintenance struct {
	control   *daemonkit.Control
	before    daemonkit.Health
	jobs      []*launchd.Maintenance
	proof     RuntimeProof
	committed bool
}

func (m *stoppedMaintenance) restore(ctx context.Context) error {
	fresh, err := m.control.Health(ctx)
	if err != nil {
		return err
	}
	if fresh.PID != m.before.PID || fresh.Generation != m.before.Generation || fresh.Build != m.before.Build {
		return daemonkit.ErrWrongIncumbent
	}
	var failures []error
	for i := len(m.jobs) - 1; i >= 0; i-- {
		if err := m.jobs[i].Restore(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (d *Deployment) pauseAndStop(ctx context.Context) (*stoppedMaintenance, error) {
	control, err := d.client.Control(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: preserving control is unavailable: %v", daemonkit.ErrDrainBusy, err)
	}
	before, err := control.Health(ctx)
	if err != nil {
		_ = control.Close(ctx)
		return nil, fmt.Errorf("%w: preserving health is unavailable: %v", daemonkit.ErrDrainBusy, err)
	}
	m := &stoppedMaintenance{control: control, before: before}
	if !before.PreserveOwned {
		return m, fmt.Errorf("%w: incumbent lacks preservation preparation", daemonkit.ErrDrainBusy)
	}
	labels, err := d.appliedServices()
	if err != nil {
		return m, err
	}
	labels = append(labels, string(d.config.Daemon.Label))
	for _, agent := range d.config.Agents {
		labels = append(labels, agent.Label)
	}
	slices.Sort(labels)
	labels = slices.Compact(labels)
	for _, label := range labels {
		pid := 0
		if label == string(d.config.Daemon.Label) {
			pid = before.PID
		}
		lease, err := launchd.PauseRestarts(ctx, d.run, label, pid)
		if lease != nil {
			m.jobs = append(m.jobs, lease)
		}
		if err != nil {
			return m, fmt.Errorf("%w: restart exclusion failed: %v", daemonkit.ErrDrainBusy, err)
		}
	}
	fresh, err := control.Health(ctx)
	if err != nil {
		return m, fmt.Errorf("%w: disabled job identity is unproven: %v", daemonkit.ErrDrainBusy, err)
	}
	if fresh.PID != before.PID || fresh.Generation != before.Generation || fresh.Build != before.Build {
		return m, daemonkit.ErrWrongIncumbent
	}
	stopped, err := control.Drain(ctx, daemonkit.Expect{Build: before.Build, Generation: before.Generation})
	if err != nil {
		return m, err
	}
	if !absenceProof(stopped.Reap) {
		return m, ErrConflict
	}
	m.committed = true
	m.proof = runtimeProof(stopped)
	return m, nil
}

// Replace installs a verified candidate while holding both deployment and
// daemon-start writers through readiness. quiesceApplication may stop the signed
// application UI after the held daemon drain commits; it must not run brew or
// reenter daemonkit deployment/Ensure. An existing target requires acknowledged
// preserving drain commitment; a first install must pass the pristine-state gate.
// Failure after commitment leaves a durable unavailable state, never a rollback.
func (d *Deployment) Replace(ctx context.Context, candidate Candidate, quiesceApplication func(context.Context) error) (activation Activation, err error) {
	if d.config.Daemon.ShutdownPolicy != daemonkit.PreserveOwned || quiesceApplication == nil {
		return Activation{}, ErrConfig
	}
	if err := d.VerifyCandidate(ctx, candidate); err != nil {
		return Activation{}, err
	}
	release, err := d.hold(ctx)
	if err != nil {
		return Activation{}, err
	}
	defer release()
	releaseStarts, err := d.holdStarts(ctx)
	if err != nil {
		return Activation{}, err
	}
	defer releaseStarts()
	if err := d.recover(ctx); err != nil {
		return Activation{}, err
	}
	var prior *Generation
	if fileExists(d.layout.canonical) {
		generation, err := d.inspect(ctx, d.layout.canonical)
		if err != nil {
			return Activation{}, err
		}
		prior = &generation
	} else if err := d.requirePristine(); err != nil {
		return Activation{}, err
	}
	staged, err := d.stage(ctx, candidate)
	if err != nil {
		return Activation{}, err
	}
	intent := maintenanceRecord{Identity: "daemonkit.deploy.maintenance.v1", Schema: recordSchema, Target: d.layout.canonical, Prior: prior}
	if err := writeRecord(d.maintenancePath(), intent); err != nil {
		return Activation{}, err
	}
	var maintenance *stoppedMaintenance
	defer func() {
		if maintenance != nil {
			defer func() { _ = maintenance.control.Close(ctx) }()
		}
		if err == nil {
			return
		}
		if errors.Is(err, daemonkit.ErrDrainBusy) && maintenance != nil && !maintenance.committed {
			resumed := maintenance.restore(ctx)
			if resumed == nil {
				err = errors.Join(err, durable.Remove(d.maintenancePath()))
				return
			}
			err = errors.Join(err, resumed)
		}
		err = errors.Join(ErrMaintenanceIncomplete, err)
	}()
	if prior != nil {
		maintenance, err = d.pauseAndStop(ctx)
		if err != nil {
			return Activation{}, err
		}
	}
	if err := quiesceApplication(ctx); err != nil {
		return Activation{}, err
	}
	if err := d.requireEmpty(); err != nil {
		return Activation{}, err
	}
	if err := d.converge(ctx, nil); err != nil {
		return Activation{}, err
	}
	if err := d.requireEmpty(); err != nil {
		return Activation{}, err
	}
	landing := staged
	landing.Path = d.layout.canonical
	record := swapRecord{Identity: swapIdentity, Schema: recordSchema, Target: d.layout.canonical, Prior: prior, Candidate: landing}
	if err := record.validate(); err != nil {
		return Activation{}, err
	}
	if err := writeRecord(d.layout.swap, record); err != nil {
		return Activation{}, err
	}
	if err := d.settleSwap(ctx, record); err != nil {
		return Activation{}, err
	}
	if err := d.retireSwap(record); err != nil {
		return Activation{}, err
	}
	generation, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return Activation{}, err
	}
	activation, err = d.activateGeneration(ctx, generation)
	if err != nil {
		return Activation{}, err
	}
	return activation, durable.Remove(d.maintenancePath())
}

// Remove consumes the same held preserving drain across application quiescence
// and removal. The callback has the same non-reentrancy contract as Replace.
func (d *Deployment) Remove(ctx context.Context, quiesceApplication func(context.Context) error) (removal Removal, err error) {
	if d.config.Daemon.ShutdownPolicy != daemonkit.PreserveOwned || quiesceApplication == nil {
		return Removal{}, ErrConfig
	}
	release, err := d.hold(ctx)
	if err != nil {
		return Removal{}, err
	}
	defer release()
	releaseStarts, err := d.holdStarts(ctx)
	if err != nil {
		return Removal{}, err
	}
	defer releaseStarts()
	if err := d.recover(ctx); err != nil {
		return Removal{}, err
	}
	prior, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return Removal{}, err
	}
	if err := writeRecord(d.maintenancePath(), maintenanceRecord{Identity: "daemonkit.deploy.maintenance.v1", Schema: recordSchema, Target: d.layout.canonical, Prior: &prior}); err != nil {
		return Removal{}, err
	}
	maintenance, err := d.pauseAndStop(ctx)
	defer func() {
		if maintenance != nil {
			defer func() { _ = maintenance.control.Close(ctx) }()
		}
		if err == nil {
			return
		}
		if errors.Is(err, daemonkit.ErrDrainBusy) && maintenance != nil && !maintenance.committed {
			resumed := maintenance.restore(ctx)
			if resumed == nil {
				err = errors.Join(err, durable.Remove(d.maintenancePath()))
				return
			}
			err = errors.Join(err, resumed)
		}
		err = errors.Join(ErrMaintenanceIncomplete, err)
	}()
	if err != nil {
		return Removal{}, err
	}
	if err := quiesceApplication(ctx); err != nil {
		return Removal{}, err
	}
	if err := d.requireEmpty(); err != nil {
		return Removal{}, err
	}
	if err := d.converge(ctx, nil); err != nil {
		return Removal{}, err
	}
	removal, err = d.removeGeneration(ctx, maintenance.proof)
	if err != nil {
		return Removal{}, err
	}
	return removal, durable.Remove(d.maintenancePath())
}
