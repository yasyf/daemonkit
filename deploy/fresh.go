//go:build darwin

package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/launchd"
)

// Fresh reports whether this deployment has never been here: nil when nothing
// of it exists, and an [ErrNotFresh] naming the evidence when anything does or
// when a check cannot say. It is [Deployment.InstallFresh]'s own gate run
// read-only — no lock, no directory, no record — so a caller deciding whether to
// install at all decides on exactly the predicate the install refuses on.
//
// Fresh means every one of these holds, asked in this order, cheapest first, so
// an installed deployment refuses on one stat of the canonical path before
// anything reaches launchd, the socket, or the process table:
//
//   - nothing occupies the canonical path;
//   - no generation occupies the private candidate, prior, or removal slot, or
//     a staging tree, no swap, activation, services, or removal record exists,
//     and the metadata directory, if there is one, holds nothing but the
//     deployment lock;
//   - the daemon's owner record does not exist;
//   - [launchd.RequireAbsent] holds for every configured agent: no plist at its
//     label, and launchd reporting the label not loaded;
//   - the control attach proves nothing listening: a daemon serving, draining,
//     untrusted, or silent until the deadline is not fresh;
//   - no live process runs any of the deployment's executables.
//
// Like every verb that reaches the control lane, Fresh requires a context
// carrying a deadline.
func (d *Deployment) Fresh(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("deploy: Fresh requires a context deadline")
	}
	return d.fresh(ctx, false)
}

// InstallFresh installs and activates the first generation of a deployment that
// has never been here, and refuses with [ErrNotFresh], before it moves a byte, a
// deployment that has. It is Install and Activate as one verb for exactly the
// state neither has anything to clean up in: it never recovers an outstanding
// swap, never quiesces, drains, or terminates anything, and never boots a job
// out of launchd. Whatever it finds already there it leaves as it found it.
//
// The whole lifecycle runs under one hold of the deployment lock, so no other
// writer can land, supersede, or leave a swap between the gate and the
// activation it authorizes. Inside that hold the [Deployment.Fresh] predicate
// runs twice: before anything is staged, and again once the candidate has
// landed, when the canonical path must be exactly the generation this call put
// there and everything else must still be absent. Only then is every
// configured label recorded in the services record, on converge's terms, and
// each agent installed through [launchd.ApplyFresh], which bootstraps a label
// nothing holds and refuses one something does. Whatever ApplyFresh reports
// before launchd has been asked to change anything — a label found held, a
// check that cannot answer, a plist it could not publish — is ErrNotFresh too,
// and so is a bootstrap launchd refuses while it reports the label loaded; a
// failure once launchd has been asked to change something is reported as
// itself. Either way nothing is ended: the landed bytes stay installed, every
// agent before the refused one stays bootstrapped and started, and the services
// record keeps naming every configured label. The daemon those agents start is
// proved ready and the activation sealed.
//
// A lock that cannot be acquired and a check that cannot answer are ErrNotFresh:
// uncertain is not fresh. A metadata directory that cannot be made private to
// its owner is the error it is for every verb, not ErrNotFresh. A refusal
// before the candidate lands leaves at most the metadata directory and its
// empty deployment lock, which Fresh still counts as fresh. Like Fresh,
// InstallFresh requires a context carrying a deadline.
func (d *Deployment) InstallFresh(ctx context.Context, candidate Candidate) (Activation, error) {
	if _, ok := ctx.Deadline(); !ok {
		return Activation{}, errors.New("deploy: InstallFresh requires a context deadline")
	}
	if err := candidate.validate(); err != nil {
		return Activation{}, err
	}
	if err := d.layout.ensureMetadata(); err != nil {
		return Activation{}, err
	}
	release, err := d.hold(ctx)
	if err != nil {
		return Activation{}, fmt.Errorf("%w: %w", ErrNotFresh, err)
	}
	defer release()
	if err := d.fresh(ctx, false); err != nil {
		return Activation{}, err
	}
	landing, err := d.landFresh(ctx, candidate)
	if err != nil {
		return Activation{}, err
	}
	generation, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return Activation{}, fmt.Errorf("%w: %w", ErrNotFresh, err)
	}
	if err := candidate.matches(generation); err != nil {
		return Activation{}, fmt.Errorf("%w: %w", ErrNotFresh, err)
	}
	if !generation.sameTree(landing) {
		return Activation{}, fmt.Errorf("%w: %q is not the generation this install landed", ErrNotFresh, d.layout.canonical)
	}
	if err := d.fresh(ctx, true); err != nil {
		return Activation{}, err
	}
	if err := d.bootstrapAgents(ctx); err != nil {
		return Activation{}, err
	}
	readiness, err := d.prove(ctx)
	if err != nil {
		return Activation{}, err
	}
	if err := writeRecord(d.layout.activation, activationRecord{
		Identity: activationIdentity, Schema: recordSchema,
		Generation: generation, Readiness: readiness.stored(),
	}); err != nil {
		return Activation{}, err
	}
	return Activation{Generation: generation, Readiness: readiness}, nil
}

func (d *Deployment) landFresh(ctx context.Context, candidate Candidate) (Generation, error) {
	staged, err := d.stage(ctx, candidate)
	if err != nil {
		return Generation{}, err
	}
	landing := staged
	landing.Path = d.layout.canonical
	record := swapRecord{
		Identity: swapIdentity, Schema: recordSchema, Target: d.layout.canonical, Candidate: landing,
	}
	if err := record.validate(); err != nil {
		return Generation{}, err
	}
	if err := writeRecord(d.layout.swap, record); err != nil {
		return Generation{}, err
	}
	if err := d.settleSwap(ctx, record); err != nil {
		return Generation{}, err
	}
	if err := d.retireSwap(record); err != nil {
		return Generation{}, err
	}
	return landing, nil
}

func (d *Deployment) bootstrapAgents(ctx context.Context) error {
	desired := make([]string, 0, len(d.config.Agents))
	for _, agent := range d.config.Agents {
		desired = append(desired, agent.Label)
	}
	if err := d.recordServices(slices.Compact(slices.Sorted(slices.Values(desired)))); err != nil {
		return err
	}
	for _, agent := range d.config.Agents {
		mutated := false
		run := func(ctx context.Context, path string, args ...string) (string, int, error) {
			mutated = mutated || args[0] != "print"
			return d.run(ctx, path, args...)
		}
		switch err := launchd.ApplyFresh(ctx, run, agent); {
		case err == nil:
		case errors.Is(err, launchd.ErrPresent), !mutated:
			return fmt.Errorf("%w: %w", ErrNotFresh, err)
		default:
			return fmt.Errorf("deploy: apply agent %q: %w", agent.Label, err)
		}
	}
	return nil
}

func (d *Deployment) fresh(ctx context.Context, landed bool) error {
	if err := d.freshOnDisk(landed); err != nil {
		return err
	}
	for _, agent := range d.config.Agents {
		if err := launchd.RequireAbsent(ctx, d.run, agent.Label); err != nil {
			return fmt.Errorf("%w: %w", ErrNotFresh, err)
		}
	}
	if err := d.daemonAbsent(ctx); err != nil {
		return err
	}
	if err := d.requireEmpty(); err != nil {
		return fmt.Errorf("%w: %w", ErrNotFresh, err)
	}
	return nil
}

func (d *Deployment) freshOnDisk(landed bool) error {
	if !landed {
		if err := requireAbsent(d.layout.canonical); err != nil {
			return err
		}
	}
	slots, err := d.generationSlots()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNotFresh, err)
	}
	held := slices.Concat(
		slices.DeleteFunc(slots, func(slot string) bool { return slot == d.layout.canonical }),
		[]string{d.layout.swap, d.layout.activation, d.layout.services, d.layout.removal},
	)
	for _, path := range held {
		if err := requireAbsent(path); err != nil {
			return err
		}
	}
	if err := d.bareMetadata(); err != nil {
		return err
	}
	return requireAbsent(d.config.Daemon.RecordPath())
}

func requireAbsent(path string) error {
	switch _, err := os.Lstat(path); {
	case err == nil:
		return fmt.Errorf("%w: %q exists", ErrNotFresh, path)
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: inspect %q: %w", ErrNotFresh, path, err)
	}
	return nil
}

func (d *Deployment) bareMetadata() error {
	entries, err := os.ReadDir(d.layout.metadata)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("%w: scan %q: %w", ErrNotFresh, d.layout.metadata, err)
	}
	for _, entry := range entries {
		if entry.Name() != filepath.Base(d.layout.lock) {
			return fmt.Errorf("%w: %q holds %q", ErrNotFresh, d.layout.metadata, entry.Name())
		}
	}
	return nil
}

func (d *Deployment) daemonAbsent(ctx context.Context) error {
	control, err := d.client.Control(ctx)
	switch {
	case errors.Is(err, daemonkit.ErrAbsent):
		return nil
	case err != nil:
		return fmt.Errorf("%w: the control attach proved no absence: %w", ErrNotFresh, err)
	}
	_ = control.Close(ctx)
	return fmt.Errorf("%w: a daemon is serving %q", ErrNotFresh, d.config.Daemon.Label)
}
