// Package deploy owns sealed installation, activation, supersession, and
// removal of one fixed signed application.
//
// Everything deploy writes down is something it could not read back off the
// disk. The swap record names the two generations of a rename pair, because a
// superseded bundle's bytes are unrecoverable from bare stat once they move;
// which of the pair's two renames already landed is recomputed from stat plus
// a codesign re-verify on every call, never stored. The activation and
// removal records keep proofs for the same reason: a proof is evidence about
// a moment, and no later stat reconstructs it. The services record names the
// labels launchd was converged to, because deploy never asks the machine what
// daemonkit owns — a scan answers for every consumer at once, so one product's
// converge would evict another's agents. Whether an app is installed, whether
// an agent's plist is the one it should be, whether a generation is the one a
// record names — all of that is asked of the filesystem, every time.
package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/flock"
	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/launchd"
)

// Sentinel identity is load-bearing: consumers alias these and match with
// errors.Is across module boundaries.
var (
	// ErrUntrusted means a bundle failed its designated requirement.
	ErrUntrusted = errors.New("deploy: bundle failed its designated requirement")

	// ErrVersion means a bundle declares a marketing version other than the
	// one the request named.
	ErrVersion = errors.New("deploy: bundle version mismatch")

	// ErrConfig means the deployment configuration is incomplete or
	// self-contradictory.
	ErrConfig = errors.New("deploy: invalid config")

	// ErrConflict means the filesystem disagrees with the request or with a
	// durable record: installed bytes changed, a slot is occupied, or a
	// generation is not the one named.
	ErrConflict = errors.New("deploy: installed state differs from the request")

	// ErrState means a durable deployment record is corrupt or inexact.
	ErrState = errors.New("deploy: durable deployment state is invalid")
)

const lockGrace = 30 * time.Second

// Config names the one signed application a Deployment owns.
type Config struct {
	// App is the canonical absolute .app path this deployment installs to.
	// Every path the deployment owns derives from it.
	App string

	// Requirement is the trusted-publisher policy every generation must
	// satisfy, rendered to a designated requirement for static verification.
	// It is the coarse gate; a generation's CDHash is the fine one.
	Requirement daemonkit.Requirement

	// Daemon names the daemon the application serves, so quiesce can reach it
	// and readiness can be observed. Trust.Serving pins what the process
	// answering on the socket must prove — without it an absence proof is
	// forgeable by any same-UID process that binds the socket first.
	//
	// Leaving it nil is the caller's risk to take, and deploy takes it rather
	// than refusing: no irreversible step here rests on that proof alone. Each
	// one also requires the executable-scoped inventory, which reads the
	// kernel's own process table and no same-UID process can forge, so a forged
	// socket proof buys a swap or a removal nothing — it still has to be true
	// that no process of this deployment is running. A Serving that is set is
	// held to the same terms as Requirement: Open renders it once, so a policy
	// that could admit nobody fails there rather than at the first attach.
	Daemon daemonkit.Daemon

	// Agents is the exact LaunchAgent set activation converges launchd to.
	// Every Program must live inside App.
	Agents []launchd.Agent

	// Executables name programs the quiesce inventory gate must also find
	// empty, beyond the agents' own and the bundle's own: the host binary a
	// launcher runs from outside the bundle. Open resolves each to the exact
	// form the kernel reports.
	Executables []string
}

// Deployment is one application's sealed lifecycle.
type Deployment struct {
	config      Config
	layout      layout
	requirement string
	verify      verifier
	run         launchd.Runner
	client      *daemonkit.Client
	inventory   func(...string) (Survivors, error)
}

// Open binds a deployment to its configuration and resolves the host
// executables the inventory gate covers. That resolution is the only I/O it
// does, and it is not optional: the gate matches a kernel-reported executable
// exactly, so a declared path in any other form matches nothing and the gate
// silently passes.
func Open(config Config) (*Deployment, error) {
	if !validAppPath(config.App) {
		return nil, fmt.Errorf("%w: App must be an exact absolute .app path", ErrConfig)
	}
	requirement, err := designatedRequirement(config.Requirement)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := config.Daemon.ValidateForServe(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if serving := config.Daemon.Trust.Serving; serving != nil {
		if _, err := designatedRequirement(*serving); err != nil {
			return nil, fmt.Errorf("%w: Daemon.Trust.Serving: %w", ErrConfig, err)
		}
	}
	if len(config.Agents) == 0 {
		return nil, fmt.Errorf("%w: at least one agent is required", ErrConfig)
	}
	for _, agent := range config.Agents {
		if !filepath.IsAbs(agent.Program) || filepath.Clean(agent.Program) != agent.Program {
			return nil, fmt.Errorf("%w: agent program %q must be an exact absolute path", ErrConfig, agent.Program)
		}
		if within, err := filepath.Rel(config.App, agent.Program); err != nil || !filepath.IsLocal(within) {
			return nil, fmt.Errorf("%w: agent program %q is outside %q", ErrConfig, agent.Program, config.App)
		}
	}
	executables, err := resolveExecutables(config.Executables)
	if err != nil {
		return nil, err
	}
	config.Executables = executables
	return &Deployment{
		config:      config,
		layout:      layoutFor(config.App),
		requirement: requirement,
		verify:      codesignVerifier{},
		run:         execRunner,
		client:      daemonkit.Open(config.Daemon),
		inventory:   Inventory,
	}, nil
}

// Activation is one sealed activation: the generation launchd was converged
// to, and what the daemon it started proved about itself.
type Activation struct {
	Generation Generation
	Readiness  ReadinessProof
}

// Removal is one sealed removal: the generation that left, and the absence
// proof this call made to authorize removing it. A replay's generation comes
// out of the sealed tombstone — nothing else can name bytes that are already
// gone — while its proof is always the one just made.
type Removal struct {
	Generation Generation
	Runtime    RuntimeProof
}

// Candidate names the signed bundle a deployment is asked to land.
type Candidate struct {
	// Source is the absolute .app path holding the candidate bytes. They are
	// copied into a private slot beside the canonical path, never moved from
	// under the caller.
	Source string
	// Version is the exact CFBundleShortVersionString the bundle must declare.
	Version string
	// Digest is the exact bundle-tree digest the bundle must hash to.
	Digest SHA256
}

func (c Candidate) validate() error {
	if !validAppPath(c.Source) || c.Version == "" || c.Digest == (SHA256{}) {
		return fmt.Errorf("%w: candidate source, version, and digest are required", ErrConfig)
	}
	return nil
}

// matches holds the bundle to what the request said it was. The version guard
// is what keeps a downgrade or a mispackaged build from riding in under a
// signature that is perfectly valid for it.
func (c Candidate) matches(g Generation) error {
	if g.Version != c.Version {
		return fmt.Errorf("%w: got %q want %q", ErrVersion, g.Version, c.Version)
	}
	if g.BundleDigest != c.Digest.String() {
		return fmt.Errorf("%w: bundle at %q is not the requested digest", ErrConflict, g.Path)
	}
	return nil
}

// Install lands the first generation at the canonical path. It refuses an
// occupied canonical path: replacing a live installation is Supersede's job,
// and only Supersede quiesces the incumbent first.
func (d *Deployment) Install(ctx context.Context, candidate Candidate) (Generation, error) {
	return d.land(ctx, candidate, false)
}

// Supersede replaces the installed generation with candidate. It quiesces the
// incumbent first — proved gone and its executables proved empty — then moves
// the incumbent aside and the candidate into place as one recorded rename
// pair, so a crash anywhere in the middle resumes to the same end.
//
// A whole bundle tree is copied between the quiesce and the rename, so the
// inventory half runs again immediately before it: an unbounded copy is exactly
// long enough for a process to come back, and the rename destroys the bytes it
// would be running from. That gate precedes the swap record, so a refusal is
// durable — with no record outstanding there is no swap for the next verb's
// resume to drive past it.
func (d *Deployment) Supersede(ctx context.Context, candidate Candidate) (Generation, error) {
	return d.land(ctx, candidate, true)
}

func (d *Deployment) land(ctx context.Context, candidate Candidate, supersede bool) (Generation, error) {
	if err := candidate.validate(); err != nil {
		return Generation{}, err
	}
	release, err := d.hold(ctx)
	if err != nil {
		return Generation{}, err
	}
	defer release()
	if err := d.recover(ctx); err != nil {
		return Generation{}, err
	}
	var prior *Generation
	switch installed := fileExists(d.layout.canonical); {
	case installed && !supersede:
		return Generation{}, fmt.Errorf("%w: %q is already installed", ErrConflict, d.layout.canonical)
	case !installed && supersede:
		return Generation{}, fmt.Errorf("%w: nothing is installed at %q", ErrConflict, d.layout.canonical)
	case installed:
		incumbent, err := d.inspect(ctx, d.layout.canonical)
		if err != nil {
			return Generation{}, err
		}
		if _, err := d.quiesceAndConverge(ctx, nil); err != nil {
			return Generation{}, err
		}
		prior = &incumbent
	}
	staged, err := d.stage(ctx, candidate)
	if err != nil {
		return Generation{}, err
	}
	landing := staged
	landing.Path = d.layout.canonical
	record := swapRecord{
		Identity: swapIdentity, Schema: recordSchema, Target: d.layout.canonical,
		Prior: prior, Candidate: landing,
	}
	if err := record.validate(); err != nil {
		return Generation{}, err
	}
	if prior != nil {
		if err := d.requireEmpty(); err != nil {
			return Generation{}, err
		}
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
	return d.inspect(ctx, d.layout.canonical)
}

// Activate converges launchd to the deployment's exact agent set and seals
// what the daemon they start proves about itself.
//
// A sealed activation is immutable evidence, so a re-activation must reproduce
// it exactly: a readiness observation that disagrees with the seal — a
// different build, or a different instance because the daemon restarted — is
// refused rather than silently overwritten. Reset discards the seal when the
// caller genuinely means to re-prove readiness from scratch.
//
// A converge that only applies agents starts things and takes nothing away, so
// it runs against the live daemon it is re-proving. One that also retires a
// label goes through the gate first — see [Deployment.convergeAgents].
func (d *Deployment) Activate(ctx context.Context) (Activation, error) {
	release, err := d.hold(ctx)
	if err != nil {
		return Activation{}, err
	}
	defer release()
	if err := d.recover(ctx); err != nil {
		return Activation{}, err
	}
	generation, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return Activation{}, err
	}
	if err := d.convergeAgents(ctx); err != nil {
		return Activation{}, err
	}
	readiness, err := d.prove(ctx)
	if err != nil {
		return Activation{}, err
	}
	record := activationRecord{
		Identity: activationIdentity, Schema: recordSchema,
		Generation: generation, Readiness: readiness.stored(),
	}
	var sealed activationRecord
	switch err := readRecord(d.layout.activation, &sealed); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return Activation{}, err
	case sealed != record:
		return Activation{}, fmt.Errorf("%w: readiness differs from the sealed activation", ErrConflict)
	}
	if err := errors.Join(
		removeFileDurable(d.layout.removal),
		writeRecord(d.layout.activation, record),
	); err != nil {
		return Activation{}, err
	}
	return Activation{Generation: generation, Readiness: readiness}, nil
}

// Uninstall quiesces the daemon, converges its services away, and removes the
// installed application.
//
// The quiesce gate runs before anything irreversible, on every call: Stopped's
// reap proves the pinned incumbent left, and the executable-scoped inventory
// proves no process of this deployment survives it — an orphaned child, a
// second instance, or the app half. Removing the services a husk still needs
// is what that pair exists to prevent. A sealed tombstone authorizes nothing
// on its own: it is evidence about the moment it was minted, so a resumed
// uninstall re-proves both halves against this one — a replay that finds the
// app already gone is gated too, because the proof it hands back is what the
// caller acts on either way. That proof is therefore this call's, never the
// tombstone's; only the generation comes out of the record, because once the
// bytes are gone nothing else can name what left. The inventory half runs once more immediately
// before the removal, since sealing the tombstone stands between it and the
// rename. The removal is staged through a private slot, so the canonical path
// goes from whole to absent in one rename and is never left half-deleted.
//
// What it removes is every generation slot but the canonical path, out of the
// same enumeration Reset draws from and the gate just scanned: a leaked staging
// tree and a superseded prior are whole copies of the same signed application,
// and an uninstall that left them behind left the application it removed on
// disk.
func (d *Deployment) Uninstall(ctx context.Context) (Removal, error) {
	release, err := d.hold(ctx)
	if err != nil {
		return Removal{}, err
	}
	defer release()
	if err := d.recover(ctx); err != nil {
		return Removal{}, err
	}
	runtime, err := d.quiesceAndConverge(ctx, nil)
	if err != nil {
		return Removal{}, err
	}
	record, err := d.tombstone(ctx, runtime)
	if err != nil {
		return Removal{}, err
	}
	if err := d.requireEmpty(); err != nil {
		return Removal{}, err
	}
	if fileExists(d.layout.canonical) {
		if fileExists(d.layout.removed) {
			return Removal{}, fmt.Errorf("%w: private removal slot is occupied", ErrConflict)
		}
		if err := d.attest(ctx, record.Generation); err != nil {
			return Removal{}, err
		}
		if err := renameDurable(d.layout.canonical, d.layout.removed); err != nil {
			return Removal{}, err
		}
	}
	if err := d.discardGenerations(); err != nil {
		return Removal{}, err
	}
	if fileExists(d.layout.canonical) {
		return Removal{}, fmt.Errorf("%w: canonical app returned during uninstall", ErrConflict)
	}
	return Removal{Generation: record.Generation, Runtime: runtime}, nil
}

// tombstone names the generation this removal is of: the one an earlier pass
// sealed, or the installed one sealed now. It is the half a replay cannot
// re-derive — the bytes it describes are gone by then — and the only half a
// replay takes from the record. The absence proof stored beside it is evidence
// about the moment it was minted and is never handed back as this moment's.
func (d *Deployment) tombstone(ctx context.Context, runtime RuntimeProof) (removalRecord, error) {
	var record removalRecord
	switch err := readRecord(d.layout.removal, &record); {
	case err == nil:
		return record, nil
	case !errors.Is(err, os.ErrNotExist):
		return removalRecord{}, err
	}
	generation, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return removalRecord{}, err
	}
	record = removalRecord{
		Identity: removalIdentity, Schema: recordSchema,
		Generation: generation, Runtime: runtime.stored(),
	}
	if err := errors.Join(
		removeFileDurable(d.layout.activation),
		writeRecord(d.layout.removal, record),
	); err != nil {
		return removalRecord{}, err
	}
	return record, nil
}

// Reset returns the deployment to a clean slate: whatever swap was
// outstanding is driven to its end, the daemon is quiesced under the same
// gate every irreversible step uses, launchd is converged empty, every record
// deploy owns is discarded, and so is every generation slot the gate just
// scanned.
//
// The inventory half runs once more immediately before the discard, exactly as
// it does on Uninstall's arm: converging launchd empty is a bootout exec per
// label plus plist removals and directory syncs, and that is long enough for a
// process to come back onto the bytes the discard is about to take.
//
// It is the way out of a state no other verb accepts — a sealed activation
// whose daemon has since restarted, a removal proof for an app that was put
// back by hand, a plain file planted at a generation slot — and it never
// destroys installed bytes: the canonical path keeps whatever generation the
// settled swap left there.
func (d *Deployment) Reset(ctx context.Context) error {
	release, err := d.hold(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := d.recover(ctx); err != nil {
		return err
	}
	if _, err := d.quiesceAndConverge(ctx, nil); err != nil {
		return err
	}
	if err := d.requireEmpty(); err != nil {
		return err
	}
	return errors.Join(
		removeFileDurable(d.layout.activation),
		removeFileDurable(d.layout.removal),
		removeFileDurable(d.layout.swap),
		removeFileDurable(d.layout.services),
		d.discardGenerations(),
	)
}

// quiesceAndConverge proves the daemon gone, then converges launchd to
// agents. The order is the invariant: services are only ever taken away from
// a runtime that was already proved absent, never from a live one.
func (d *Deployment) quiesceAndConverge(ctx context.Context, agents []launchd.Agent) (RuntimeProof, error) {
	proof, err := d.Quiesce(ctx)
	if err != nil {
		return RuntimeProof{}, err
	}
	if err := d.converge(ctx, agents); err != nil {
		return RuntimeProof{}, err
	}
	return proof, nil
}

// convergeAgents converges launchd to the deployment's own agent set on
// activation's terms: a converge that only applies agents proves nothing
// first, and one that retires a label proves the daemon gone like every other
// removal does.
//
// The distinction is the whole point. Applying an agent starts a job and
// destroys nothing, so demanding an absence proof for it would mean draining a
// healthy daemon on every re-activation — and a re-activation that restarted
// the daemon could never reproduce its own sealed readiness. Retiring a label
// the services record names and the config no longer does is the opposite: it
// boots that label's job out of launchd, which is how a live daemon dies, and
// it may only ever happen to a runtime already proved absent.
func (d *Deployment) convergeAgents(ctx context.Context) error {
	retires, err := d.retiresServices()
	if err != nil {
		return err
	}
	if !retires {
		return d.converge(ctx, d.config.Agents)
	}
	_, err = d.quiesceAndConverge(ctx, d.config.Agents)
	return err
}

// retiresServices reports whether converging to the configured agent set takes
// a label away from launchd: one the durable services record holds and the
// configuration no longer names.
func (d *Deployment) retiresServices() (bool, error) {
	applied, err := d.appliedServices()
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(applied, func(label string) bool {
		return !slices.ContainsFunc(d.config.Agents, func(agent launchd.Agent) bool {
			return agent.Label == label
		})
	}), nil
}

// converge drives launchd to exactly agents, one label at a time, and takes
// away only the labels this deployment's own durable record says it applied.
// Nothing is ever discovered from the machine: a consumer always knows its own
// labels, so no converge of one product can reach another product's agents.
//
// The record names the union before the first launchctl call and the desired
// set after the last, so it is never a subset of what is applied — a crash
// mid-converge leaves a label recorded that may already be gone, never one
// that exists unrecorded.
func (d *Deployment) converge(ctx context.Context, agents []launchd.Agent) error {
	desired := make([]string, 0, len(agents))
	for _, agent := range agents {
		desired = append(desired, agent.Label)
	}
	slices.Sort(desired)
	desired = slices.Compact(desired)
	previous, err := d.appliedServices()
	if err != nil {
		return err
	}
	held := slices.Compact(slices.Sorted(slices.Values(slices.Concat(previous, desired))))
	if err := d.recordServices(held); err != nil {
		return err
	}
	for _, agent := range agents {
		if err := launchd.Apply(ctx, d.run, agent); err != nil {
			return fmt.Errorf("deploy: apply agent %q: %w", agent.Label, err)
		}
	}
	for _, label := range previous {
		if slices.Contains(desired, label) {
			continue
		}
		if err := launchd.Remove(ctx, d.run, label); err != nil {
			return fmt.Errorf("deploy: remove agent %q: %w", label, err)
		}
	}
	return d.recordServices(desired)
}

func (d *Deployment) appliedServices() ([]string, error) {
	var record serviceRecord
	switch err := readRecord(d.layout.services, &record); {
	case errors.Is(err, os.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return record.Labels, nil
}

func (d *Deployment) recordServices(labels []string) error {
	if len(labels) == 0 {
		return removeFileDurable(d.layout.services)
	}
	return writeRecord(d.layout.services, serviceRecord{
		Identity: serviceIdentity, Schema: recordSchema, Labels: labels,
	})
}

func (d *Deployment) hold(ctx context.Context) (func(), error) {
	if err := d.layout.ensureMetadata(); err != nil {
		return nil, err
	}
	lock, err := (flock.Spec{Path: d.layout.lock, Mode: flock.Exclusive, Deadline: lockGrace}).Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("deploy: acquire deployment lock: %w", err)
	}
	return func() { _ = lock.Close() }, nil
}

// designatedRequirement validates the whole publisher policy and renders the
// static designated requirement from it. The entitlement clauses never reach
// the rendered string — a signed image satisfies its own designated
// requirement from any path an attacker can write to, so entitlements narrow
// peer admission, never on-disk verification — but a policy that contradicts
// itself is a configuration error wherever it is used.
func designatedRequirement(r daemonkit.Requirement) (string, error) {
	requirement := trust.Requirement{
		TeamID:            r.TeamID,
		SigningIdentifier: r.SigningIdentifier,
		RequiredAppGroup:  r.RequiredAppGroup,
		AllowJIT:          r.AllowJIT,
	}
	if len(r.RequiredEntitlements) > 0 {
		requirement.RequiredEntitlements = make(map[string]trust.EntitlementRequirement, len(r.RequiredEntitlements))
		for key, entitlement := range r.RequiredEntitlements {
			requirement.RequiredEntitlements[key] = trust.EntitlementRequirement{
				Match:   trust.EntitlementMatch(entitlement.Match),
				Boolean: entitlement.Boolean,
				String:  entitlement.String,
			}
		}
	}
	return requirement.DRString()
}

func execRunner(ctx context.Context, path string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, path, args...)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(out), exitErr.ExitCode(), nil
	}
	if err != nil {
		return string(out), -1, err
	}
	return string(out), 0, nil
}
