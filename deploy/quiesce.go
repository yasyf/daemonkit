package deploy

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/internal/proc"
)

// readinessReserve is the share of the readiness budget the wait gives up to
// the attach that classifies its outcome. A quarter, because that attach is a
// full dial, handshake, trust check and health self-attestation against a
// daemon that may be busy starting — a sliver of the budget times out on a
// daemon that is really there and reports it absent.
const readinessReserve = 4

// escalationReserve is the most of the quiesce budget the drain may spend
// before the ladder that ends an incumbent which did not leave on its own:
// half, so a TERM-resistant incumbent still has a grace, a SIGKILL, and an
// observed absence inside the transaction's deadline.
const escalationReserve = 2

// drainMargin is the share of the incumbent's shutdown grace the drain waits
// past it. The ladder abandons its last stage at the grace and parks; the
// margin is the process leaving after a ladder that settled at the wire.
const drainMargin = 4

// RuntimeProof is deploy's absence evidence: the daemon this deployment owns
// was proved gone, and every executable it runs was proved to have no live
// process. Generation names the instance that left, zero when nothing was
// running to begin with. Digest binds the whole observation — the identity
// echo and how absence was reached — into one value a consumer can seal into
// its own records.
type RuntimeProof struct {
	absent     bool
	generation uint64
	digest     SHA256
}

// Absent reports whether the runtime was proved gone. A RuntimeProof only
// exists when it was, so this is false on the zero value alone.
func (p RuntimeProof) Absent() bool { return p.absent }

// Generation names the stopped instance, zero when the daemon was already
// absent and no owner record named one.
func (p RuntimeProof) Generation() uint64 { return p.generation }

// Digest is the exact quiescence evidence digest.
func (p RuntimeProof) Digest() SHA256 { return p.digest }

// ReadinessProof is what a converged generation proved about itself: the
// build serving, the instance serving it, and a digest over the whole health
// observation, the product's own report bytes included.
type ReadinessProof struct {
	build      string
	generation uint64
	digest     SHA256
}

// Build is the exact runtime build proved ready.
func (p ReadinessProof) Build() string { return p.build }

// Generation names the exact instance proved ready.
func (p ReadinessProof) Generation() uint64 { return p.generation }

// Digest is the exact readiness evidence digest.
func (p ReadinessProof) Digest() SHA256 { return p.digest }

// Quiesce proves this deployment's daemon gone and returns the evidence.
//
// The ladder observes before it acts. A live control session reads Health and
// drains the pinned incumbent under an Expect derived from that read, so a
// replacement that raced in between is refused rather than stopped. An
// incumbent already leaving (ErrDraining) or a husk holding the lock with a
// dead listener (ErrAbsent) has no session to pin, and settles from the
// durable owner record instead. A daemon that never recorded itself
// (ErrUnrecorded) is the already-absent arm.
//
// An incumbent that does not leave inside the drain's share of the budget is
// escalated through [daemonkit.Client.Terminate]: SIGTERM at the pinned
// identity, SIGKILL, then observed absence. That reaches a daemon parked over
// abandoned shutdown stages holding its flock, whatever daemonkit it was built
// on, because the ladder runs in the process doing the deploy.
//
// Every arm then terminates what still runs from the bundle's own executables
// — an app extension the system launched, an orphaned helper — through the
// same identity-checked ladder, and ends at the same gate: the executable-
// scoped inventory over every program this deployment runs — at every declared
// host binary, and inside a bundle at every location a whole generation can
// occupy — must be empty. That gate is not a belt-and-braces check, it is what
// closes the hole — see [Inventory] and [Deployment.generationSlots]. A process
// on a declared host binary is refused rather than terminated: it lives outside
// the bundle, and may be the launcher driving this very deploy.
//
// A live process nothing could name counts against that gate when the daemon's
// owner record names its exact pin, so this deployment's own husk refuses while
// a stranger's husk on the same machine does not. The residual is worth stating
// plainly: a husk that never recorded itself is attributable to nothing, and no
// scan of the process table can attribute it — which is why the reap above this
// gate is not optional. It observes a recorded identity out of the table
// directly, including one whose executable is gone.
//
// Like every verb that reaches the control lane, Quiesce requires a context
// carrying a deadline: it is the whole settlement budget, and every stall
// bound inside derives from it.
func (d *Deployment) Quiesce(ctx context.Context) (RuntimeProof, error) {
	if err := d.requireSupportedTransition(); err != nil {
		return RuntimeProof{}, err
	}
	if d.config.Daemon.ShutdownPolicy == daemonkit.PreserveOwned {
		return RuntimeProof{}, fmt.Errorf("%w: preserving deployment requires Replace or Remove", daemonkit.ErrDrainBusy)
	}
	if _, ok := ctx.Deadline(); !ok {
		return RuntimeProof{}, errors.New("deploy: Quiesce requires a context deadline")
	}
	stopped, err := d.stop(ctx)
	if err != nil {
		return RuntimeProof{}, err
	}
	if !absenceProof(stopped.Reap) {
		return RuntimeProof{}, fmt.Errorf("%w: settlement returned reap %d, not an absence proof", ErrConflict, stopped.Reap)
	}
	if err := d.terminateSurvivors(ctx); err != nil {
		return RuntimeProof{}, err
	}
	if err := d.requireEmpty(); err != nil {
		return RuntimeProof{}, err
	}
	return runtimeProof(stopped), nil
}

// absenceProof names the reaps that were reached by observing the process
// table: the three Drain and Settle answer, and ReapTerminated, which the
// escalation ladder answers only after observing the instance it signalled
// leave. ReapUndetermined is a settlement that timed out and proves nothing.
func absenceProof(reap daemonkit.Reap) bool {
	return reap == daemonkit.ReapAbsent || reap == daemonkit.ReapCrossBoot ||
		reap == daemonkit.ReapReused || reap == daemonkit.ReapTerminated
}

// stop drains the incumbent for its own shutdown grace plus a margin — the
// point past which one still in the table has parked over an abandoned stage —
// or the drain's share of the budget, whichever ends first, and hands one that
// is still there to the escalation ladder with the rest. A SIGTERM that lands
// on an incumbent still inside a longer grace of its own is one more drain
// trigger, not a kill. The pin the drain observed is what the ladder is
// addressed to, so an incumbent that was replaced in between is refused rather
// than signalled.
func (d *Deployment) stop(ctx context.Context) (daemonkit.Stopped, error) {
	grace := time.Duration(d.config.Daemon.ShutdownGrace())
	drainCtx, cancel := context.WithTimeout(ctx, min(left(ctx)/escalationReserve, grace+grace/drainMargin))
	defer cancel()
	expect, stopped, err := d.drain(drainCtx)
	switch {
	case err == nil:
		return stopped, nil
	case preservationRefused(err):
		return daemonkit.Stopped{}, err
	case errors.Is(err, daemonkit.ErrUnsettled), timedOut(err):
		return d.client.Terminate(ctx, expect)
	default:
		return daemonkit.Stopped{}, err
	}
}

func (d *Deployment) drain(ctx context.Context) (daemonkit.Expect, daemonkit.Stopped, error) {
	control, err := d.client.Control(ctx)
	switch {
	case errors.Is(err, daemonkit.ErrDraining), errors.Is(err, daemonkit.ErrAbsent):
		if d.config.Daemon.ShutdownPolicy == daemonkit.PreserveOwned {
			return daemonkit.Expect{}, daemonkit.Stopped{}, fmt.Errorf("%w: no prepared drain commitment was acknowledged", daemonkit.ErrDrainBusy)
		}
		return d.settle(ctx)
	case err != nil:
		if d.config.Daemon.ShutdownPolicy == daemonkit.PreserveOwned && timedOut(err) {
			return daemonkit.Expect{}, daemonkit.Stopped{}, fmt.Errorf("%w: drain attachment did not finish", daemonkit.ErrDrainPreparationTimeout)
		}
		return daemonkit.Expect{}, daemonkit.Stopped{}, err
	}
	defer func() { _ = control.Close(ctx) }()
	health, err := control.Health(ctx)
	if err != nil {
		if control.PreservationRequired() {
			return daemonkit.Expect{}, daemonkit.Stopped{}, fmt.Errorf("%w: preserving control health is unavailable", daemonkit.ErrDrainPreparationTimeout)
		}
		return daemonkit.Expect{}, daemonkit.Stopped{}, err
	}
	expect := daemonkit.Expect{Build: health.Build, Generation: health.Generation}
	stopped, err := control.Drain(ctx, expect)
	return expect, stopped, err
}

// settle is the session-less arm. The owner record is what pins it: the build
// and generation read here are what a later escalation must find the record
// still naming. An unrecorded incumbent is the already-absent case: nothing of
// this daemon ever named itself here, so there is no identity to observe out of
// the table and absence rests entirely on the inventory gate Quiesce runs next.
func (d *Deployment) settle(ctx context.Context) (daemonkit.Expect, daemonkit.Stopped, error) {
	owner, recorded, err := proc.ReadOwner(d.config.Daemon.RecordPath())
	if err != nil {
		return daemonkit.Expect{}, daemonkit.Stopped{}, fmt.Errorf("deploy: read owner record: %w", err)
	}
	if !recorded {
		return daemonkit.Expect{}, daemonkit.Stopped{Reap: daemonkit.ReapAbsent}, nil
	}
	expect := daemonkit.Expect{Build: owner.Build, Generation: owner.Generation}
	stopped, err := d.client.Settle(ctx, expect)
	if d.config.Daemon.ShutdownPolicy == daemonkit.PreserveOwned && (errors.Is(err, daemonkit.ErrUnsettled) || timedOut(err)) {
		return expect, stopped, fmt.Errorf("%w: incumbent absence was not proven", daemonkit.ErrDrainPreparationTimeout)
	}
	return expect, stopped, err
}

// terminateSurvivors ends every live process still running one of the bundle's
// own executables, and this deployment's own recorded husk, through the same
// identity-checked ladder the daemon is escalated with. Each is re-pinned
// before every signal, so a PID reused by a stranger is never signalled, and
// each gets an equal share of what is left, so one that ignores SIGTERM cannot
// spend the next one's SIGKILL.
func (d *Deployment) terminateSurvivors(ctx context.Context) error {
	if d.config.Daemon.ShutdownPolicy == daemonkit.PreserveOwned {
		if err := d.requireEmpty(); err != nil {
			return fmt.Errorf("%w: executable inventory is not quiet: %s", daemonkit.ErrDrainBusy, err.Error())
		}
		return nil
	}
	owned, err := d.ownedExecutables()
	if err != nil {
		return err
	}
	survivors, err := d.survivors(owned)
	if err != nil {
		return err
	}
	for i, survivor := range survivors {
		deadline, _ := ctx.Deadline()
		shareCtx, cancel := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(survivors)-i))
		_, err := proc.Terminate(shareCtx, survivor.identity())
		cancel()
		if err != nil {
			return fmt.Errorf("deploy: terminate %s: %w", survivor, err)
		}
	}
	return nil
}

func runtimeProof(stopped daemonkit.Stopped) RuntimeProof {
	h := sha256.New()
	writeDigestField(h, "daemonkit.deploy.runtime.v1")
	writeDigestField(h, strconv.Itoa(stopped.Before.PID))
	writeDigestField(h, stopped.Before.Build)
	writeDigestField(h, strconv.FormatUint(stopped.Before.Generation, 10))
	writeDigestField(h, strconv.Itoa(int(stopped.Reap)))
	var digest SHA256
	copy(digest[:], h.Sum(nil))
	return RuntimeProof{absent: true, generation: stopped.Before.Generation, digest: digest}
}

// prove waits for the daemon to publish readiness and seals what it then
// serves. It waits rather than reads because it runs the instant after launchd
// was asked to start the thing: a single dial answers absent, and a daemon
// that has not finished starting answers a phase that is not ready. The digest
// covers the product's own report bytes, so a consumer's readiness evidence
// rides in without a callback: whatever the product publishes in Health.Detail
// is what deploy seals.
func (d *Deployment) prove(ctx context.Context) (ReadinessProof, error) {
	if _, ok := ctx.Deadline(); !ok {
		return ReadinessProof{}, errors.New("deploy: proving readiness requires a context deadline")
	}
	health, err := d.awaitReady(ctx)
	if err != nil {
		return ReadinessProof{}, err
	}
	if health.Phase != daemonkit.PhaseReady {
		return ReadinessProof{}, fmt.Errorf("%w: daemon is in phase %d, not ready", ErrConflict, health.Phase)
	}
	if health.Build == "" || health.Generation == 0 {
		return ReadinessProof{}, fmt.Errorf("%w: ready daemon reported no build or generation", ErrConflict)
	}
	h := sha256.New()
	writeDigestField(h, "daemonkit.deploy.readiness.v1")
	writeDigestField(h, strconv.Itoa(health.PID))
	writeDigestField(h, strconv.Itoa(int(health.Protocol)))
	writeDigestField(h, health.Build)
	writeDigestField(h, strconv.FormatUint(health.Generation, 10))
	writeDigestField(h, string(health.Detail))
	var digest SHA256
	copy(digest[:], h.Sum(nil))
	return ReadinessProof{build: health.Build, generation: health.Generation, digest: digest}, nil
}

// awaitReady waits out the readiness budget and then answers for whatever it
// found. The wait is what a daemon launchd has only just been asked to start
// needs. The attach after it is what a daemon that will never appear needs:
// the wait derives its retry cadence from the very deadline it is racing, so
// its last attach reports either the absence it found or the timeout it ran
// into, whichever fired first — and Activate's refusal for a daemon that is
// not there is ErrAbsent, every time.
//
// So the wait gives up a share off the end of the budget and one decisive
// attach spends it proving what is actually there. That share is carved out
// twice over, because a deadline bounds nothing precisely: the wait is given
// the earlier deadline so the pair stays inside the caller's budget, and the
// attach is given the share on a clock of its own so a wait that overruns its
// deadline — measured in whole milliseconds under a loaded scheduler — cannot
// eat the answer it was carved out for.
//
// The decisive attach answers with a type, never with a transport error: a
// clock that ran out reached no daemon, and a caller branching on absence
// cannot match on "i/o timeout". A same-UID process holding the socket and
// never speaking is the case that makes this concrete — it fails every attach
// by deadline rather than by refusal, and it is not a daemon.
func (d *Deployment) awaitReady(ctx context.Context) (daemonkit.Health, error) {
	deadline, _ := ctx.Deadline()
	grace := time.Until(deadline) / readinessReserve
	waitCtx, cancelWait := context.WithDeadline(ctx, deadline.Add(-grace))
	defer cancelWait()
	health, err := d.client.WaitReady(waitCtx)
	if err == nil {
		return health, nil
	}
	if !unanswered(err) {
		return daemonkit.Health{}, err
	}
	attachCtx, cancelAttach := attachContext(ctx, grace)
	defer cancelAttach()
	control, attachErr := d.client.Control(attachCtx)
	switch {
	case attachErr == nil:
		_ = control.Close(attachCtx)
		return daemonkit.Health{}, fmt.Errorf(
			"%w: the daemon is listening but published no readiness within the budget", ErrConflict,
		)
	case errors.Is(ctx.Err(), context.Canceled):
		return daemonkit.Health{}, errors.Join(attachErr, ctx.Err())
	case timedOut(attachErr):
		return daemonkit.Health{}, fmt.Errorf(
			"%w: nothing answered the attach that classifies the readiness budget: %w",
			daemonkit.ErrAbsent, attachErr,
		)
	default:
		return daemonkit.Health{}, attachErr
	}
}

// attachContext gives the classifying attach a clock of its own. It is carved
// out of the caller's deadline — that deadline is the very thing the attach
// exists to outlive — but never out of the caller's cancellation: a caller
// that gave up is not asking for one more attach.
func attachContext(ctx context.Context, grace time.Duration) (context.Context, context.CancelFunc) {
	attach, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	stop := context.AfterFunc(ctx, func() {
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			cancel()
		}
	})
	return attach, func() { stop(); cancel() }
}

// unanswered names the wait outcomes that say nothing about the daemon:
// nothing was listening yet, the incumbent was still leaving, or a clock ran
// out mid-attach. Every other refusal — an untrusted server, an incomplete
// pin, a runtime that failed to start — is an answer, and an answer is never
// asked again.
func unanswered(err error) bool {
	return errors.Is(err, daemonkit.ErrAbsent) ||
		errors.Is(err, daemonkit.ErrDraining) ||
		timedOut(err)
}

// timedOut names a clock that ran out rather than an answer: the caller's own
// deadline, or the transport's own — a dial or a handshake that outlives its
// deadline reports os.ErrDeadlineExceeded and never the context error behind
// it.
func left(ctx context.Context) time.Duration {
	deadline, _ := ctx.Deadline()
	return time.Until(deadline)
}

func timedOut(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

func (p ReadinessProof) stored() storedProof {
	return storedProof{Build: p.build, Generation: p.generation, Digest: p.digest.String()}
}

func (p RuntimeProof) stored() storedRuntime {
	return storedRuntime{Absent: p.absent, Generation: p.generation, Digest: p.digest.String()}
}

func preservationRefused(err error) bool {
	return errors.Is(err, daemonkit.ErrDrainBusy) || errors.Is(err, daemonkit.ErrDrainPreparationTimeout)
}
