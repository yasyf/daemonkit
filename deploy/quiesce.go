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
)

// readinessReserve is the share of the readiness budget the wait gives up to
// the attach that classifies its outcome. A quarter, because that attach is a
// full dial, handshake, trust check and health self-attestation against a
// daemon that may be busy starting — a sliver of the budget times out on a
// daemon that is really there and reports it absent.
const readinessReserve = 4

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
// Every arm ends at the same gate: the executable-scoped inventory over every
// program this deployment runs — at every declared host binary, and inside a
// bundle at every location a whole generation can occupy — must be empty. That
// gate is not a belt-and-braces check, it is what closes the hole — see
// [Inventory] and [Deployment.generationSlots].
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
	if err := d.requireEmpty(); err != nil {
		return RuntimeProof{}, err
	}
	return runtimeProof(stopped), nil
}

// absenceProof names the three reaps that were reached by observing the
// process table. ReapTerminated belongs to signal-delivering child ladders and
// ReapUndetermined to a settlement that timed out; neither proves absence.
func absenceProof(reap daemonkit.Reap) bool {
	return reap == daemonkit.ReapAbsent || reap == daemonkit.ReapCrossBoot || reap == daemonkit.ReapReused
}

func (d *Deployment) stop(ctx context.Context) (daemonkit.Stopped, error) {
	control, err := d.client.Control(ctx)
	switch {
	case errors.Is(err, daemonkit.ErrDraining), errors.Is(err, daemonkit.ErrAbsent):
		return d.settle(ctx)
	case err != nil:
		return daemonkit.Stopped{}, err
	}
	defer func() { _ = control.Close(ctx) }()
	health, err := control.Health(ctx)
	if err != nil {
		return daemonkit.Stopped{}, err
	}
	return control.Drain(ctx, daemonkit.Expect{Build: health.Build, Generation: health.Generation})
}

// settle is the session-less arm. An unrecorded incumbent is the already-
// absent case: nothing of this daemon ever named itself here, so there is no
// identity to observe out of the table and absence rests entirely on the
// inventory gate Quiesce runs next.
func (d *Deployment) settle(ctx context.Context) (daemonkit.Stopped, error) {
	stopped, err := d.client.Settle(ctx, daemonkit.Expect{})
	if errors.Is(err, daemonkit.ErrUnrecorded) {
		return daemonkit.Stopped{Reap: daemonkit.ReapAbsent}, nil
	}
	if err != nil {
		return daemonkit.Stopped{}, err
	}
	return stopped, nil
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
func timedOut(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
}

func (p ReadinessProof) stored() storedProof {
	return storedProof{Build: p.build, Generation: p.generation, Digest: p.digest.String()}
}

func (p RuntimeProof) stored() storedRuntime {
	return storedRuntime{Absent: p.absent, Generation: p.generation, Digest: p.digest.String()}
}
