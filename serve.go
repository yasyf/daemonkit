package daemonkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/internal/wire"
)

// Start builds the product once ownership is proven. Its return IS readiness.
type Start func(Ctx) (Product, error)

// Ctx is what a starting product holds. It carries no knobs.
type Ctx struct {
	// Context is cancelled when the drain begins.
	Context context.Context
	// Reclaimed is the prior generation's children, already settled.
	Reclaimed []Reclaimed
	// Report publishes the product's half of Health.Detail, verbatim bytes.
	// A detail past MaxDetail(MaxFrame) is refused whole and logged at error:
	// the last published detail stands and the health verb keeps answering,
	// where a report that cannot be serialized would kill the asking session.
	Report func(detail []byte)
	// Stop begins a product-initiated drain.
	Stop func(error)

	// owner is the process-ownership scope Run, Spawn, and Adopt dispatch to.
	// Serve and Owned.Ctx are the only things that set it, so the zero Ctx
	// refuses the verbs loudly instead of spawning into a scope nothing owns.
	owner *Owned
}

// Product is the consumer's daemon. Handle owns dispatch — no route table,
// no registry — and its ctx carries the client's own deadline, inherited
// over the wire. Drain and Close each spend a share of the Shutdown budget;
// a stage that outlives its share is abandoned, never joined.
type Product interface {
	Handle(ctx context.Context, req Request) (Reply, error)
	Drain(Budget) error
	Close(Budget) error
}

// Request is one admitted business request.
type Request struct {
	Op   string
	Body []byte
	// Caller is the immediate socket peer's kernel identity as data — behind a
	// byte proxy it is the proxy, not the originator.
	Caller Caller
	// Session names the accepted session this request arrived on.
	Session Session
}

// Reply is one terminal business response.
type Reply struct{ Body []byte }

// Caller is the requesting peer's kernel identity as data: no methods, no
// authority.
type Caller struct {
	UID uint32
	PID int
}

// Session is one accepted client session: a comparable token products key
// per-session state by, with the close signal that releases it.
type Session struct {
	id   uint64
	done <-chan struct{}
}

// ID returns this session's identifier, unique and monotonic within the
// serving process.
func (s Session) ID() uint64 { return s.id }

// Done closes once this exact session has settled and been dropped, so the
// state keyed by it can be released.
func (s Session) Done() <-chan struct{} { return s.done }

// Drained is what a shutdown achieved. A non-empty Abandoned deliberately
// retains the flock and parks the process rather than releasing resources
// over half-done work; launchd's ExitTimeOut — the same Shutdown field —
// is the backstop that SIGKILLs and releases it with the process.
type Drained struct {
	Settled   []Stage
	Abandoned []Stage
	Archived  string
}

// Stage names one shutdown-ladder phase.
type Stage uint8

const (
	// StageIntake closes the listener and typed-rejects new business.
	StageIntake Stage = iota
	// StageRequests joins admitted dispatch and settles terminal acks.
	StageRequests
	// StageProductDrain is the product's own Drain share.
	StageProductDrain
	// StageProductClose is the product's own Close share.
	StageProductClose
	// StageChildren is the guaranteed settlement tail.
	StageChildren
)

const requestSettleTick = 10 * time.Millisecond

const (
	requestsShare = 0.40 / 0.95
	drainShare    = 0.30 / 0.55
	closeShare    = 1.0
)

var drainSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP}

// Serve owns the daemon's whole life in program order: arm signals → flock →
// owner record → recover prior children → bind → start → ready → serve →
// drain → release. The steps are not callable, so they cannot be reordered,
// skipped, or claimed twice, and recovery cannot precede singleton ownership.
//
// The drain — triggered by ctx, a signal, Ctx.Stop, Control.Drain, or the
// wire verb: one path, five triggers — runs the Shutdown ladder on Serve's
// own goroutine and ends by cancelling the serve context it handed the wire
// server, so the accept park unblocks, Serve returns, and the process exits
// by itself. No signal is sent and launchd does nothing on the happy path.
func Serve(ctx context.Context, d Daemon, start Start) (Drained, error) {
	if err := d.ValidateForServe(); err != nil {
		return Drained{}, err
	}
	if start == nil {
		return Drained{}, errors.New("daemonkit: Start is required")
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, drainSignals...)
	defer signal.Stop(signals)

	el, err := d.Label.element()
	if err != nil {
		return Drained{}, err
	}
	socket, err := el.socket()
	if err != nil {
		return Drained{}, fmt.Errorf("daemonkit: derive socket path: %w", err)
	}
	if err := el.state().EnsureStateDir(); err != nil {
		return Drained{}, fmt.Errorf("daemonkit: create state dir: %w", err)
	}
	build, err := buildDigest()
	if err != nil {
		return Drained{}, err
	}
	if err := probeIncumbent(socket); err != nil {
		return Drained{}, err
	}
	ownCtx, cancelOwn := d.shutdownGrace().mint("own").Context(ctx)
	store, err := proc.OpenStore(ownCtx, el.record())
	cancelOwn()
	if err != nil {
		if errors.Is(err, durable.ErrLockBusy) {
			return Drained{}, fmt.Errorf("%w: record store lock held through the wait: %w", ErrBusy, err)
		}
		return Drained{}, fmt.Errorf("daemonkit: open record store: %w", err)
	}
	if _, err := store.RecordOwner(build); err != nil {
		_ = store.Close()
		return Drained{}, fmt.Errorf("daemonkit: record owner: %w", err)
	}
	recoverCtx, cancelRecover := d.shutdownGrace().mint("recover").Context(ctx)
	reclaimed, archived, recoverErr := store.Recover(recoverCtx, nil)
	cancelRecover()
	if recoverErr != nil {
		slog.Warn("daemonkit: recovery incomplete; undetermined records kept", "err", recoverErr)
	}
	owned := newOwned(store, reclaimed)
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		_ = store.Close()
		return Drained{}, fmt.Errorf("daemonkit: bind listener: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = ln.Close()
		_ = store.Close()
		return Drained{}, fmt.Errorf("daemonkit: chmod listener: %w", err)
	}

	rt := newServeRuntime(int(MaxDetail(d.MaxFrame)))
	server, err := wire.NewServer(rt, wire.Config{
		Schemas:     d.wireSchemas(),
		Trust:       wire.Trust{Control: wireRequirement(d.Trust.Control), Business: wireRequirements(d.Trust.Business)},
		Concurrency: d.Concurrency,
		MaxFrame:    int(d.MaxFrame),
		Handshake:   time.Duration(d.Handshake),
		Serving:     wire.Serving{PID: os.Getpid(), Build: build, Generation: store.Generation(), Detail: rt.reportDetail},
	})
	if err != nil {
		_ = ln.Close()
		_ = store.Close()
		return Drained{}, err
	}

	serveCtx, cancelServe := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveCtx, ln) }()

	stopWatch := context.AfterFunc(ctx, rt.Drain)
	defer stopWatch()
	armDone := make(chan struct{})
	stopForwarder := sync.OnceFunc(func() { close(armDone) })
	defer stopForwarder()
	go func() {
		select {
		case <-signals:
			rt.Drain()
		case <-armDone:
		}
	}()

	activationCtx, cancelActivation := context.WithCancel(rt.stopped)
	defer cancelActivation()
	product, startErr := start(Ctx{
		Context:   activationCtx,
		Reclaimed: owned.Reclaimed(),
		Report:    rt.report,
		Stop:      func(error) { rt.Drain() },
		owner:     owned,
	})
	var serveErr error
	serveReturned := false
	if startErr != nil {
		rt.fail()
		product = nil
	} else {
		rt.ready(product)
		select {
		case <-rt.stopped.Done():
		case serveErr = <-serveDone:
			serveReturned = true
			rt.Drain()
		}
	}

	drained := runShutdownLadder(d.shutdownGrace(), server, product, owned, cancelActivation) //nolint:contextcheck // the ladder runs after the caller's ctx is cancelled by design: its own budget is the only deadline
	drained.Archived = archived
	cancelServe()
	if !serveReturned {
		serveErr = <-serveDone
	}
	if len(drained.Abandoned) == 0 {
		_ = owned.store.Close()
		return drained, errors.Join(startErr, serveErr)
	}
	slog.Error("daemonkit: shutdown abandoned stages; retaining flock until stopped",
		"abandoned", len(drained.Abandoned))
	park(signals, stopForwarder)
	return drained, errors.Join(startErr, serveErr)
}

// park holds the flock over half-done work until a signal arrives. The wait is
// a registration of its own, made before the drain-trigger registration is
// dropped so no signal can reach the default disposition, and made after the
// trigger was delivered so the signal that began this drain cannot unpark it.
func park(triggers chan os.Signal, stopForwarder func()) {
	parked := make(chan os.Signal, 1)
	signal.Notify(parked, drainSignals...)
	defer signal.Stop(parked)
	signal.Stop(triggers)
	stopForwarder()
	<-parked
}

// runShutdownLadder spends the Shutdown budget in the locked order: children
// reserved as the guaranteed tail, then intake, requests, product drain, and
// product close as shares of the work window. A stage that outlives its share
// is abandoned and the ladder keeps moving; an in-time error settles the
// stage and is logged — only expiry abandons.
//
// The locked shares of the work window are intake 0.05, requests 0.40, drain
// 0.30, close 0.25, while Share carves a fraction of what remains. Each
// literal below is therefore its locked share over the window still unspent
// when it is carved, so a stage that spends its whole share leaves its
// successor exactly the locked fraction and an early finish flows forward.
// Intake takes no Budget — CloseIntake does not block — so its share is spent
// only as the denominator of the requests literal.
func runShutdownLadder(
	shutdown Grace,
	server *wire.Server,
	product Product,
	owned *Owned,
	cancelActivation context.CancelFunc,
) Drained {
	budget := shutdown.mint("shutdown")
	work, children := budget.Reserve("children", 0.15)
	var drained Drained
	stage := func(s Stage, settled bool) {
		if settled {
			drained.Settled = append(drained.Settled, s)
		} else {
			drained.Abandoned = append(drained.Abandoned, s)
		}
	}
	cancelActivation()
	stage(StageIntake, server.CloseIntake() == nil)
	stage(StageRequests, settleRequests(work.Share("requests", requestsShare), server))
	if product != nil {
		stage(StageProductDrain, runStage(work.Share("drain", drainShare), product.Drain))
		stage(StageProductClose, runStage(work.Share("close", closeShare), product.Close))
	}
	stage(StageChildren, provenStage(children, func(tail Budget) error {
		settleCtx, cancel := tail.Context(context.Background())
		defer cancel()
		return errors.Join(owned.settle(settleCtx), server.Settle(settleCtx))
	}))
	return drained
}

// provenStage is runStage for the one stage whose error IS the verdict: the
// children tail answers "did everything drain", so an in-time ErrUnsettled
// abandons rather than releasing the flock over a process still in the table.
// Every other stage settles on an in-time failure, where the error names work
// that ran and failed rather than resources still held.
func provenStage(budget Budget, run func(Budget) error) bool {
	proven := make(chan bool, 1)
	inTime := runStage(budget, func(b Budget) error {
		err := run(b)
		proven <- err == nil
		return err
	})
	return inTime && <-proven
}

// settleRequests joins admitted dispatch and waits every written terminal's
// acknowledgment within the share; at expiry it cancels what remains and the
// stage is abandoned.
func settleRequests(budget Budget, server *wire.Server) bool {
	ctx, cancel := budget.Context(context.Background())
	defer cancel()
	stop := context.AfterFunc(ctx, server.CancelRequests)
	defer stop()
	for server.InFlight() > 0 {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(requestSettleTick):
		}
	}
	return server.Settle(ctx) == nil
}

// runStage refuses to start a stage whose share is already spent, and
// classifies the stage it does start by what the share had left when the work
// returned — never by which arm of a select won. A stage bounded by its own
// share deadline finishes as its timer fires, so a verdict read off the select
// is a coin flip between settled and abandoned, and whether the ladder releases
// the flock or parks the process hangs on that bit.
func runStage(budget Budget, run func(Budget) error) bool {
	if budget.Left() <= 0 {
		return false
	}
	inTime := make(chan bool, 1)
	go func() {
		err := run(budget)
		settled := budget.Left() > 0
		if err != nil {
			slog.Warn("daemonkit: shutdown stage failed", "err", err)
		}
		inTime <- settled
	}()
	timer := time.NewTimer(budget.Left())
	defer timer.Stop()
	select {
	case settled := <-inTime:
		return settled
	case <-timer.C:
		return false
	}
}

// probeIncumbent is the fossil listen() ownership probe: a socket that still
// accepts names a live incumbent, and no takeover exists here. Singleton
// ownership itself lives in the record store's lock, which the store open
// takes, so an OwnProcesses scope over the same record excludes a serving
// daemon and cannot reclaim its children.
func probeIncumbent(socket string) error {
	conn, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return ErrBusy
	}
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("daemonkit: probe incumbent listener: %w", err)
	}
	return nil
}

// buildDigest re-reads the path this process was executed from, and carries a
// documented residual: a replace landing between execve and this read has the
// daemon publish and record the replacement's build while running the old
// code. Ensure's placement is serialized by the start lock and evicts whatever
// is serving first, so the triggering agent is an out-of-band replace — a
// package manager bypassing deploy's sealed supersession — racing a launchd
// restart, a window of microseconds at daemon start. Against a copied Program
// the next Ensure evicts; against a bundled one bundled.build re-reads the
// same replaced path, agrees with the record, and every later Ensure decides
// ActionNothing — silent forever. Nothing reachable from the path can close
// it. The fix is digesting the running image: a CS_OPS_CDHASH self-read
// beside internal/trust's peer path, with the owner record carrying cdhash
// alongside sha256 for one release cycle — build identity is
// record-schema-visible, so that is a v0.22+ change riding the soak gate.
func buildDigest() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("daemonkit: resolve current executable: %w", err)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		return "", fmt.Errorf("daemonkit: read executable %q: %w", exe, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func wireRequirement(r *Requirement) *trust.Requirement {
	if r == nil {
		return nil
	}
	requirement := wiredRequirement(*r)
	return &requirement
}

func wireRequirements(rs Requirements) []trust.Requirement {
	if rs == nil {
		return nil
	}
	wired := make([]trust.Requirement, len(rs))
	for i, r := range rs {
		wired[i] = wiredRequirement(r)
	}
	return wired
}

func wiredRequirement(r Requirement) trust.Requirement {
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
	return requirement
}

// serveRuntime is the wire.Runtime inside Serve: one in-memory phase, the
// product mux, and the stop context every drain trigger lands on — the one
// Serve's ladder waits for and the one a blocked Start is released by.
type serveRuntime struct {
	mu        sync.Mutex
	snapshot  wire.PhaseSnapshot
	changed   chan struct{}
	product   Product
	detail    []byte
	maxDetail int

	stopped context.Context
	signal  context.CancelFunc
}

func newServeRuntime(maxDetail int) *serveRuntime {
	stopped, signalStop := context.WithCancel(context.Background())
	return &serveRuntime{
		snapshot:  wire.PhaseSnapshot{Sequence: 1, Phase: wire.PhaseStarting},
		changed:   make(chan struct{}),
		maxDetail: maxDetail,
		stopped:   stopped,
		signal:    signalStop,
	}
}

func (r *serveRuntime) Handle(ctx context.Context, req wire.Request) (any, error) {
	r.mu.Lock()
	product := r.product
	r.mu.Unlock()
	return handleBusiness(ctx, req, product.Handle)
}

func (r *serveRuntime) Phase() wire.PhaseSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshot
}

func (r *serveRuntime) WaitPhase(ctx context.Context, after uint64) (wire.PhaseSnapshot, error) {
	for {
		r.mu.Lock()
		snapshot := r.snapshot
		changed := r.changed
		r.mu.Unlock()
		if snapshot.Sequence > after {
			return snapshot, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return wire.PhaseSnapshot{}, ctx.Err()
		}
	}
}

// Drain publishes PhaseDraining synchronously, then signals the ladder — the
// structural heir of the deleted daemon.Runtime.signalStop. The wire server's
// executeDrain re-reads Phase right after Drain returns, so the publication
// can never lag the ack; the ladder's executor is Serve's own goroutine,
// never this caller.
func (r *serveRuntime) Drain() {
	r.publish(wire.PhaseDraining)
	r.signal()
}

func (r *serveRuntime) ready(product Product) {
	r.mu.Lock()
	r.product = product
	r.mu.Unlock()
	r.publish(wire.PhaseReady)
}

func (r *serveRuntime) fail() {
	r.publish(wire.PhaseFailed)
}

// publish advances the phase stream; a terminal phase is never overwritten.
func (r *serveRuntime) publish(phase wire.Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot.Phase == wire.PhaseDraining || r.snapshot.Phase == wire.PhaseFailed {
		return
	}
	r.snapshot = wire.PhaseSnapshot{Sequence: r.snapshot.Sequence + 1, Phase: phase}
	changed := r.changed
	r.changed = make(chan struct{})
	close(changed)
}

func (r *serveRuntime) report(detail []byte) {
	if len(detail) > r.maxDetail {
		slog.Error("daemonkit: refusing an oversized product report",
			"bytes", len(detail), "max", r.maxDetail)
		return
	}
	copied := append([]byte(nil), detail...)
	r.mu.Lock()
	r.detail = copied
	r.mu.Unlock()
}

func (r *serveRuntime) reportDetail() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.detail...)
}
