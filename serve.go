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

	"github.com/yasyf/daemonkit/internal/flock"
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
	Reclaimed []proc.Reclaimed
	// Report publishes the product's half of Health.Detail, verbatim bytes.
	// A detail past MaxDetail(MaxFrame) is refused whole and logged at error:
	// the last published detail stands and the health verb keeps answering,
	// where a report that cannot be serialized would kill the asking session.
	Report func(detail []byte)
	// Stop begins a product-initiated drain.
	Stop func(error)
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
	Op     string
	Body   []byte
	Caller Caller
}

// Reply is one terminal business response.
type Reply struct{ Body []byte }

// Caller is the requesting peer's kernel identity as data: no methods, no
// authority.
type Caller struct {
	UID uint32
	PID int
}

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
	lock, err := ownListener(ctx, socket, time.Duration(d.shutdownGrace()))
	if err != nil {
		return Drained{}, err
	}
	store, err := proc.OpenStore(el.record())
	if err != nil {
		_ = lock.Close()
		return Drained{}, fmt.Errorf("daemonkit: open record store: %w", err)
	}
	if _, err := store.RecordOwner(build); err != nil {
		_ = store.Close()
		_ = lock.Close()
		return Drained{}, fmt.Errorf("daemonkit: record owner: %w", err)
	}
	recoverCtx, cancelRecover := d.shutdownGrace().mint("recover").Context(ctx)
	reclaimed, archived, recoverErr := store.Recover(recoverCtx, nil)
	cancelRecover()
	if recoverErr != nil {
		slog.Warn("daemonkit: recovery incomplete; undetermined records kept", "err", recoverErr)
	}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		_ = store.Close()
		_ = lock.Close()
		return Drained{}, fmt.Errorf("daemonkit: bind listener: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = ln.Close()
		_ = store.Close()
		_ = lock.Close()
		return Drained{}, fmt.Errorf("daemonkit: chmod listener: %w", err)
	}

	rt := newServeRuntime(int(MaxDetail(d.MaxFrame)))
	server, err := wire.NewServer(rt, wire.Config{
		Schemas:     d.wireSchemas(),
		Trust:       wire.Trust{Control: wireRequirement(d.Trust.Control), Business: wireRequirement(d.Trust.Business)},
		Concurrency: d.Concurrency,
		MaxFrame:    int(d.MaxFrame),
		Handshake:   time.Duration(d.Handshake),
		Serving:     wire.Serving{PID: os.Getpid(), Build: build, Generation: store.Generation(), Detail: rt.reportDetail},
	})
	if err != nil {
		_ = ln.Close()
		_ = store.Close()
		_ = lock.Close()
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
		Reclaimed: reclaimed,
		Report:    rt.report,
		Stop:      func(error) { rt.Drain() },
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

	drained := runShutdownLadder(d.shutdownGrace(), server, product, store, cancelActivation) //nolint:contextcheck // the ladder runs after the caller's ctx is cancelled by design: its own budget is the only deadline
	drained.Archived = archived
	cancelServe()
	if !serveReturned {
		serveErr = <-serveDone
	}
	if len(drained.Abandoned) == 0 {
		_ = lock.Close()
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
	store *proc.Store,
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
	stage(StageChildren, runStage(children, func(tail Budget) error {
		closeErr := store.Close()
		settleCtx, cancel := tail.Context(context.Background())
		defer cancel()
		return errors.Join(closeErr, server.Settle(settleCtx))
	}))
	return drained
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

// ownListener is the fossil listen() ownership primitive: TryAcquire the
// socket-adjacent flock, probe for a live incumbent listener, then wait out
// a dying incumbent's lock bounded by wait.
func ownListener(ctx context.Context, socket string, wait time.Duration) (*flock.Handle, error) {
	spec := flock.Spec{Path: socket + ".lock", Mode: flock.Exclusive, Deadline: wait}
	lock, err := spec.TryAcquire()
	if err != nil && !errors.Is(err, flock.ErrLockBusy) {
		return nil, fmt.Errorf("daemonkit: acquire listener lock: %w", err)
	}
	conn, probeErr := net.DialTimeout("unix", socket, 100*time.Millisecond)
	if probeErr == nil {
		_ = conn.Close()
		if lock != nil {
			_ = lock.Close()
		}
		return nil, ErrBusy
	}
	if !errors.Is(probeErr, os.ErrNotExist) && !errors.Is(probeErr, syscall.ENOENT) && !errors.Is(probeErr, syscall.ECONNREFUSED) {
		if lock != nil {
			_ = lock.Close()
		}
		return nil, fmt.Errorf("daemonkit: probe incumbent listener: %w", probeErr)
	}
	if lock == nil {
		lock, err = spec.Acquire(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: lock held through wait: %w", ErrBusy, err)
		}
		if err != nil {
			return nil, fmt.Errorf("daemonkit: wait for listener lock: %w", err)
		}
	}
	return lock, nil
}

// TODO: digest the image this process is executing — the cdhash csops already
// reads in internal/trust — instead of re-reading the path it was executed
// from. A replace landing between execve and this read has the daemon publish
// and record the replacement's build while running the old code, and no
// launcher can tell. Ensure's placement is serialized by the start lock and the
// pass that made it evicts whatever is serving before it starts anything, so
// what is left is a launcher that died between its own placement and that
// eviction, and an out-of-band replace by a package manager. Against a bundled
// Program the divergence is not merely undetected but silent forever:
// bundled.build re-reads that same replaced path, agrees with the record, and
// every later Ensure decides ActionNothing. Only digesting the running image
// closes it — nothing reachable from the path can.
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
	requirement := &trust.Requirement{
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
	reply, err := product.Handle(ctx, Request{
		Op:     string(req.Op),
		Body:   req.Payload,
		Caller: Caller{UID: uint32(req.Peer.UID), PID: req.Peer.Token.PID()}, //nolint:gosec // kernel UIDs are non-negative
	})
	if err != nil {
		return nil, err
	}
	return reply, nil
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
