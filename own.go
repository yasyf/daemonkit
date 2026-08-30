package daemonkit

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
)

// spawnNonceBytes is the attach nonce's fixed width, the one the spawned wire
// session accepts.
const spawnNonceBytes = 32

// OwnProcesses opens a durable process-ownership scope with no daemon, socket,
// or wire: the exclusive lock at recordPath+".lock", a fresh owner generation,
// and reclaim of the prior generation's leaked children — Serve's own
// ownership steps, without Serve. The lock lives inside the store open, so
// Serve takes the identical lock on the identical path: a serving daemon and
// an OwnProcesses scope over the same record exclude each other structurally,
// and a reclaim that would kill a live daemon's children is not constructible.
// Contention returns durable.ErrLockBusy. ctx must carry a deadline; it
// bounds the lock and the reclaim.
func OwnProcesses(ctx context.Context, recordPath string) (*Owned, error) {
	if err := budgeted(ctx, "OwnProcesses"); err != nil {
		return nil, err
	}
	store, err := proc.OpenStore(ctx, recordPath)
	if err != nil {
		return nil, err
	}
	reclaimed, _, recoverErr := store.Recover(ctx)
	if recoverErr != nil {
		slog.Warn("daemonkit: reclaim incomplete; undetermined records kept", "err", recoverErr)
	}
	return newOwned(store, reclaimed), nil
}

// Owned is one open ownership scope.
type Owned struct {
	store     *proc.Store
	reclaimed []Reclaimed

	mu       sync.Mutex
	closed   bool
	starting map[*reservation]struct{}
	children map[*Child]struct{}
	tracked  map[*Tracked]struct{}
}

// reservation is one verb's admission, taken before the verb may start
// anything and resolved by whatever it started. Admission and registration are
// therefore one step under one lock: a verb that reserved is already in the
// set settle observes, well before its child exists, so no process this scope
// started can exist outside that set. child and tracked are written before
// done closes and read only after it.
type reservation struct {
	verb    string
	done    chan struct{}
	child   *Child
	tracked *Tracked
}

func newOwned(store *proc.Store, reclaimed []proc.Reclaimed) *Owned {
	settled := make([]Reclaimed, len(reclaimed))
	for i, entry := range reclaimed {
		settled[i] = Reclaimed{PID: entry.PID, Exit: exitOf(entry.Exit)}
	}
	return &Owned{
		store:     store,
		reclaimed: settled,
		starting:  make(map[*reservation]struct{}),
		children:  make(map[*Child]struct{}),
		tracked:   make(map[*Tracked]struct{}),
	}
}

// Reclaimed is the prior generation's children, already settled at open.
func (o *Owned) Reclaimed() []Reclaimed {
	return append([]Reclaimed(nil), o.reclaimed...)
}

// Ctx mints a Ctx bound to this scope, so product code written against Ctx
// runs identically under a daemon and under a CLI-owned scope — honest in
// production, not a test seam. Context is ctx; Reclaimed is the scope's;
// Report is a no-op, stated: no health lane exists here to publish to; Stop
// cancels Context.
func (o *Owned) Ctx(ctx context.Context) Ctx {
	scoped, cancel := context.WithCancel(ctx)
	return Ctx{
		Context:   scoped,
		Reclaimed: o.Reclaimed(),
		Report:    func([]byte) {},
		Stop:      func(error) { cancel() },
		owner:     o,
	}
}

// Run executes one bounded disposable command to completion, reap included.
// ctx must carry a deadline: the run's whole budget — spawn, exec
// verification, streams, termination, settlement — derives from it, with the
// terminate ladder reserved a tail so settlement is never starved. The
// RunResult is returned alongside any error; the exit lands in *ExitError,
// truncation in ErrTruncated, a failed Cmd.Exec verify in ErrUntrusted, and
// a deadline in an error matching context.DeadlineExceeded. The run's child
// is held for its duration exactly as Spawn's is, so a scope settling under
// an in-flight Run terminates it: that run returns the bytes collected so
// far and an *ExitError naming the fatal signal.
//
// The child gets a dedicated session, always: Cmd.Session is Spawn's posture
// and is refused here, because a disposable command that outlives itself
// through a fork is a leak with no legitimate reading. The run therefore does
// not settle until the whole session has, so a descendant the command forked
// is terminated and proven gone before Run returns — and, being no longer in
// the caller's process group, the command receives neither the terminal's
// signals nor a controlling tty. The one shape outside this scope is a
// descendant that setsid()s out of it: macOS offers no scope that survives
// that, so it is neither signalled nor counted.
func (o *Owned) Run(ctx context.Context, c Cmd) (RunResult, error) {
	if err := budgeted(ctx, "Run"); err != nil {
		return RunResult{}, err
	}
	if err := c.validate("Run", ChannelNone); err != nil {
		return RunResult{}, err
	}
	res, err := o.reserve("Run")
	if err != nil {
		return RunResult{}, err
	}
	defer o.abandon(res)
	result, err := o.store.Run(ctx, command(c, ChannelNone, nil), func(spawned *proc.Child) {
		child := &Child{child: spawned}
		o.hold(res, child)
		go o.releaseOnExit(child)
	})
	if err != nil {
		return RunResult{Exit: exitOf(result.Exit), Stdout: result.Stdout, Stderr: result.Stderr}, err
	}
	out := RunResult{Exit: exitOf(result.Exit), Stdout: result.Stdout, Stderr: result.Stderr}
	var faults []error
	if result.Truncated {
		faults = append(faults, ErrTruncated)
	}
	if !out.Exit.clean() {
		faults = append(faults, &ExitError{Exit: out.Exit})
	}
	if result.Expired {
		expiry := ctx.Err()
		if expiry == nil {
			expiry = context.DeadlineExceeded
		}
		faults = append(faults, expiry)
	}
	return out, errors.Join(faults...)
}

// Spawn starts one long-lived owned child whose record is durable and whose
// Cmd.Exec posture is proven before its first instruction runs, with channel
// established and stderr already draining before release — the child can
// never block on an unwired pipe or race its own recording. The verification
// order is fixed: spawn suspended, record durably, read the suspended child's
// kernel-held code identity in place, release only on success; a failed
// verify aborts through the same path as a failed record and the child never
// executes an instruction. ctx must carry a deadline; it bounds the record
// write, the probe, and the verification — never the child's life. stderr
// nil goes to /dev/null; the copy runs for the child's whole life and a copy
// failure surfaces via Child.StderrErr without touching the child.
func (o *Owned) Spawn(ctx context.Context, c Cmd, channel Channel, stderr io.Writer) (*Child, error) {
	if err := budgeted(ctx, "Spawn"); err != nil {
		return nil, err
	}
	if err := c.validate("Spawn", channel); err != nil {
		return nil, err
	}
	res, err := o.reserve("Spawn")
	if err != nil {
		return nil, err
	}
	defer o.abandon(res)
	var nonce []byte
	if channel == ChannelHandoff {
		nonce = make([]byte, spawnNonceBytes)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("daemonkit: mint the handoff attach nonce: %w", err)
		}
	}
	spawnCmd := command(c, channel, nonce)
	verify := spawnCmd.Verify
	var token proc.AuditToken
	spawnCmd.Verify = func(pid int) error {
		if err := verify(pid); err != nil {
			return err
		}
		minted, err := trust.ProcessToken(pid)
		if err != nil {
			return fmt.Errorf("daemonkit: read the suspended child's audit token: %w", err)
		}
		if !minted.Valid() || minted.PID() != pid {
			return fmt.Errorf("daemonkit: suspended child audit token names pid %d, want %d", minted.PID(), pid)
		}
		token = minted
		return nil
	}
	spawned, err := o.store.Spawn(ctx, spawnCmd, stderr)
	if err != nil {
		return nil, err
	}
	child := &Child{child: spawned, channel: channel, nonce: nonce, limits: c.Limits, token: token}
	o.hold(res, child)
	go o.releaseOnExit(child)
	return child, nil
}

// Adopt records an externally started process — one whose fork the caller had
// to own, a pty child — as a durable session/group under this generation.
// ctx must carry a deadline; it bounds the record's fsync and the process-
// table probe. The caller keeps the reap: daemonkit never wait(2)s an adopted
// process, so the caller's own Wait and this record cannot race a lost
// wakeup. Session leadership is probed, never declared: a leader is recorded
// as its group.
func (o *Owned) Adopt(ctx context.Context, pid int) (*Tracked, error) {
	if err := budgeted(ctx, "Adopt"); err != nil {
		return nil, err
	}
	res, err := o.reserve("Adopt")
	if err != nil {
		return nil, err
	}
	defer o.abandon(res)
	adopted, err := o.store.Adopt(ctx, pid)
	if err != nil {
		return nil, fmt.Errorf("daemonkit: adopt pid %d: %w", pid, err)
	}
	tracked := &Tracked{adopted: adopted, owner: o}
	o.track(res, tracked)
	return tracked, nil
}

// Close settles the scope: every live child and adopted record is terminated
// and proven gone within ctx, the store closes, the lock releases. A scope
// that could not settle everything returns ErrUnsettled joined with the
// detail — the caller's "did everything drain" answer is err == nil.
//
// A verb admitted before the settle is waited out, not raced: a Run or Spawn
// whose child was still being started is settled here like any other, and one
// that has not finished starting when ctx runs out is itself an ErrUnsettled
// fault naming the verb, since whatever it started is durably recorded and the
// next generation reclaims it.
//
// What "terminated and proven gone" covers is exactly the kernel scope each
// verb recorded. A Run child's is its whole dedicated session, descendants
// included. A Spawn child's is its session when the Cmd named one, and the
// child alone otherwise — the descendants of a session-less Spawn are the
// caller's to account for. An Adopt record's is the session the probe found
// it leading, and otherwise the process itself. Nothing here reaches a
// descendant that setsid()s out of its session.
func (o *Owned) Close(ctx context.Context) error {
	return errors.Join(o.settle(ctx), o.store.Close())
}

// settle terminates every live child and adopted record and proves each gone
// within ctx. It closes admission first, so a verb racing the settle is refused
// before it can start anything — and then waits out every verb already admitted
// but still starting, because that verb's admission is what put it in this
// snapshot: whatever it starts is settled here, and a verb that never finishes
// starting within ctx is an ErrUnsettled fault of its own rather than a child
// answered over.
func (o *Owned) settle(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("daemonkit: settling an ownership scope requires a context deadline")
	}
	o.mu.Lock()
	o.closed = true
	starting := make([]*reservation, 0, len(o.starting))
	for res := range o.starting {
		starting = append(starting, res)
	}
	children := make([]*Child, 0, len(o.children))
	for child := range o.children {
		children = append(children, child)
	}
	tracked := make([]*Tracked, 0, len(o.tracked))
	for entry := range o.tracked {
		tracked = append(tracked, entry)
	}
	o.mu.Unlock()

	var faults []error
	for _, child := range children {
		child.child.TerminateBy(deadline)
	}
	for _, res := range starting {
		select {
		case <-res.done:
		case <-ctx.Done():
			faults = append(faults, fmt.Errorf(
				"%s still starting, so whatever it started is left for the next generation: %w",
				res.verb, errors.Join(ErrUnsettled, ctx.Err()),
			))
			continue
		}
		if res.child != nil {
			res.child.child.TerminateBy(deadline)
			children = append(children, res.child)
		}
		if res.tracked != nil {
			tracked = append(tracked, res.tracked)
		}
	}
	for _, child := range children {
		select {
		case exit := <-child.child.Done():
			if exitOf(exit).Reap == ReapUndetermined {
				faults = append(faults, fmt.Errorf("child %d: %w (the settlement ladder timed out unproven; the record is left for the next generation)", child.PID(), ErrUnsettled))
			}
		case <-ctx.Done():
			faults = append(faults, fmt.Errorf("child %d: %w", child.PID(), errors.Join(ErrUnsettled, ctx.Err())))
		}
	}
	for _, entry := range tracked {
		if _, err := entry.Stop(ctx); err != nil {
			faults = append(faults, fmt.Errorf("adopted %d: %w", entry.PID(), err))
		}
	}
	return errors.Join(faults...)
}

func (o *Owned) reserve(verb string) (*reservation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, errScopeSettling
	}
	res := &reservation{verb: verb, done: make(chan struct{})}
	o.starting[res] = struct{}{}
	return res, nil
}

// budgeted is every scope verb's contract on its context: a deadline to settle
// within, and budget still left to settle in. A spent context is refused here,
// before a child exists to abort, so the caller never reads the record store's
// wording — or the lock's, which would name a contention that never happened.
func budgeted(ctx context.Context, verb string) error {
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("daemonkit: %s requires a context deadline", verb)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("daemonkit: %s was handed a context with no budget left: %w", verb, err)
	}
	return nil
}

// hold resolves a reservation with the child its verb started. It cannot
// refuse: a settle that already closed admission is waiting on this very
// reservation, so registering into a settling scope is what gets the child
// terminated instead of missed.
func (o *Owned) hold(res *reservation, child *Child) {
	o.mu.Lock()
	defer o.mu.Unlock()
	res.child = child
	o.children[child] = struct{}{}
	o.resolve(res)
}

// abandon resolves a reservation whose verb started nothing. A verb that
// already resolved its own is unaffected, so it is the safe tail of every verb.
func (o *Owned) abandon(res *reservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.resolve(res)
}

func (o *Owned) resolve(res *reservation) {
	if _, starting := o.starting[res]; !starting {
		return
	}
	delete(o.starting, res)
	close(res.done)
}

// A terminal that proved nothing releases nothing: the child may still be in
// the process table, so it stays registered for settle to fault over.
func (o *Owned) releaseOnExit(child *Child) {
	if exit := <-child.child.Done(); exitOf(exit).Reap == ReapUndetermined {
		return
	}
	o.mu.Lock()
	delete(o.children, child)
	o.mu.Unlock()
}

// track resolves a reservation with the record its Adopt wrote, on hold's
// terms: a settle waiting on this reservation stops the adopted process rather
// than answering over it.
func (o *Owned) track(res *reservation, tracked *Tracked) {
	o.mu.Lock()
	defer o.mu.Unlock()
	res.tracked = tracked
	o.tracked[tracked] = struct{}{}
	o.resolve(res)
}

func (o *Owned) untrack(tracked *Tracked) {
	o.mu.Lock()
	delete(o.tracked, tracked)
	o.mu.Unlock()
}

// Run executes one bounded disposable command under the daemon's ownership,
// with Owned.Run's contract.
func (x Ctx) Run(ctx context.Context, c Cmd) (RunResult, error) {
	if x.owner == nil {
		return RunResult{}, errZeroCtx
	}
	return x.owner.Run(ctx, c)
}

// Spawn starts one long-lived owned child under the daemon's ownership, with
// Owned.Spawn's contract. Serve's shutdown ladder settles what is still live
// when the drain reaches StageChildren.
func (x Ctx) Spawn(ctx context.Context, c Cmd, channel Channel, stderr io.Writer) (*Child, error) {
	if x.owner == nil {
		return nil, errZeroCtx
	}
	return x.owner.Spawn(ctx, c, channel, stderr)
}

// Adopt records an externally started process under the daemon's ownership,
// with Owned.Adopt's contract.
func (x Ctx) Adopt(ctx context.Context, pid int) (*Tracked, error) {
	if x.owner == nil {
		return nil, errZeroCtx
	}
	return x.owner.Adopt(ctx, pid)
}

var errZeroCtx = errors.New("daemonkit: the zero Ctx owns no process scope; Serve and Owned.Ctx are the only things that mint one")

var errScopeSettling = errors.New("daemonkit: the ownership scope is settling and admits no more work")
