package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"syscall"
	"time"
)

// DefaultReapGrace bounds the wait between an orphan's SIGTERM and its SIGKILL.
const DefaultReapGrace = 5 * time.Second

// DefaultReapSettlement bounds post-SIGKILL identity polling.
const DefaultReapSettlement = 2 * time.Second

const (
	settlementPollInterval = 10 * time.Millisecond
	recordSchemaVersion    = 1
)

// errNoProc is a definitive "gone", distinct from a probe failure (Undetermined, fails closed).
var errNoProc = errors.New("no such process")

var (
	// ErrInvalidRecord means a durable process record lacks required identity.
	ErrInvalidRecord = errors.New("proc: invalid durable process record")
	// ErrIdentityChanged means a process no longer has the exact boot/start identity supplied by its authenticated peer.
	ErrIdentityChanged = errors.New("proc: process identity changed")
	// ErrRecordSchema means a durable process store is not the exact current format.
	ErrRecordSchema = errors.New("proc: unsupported durable process record schema")
)

// Record identifies one spawned child across daemon generations. Reap pairs
// PID with the boot session and opaque kernel start stamp before signaling;
// PID alone is never kill authority, while Comm remains informational across exec.
type Record struct {
	// RecoveryClass names the consumer barrier that must settle before the
	// retirement receipt can be acknowledged.
	RecoveryClass RecoveryClass `json:"recovery_class"`
	// PID is the spawned child's process id.
	PID int `json:"pid"`
	// StartTime is the prober's opaque, platform-native process start stamp.
	StartTime string `json:"start_time"`
	// Boot is the kernel boot session in which StartTime was captured.
	Boot string `json:"boot"`
	// Comm is the child's initial OS-reported (truncated) process name.
	Comm string `json:"comm"`
	// Executable is the exact kernel-resolved path. On Darwin it is bound to
	// AuditToken; other platforms bind it with Boot and StartTime.
	Executable string `json:"executable,omitempty"`
	// AuditToken is Darwin's stable (pid, pidversion) kill authority for a
	// protected peer. Spawned disposable workers use the zero value.
	AuditToken AuditToken `json:"audit_token,omitzero"`
	// Generation tags the daemon instance that spawned the child.
	Generation string `json:"generation"`
	// ProcessGroup means PID is also the process-group id and signals target the
	// entire group after its dedicated session membership is revalidated.
	ProcessGroup bool `json:"process_group"`
	// SessionID is the dedicated session created with a process-group leader.
	// It remains the group's durable kernel identity after the leader exits.
	SessionID int `json:"session_id,omitempty"`
	// Role, RuntimeBuild, RuntimeProtocol, TargetProcessGeneration, Intent, and ExpiresUnixMilli are
	// present only for one-shot stop-control process receipts.
	Role                    string `json:"role,omitempty"`
	RuntimeBuild            string `json:"runtime_build,omitempty"`
	RuntimeProtocol         int    `json:"runtime_protocol,omitempty"`
	TargetProcessGeneration string `json:"target_process_generation,omitempty"`
	Intent                  string `json:"intent,omitempty"`
	ExpiresUnixMilli        int64  `json:"expires_unix_milli,omitempty"`
}

// Validate rejects an incomplete durable process identity.
func (r Record) Validate() error {
	if err := r.RecoveryClass.Validate(); err != nil {
		return errors.Join(ErrInvalidRecord, err)
	}
	if err := validateRecordIdentity(r); err != nil {
		return err
	}
	if r.Generation == "" {
		return fmt.Errorf("%w: generation is required", ErrInvalidRecord)
	}
	if r.ProcessGroup {
		if r.PID <= 1 || r.SessionID != r.PID {
			return fmt.Errorf("%w: process group requires a dedicated session leader", ErrInvalidRecord)
		}
	} else if r.SessionID != 0 {
		return fmt.Errorf("%w: non-group record has a session id", ErrInvalidRecord)
	}
	if r.RecoveryClass == RecoveryStopControl {
		if r.Role == "" || r.RuntimeBuild == "" || r.RuntimeProtocol <= 0 || r.TargetProcessGeneration == "" ||
			(r.Intent != "upgrade" && r.Intent != "restart" && r.Intent != "uninstall") ||
			r.ExpiresUnixMilli <= 0 || r.ProcessGroup {
			return fmt.Errorf("%w: incomplete stop-control authority", ErrInvalidRecord)
		}
	} else if r.Role != "" || r.RuntimeBuild != "" || r.RuntimeProtocol != 0 || r.TargetProcessGeneration != "" ||
		r.Intent != "" || r.ExpiresUnixMilli != 0 {
		return fmt.Errorf("%w: non-stop record carries stop authority", ErrInvalidRecord)
	}
	return nil
}

func validateRecordIdentity(r Record) error {
	if r.PID <= 0 {
		return fmt.Errorf("%w: pid is required", ErrInvalidRecord)
	}
	if r.StartTime == "" {
		return fmt.Errorf("%w: start time is required", ErrInvalidRecord)
	}
	if r.Boot == "" {
		return fmt.Errorf("%w: boot is required", ErrInvalidRecord)
	}
	if !r.AuditToken.IsZero() {
		if err := validateAuditToken(r.AuditToken, r.PID); err != nil {
			return err
		}
		if r.Executable == "" {
			return fmt.Errorf("%w: audit-token record requires executable", ErrInvalidRecord)
		}
	}
	return nil
}

// Store persists orphan Records across daemon generations; implementations
// serialize read-modify-writes so a spawning daemon's Add never races a
// successor's Remove.
type Store interface {
	// Add records a spawned child, replacing any prior record for the same
	// process instance (PID + StartTime + Boot).
	Add(ctx context.Context, rec Record) error
	// Load returns every stored record.
	Load(ctx context.Context) ([]Record, error)
	// Remove deletes only complete exact record matches. A newer generation
	// owning the same process instance is never removed by stale cleanup.
	Remove(ctx context.Context, victims []Record) error
	// BeginReap durably claims an exact prior-generation record so concurrent
	// graceful untracking cannot erase it before receipt commit.
	BeginReap(ctx context.Context, rec Record, reaperGeneration string) error
	// CommitReap atomically replaces one exact process record with its ordered
	// durable retirement receipt.
	CommitReap(ctx context.Context, rec Record, reaperGeneration string, outcome ReapOutcome) (ReapReceipt, error)
	// LoadReapReceipts returns a bounded stable class page.
	LoadReapReceipts(
		ctx context.Context,
		class RecoveryClass,
		after ReapReceiptCursor,
		limit int,
	) (ReapReceiptPage, error)
	// HasReapReceipt reports an exact durable receipt match.
	HasReapReceipt(ctx context.Context, receipt ReapReceipt) (bool, error)
	// FindReapReceipt returns the durable receipt for one exact process record,
	// independent of bounded page position.
	FindReapReceipt(ctx context.Context, record Record) (ReapReceipt, bool, error)
	// AcknowledgeReap forgets an exact receipt; absence is idempotent.
	AcknowledgeReap(ctx context.Context, receipt ReapReceipt) (ReapReceiptFloor, error)
	// ReapReceiptFloor returns the durable contiguous acknowledgement floor.
	ReapReceiptFloor(ctx context.Context, class RecoveryClass) (ReapReceiptFloor, error)
}

type procInfo struct {
	startTime string
	comm      string
	groupID   int
	sessionID int
	zombie    bool
}

type groupMember struct {
	pid  int
	info procInfo
}

type prober interface {
	probe(pid int) (procInfo, error)
	groupMembers(groupID, sessionID int) ([]groupMember, error)
	bootID() (string, error)
}

type sysProber struct{}

func (sysProber) probe(pid int) (procInfo, error) { return probeProc(pid) }

func (sysProber) groupMembers(groupID, sessionID int) ([]groupMember, error) {
	return probeGroupMembers(groupID, sessionID)
}

func (sysProber) bootID() (string, error) { return BootID() }

type signaler interface {
	signal(pid int, sig syscall.Signal) error
}

type sysSignaler struct{}

func (sysSignaler) signal(pid int, sig syscall.Signal) error { return syscall.Kill(pid, sig) }

// Reaper reaps provably-ours orphaned children of a prior daemon generation:
// build one with a fresh unique Generation and run one Reap at cold start,
// before accepting registrations. Reap signals only a record whose process or
// dedicated-session identity is revalidated and whose generation differs;
// any unresolved probe fails closed.
type Reaper struct {
	// Store persists orphan records across generations. Required.
	Store Store
	// Generation uniquely identifies this daemon instance; children Track records
	// carry it, and records bearing it are never signaled by this reaper. Required.
	Generation string
	// Grace bounds the wait between SIGTERM and SIGKILL; zero means DefaultReapGrace.
	Grace time.Duration
	// Settlement bounds post-SIGKILL proof; zero means DefaultReapSettlement.
	Settlement time.Duration

	prober      prober
	signaler    signaler
	auditSignal func(AuditToken, syscall.Signal) (bool, error)
	auditPath   func(AuditToken) (string, error)
	clock       clock
}

// Track snapshots a freshly spawned child's identity through the same prober
// Reap revalidates with and records it under this reaper's Generation.
func (r *Reaper) Track(ctx context.Context, pid int, class RecoveryClass) (Record, error) {
	return r.track(ctx, pid, false, class)
}

// TrackIdentity durably records an already authenticated exact process identity.
// It re-probes before writing so PID reuse can never turn the supplied identity
// into authority over a different process.
func (r *Reaper) TrackIdentity(ctx context.Context, identity Identity, class RecoveryClass) (Record, error) {
	rec := Record{
		RecoveryClass: class,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Generation: r.Generation, Executable: identity.Executable,
		AuditToken: identity.AuditToken,
	}
	return r.trackIdentityRecord(ctx, rec)
}

func (r *Reaper) trackIdentityRecord(ctx context.Context, rec Record) (Record, error) {
	if err := rec.Validate(); err != nil {
		return Record{}, err
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return Record{}, fmt.Errorf("revalidate boot identity: %w", err)
	}
	if boot != rec.Boot {
		return Record{}, ErrIdentityChanged
	}
	if rec.AuditToken.Valid() {
		path, err := r.auditExecutable(rec.AuditToken)
		if err != nil {
			return Record{}, fmt.Errorf("revalidate audit-token process: %w", err)
		}
		if path != rec.Executable {
			return Record{}, ErrIdentityChanged
		}
	}
	info, err := r.prb().probe(rec.PID)
	if errors.Is(err, errNoProc) {
		return Record{}, ErrNoProcess
	}
	if err != nil {
		return Record{}, fmt.Errorf("revalidate pid %d: %w", rec.PID, err)
	}
	if info.startTime != rec.StartTime || info.zombie {
		return Record{}, ErrIdentityChanged
	}
	if rec.AuditToken.Valid() {
		path, err := r.auditExecutable(rec.AuditToken)
		if err != nil {
			return Record{}, fmt.Errorf("settle audit-token process identity: %w", err)
		}
		if path != rec.Executable {
			return Record{}, ErrIdentityChanged
		}
	}
	if err := r.Store.Add(ctx, rec); err != nil {
		return Record{}, fmt.Errorf("record authenticated process %d: %w", rec.PID, err)
	}
	return rec, nil
}

// TrackStopControl durably authorizes one exact already-running child to issue
// one bounded stop request against one target runtime generation.
func (r *Reaper) TrackStopControl(
	ctx context.Context,
	identity Identity,
	role, build string,
	protocol int,
	targetProcessGeneration string,
	intent string,
	expires time.Time,
) (Record, error) {
	record := Record{
		RecoveryClass: RecoveryStopControl,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Generation: r.Generation, Executable: identity.Executable,
		AuditToken: identity.AuditToken, Role: role, RuntimeBuild: build, RuntimeProtocol: protocol,
		TargetProcessGeneration: targetProcessGeneration, Intent: intent, ExpiresUnixMilli: expires.UnixMilli(),
	}
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	return r.trackIdentityRecord(ctx, record)
}

// StopControlStore atomically consumes one exact one-shot stop authority.
type StopControlStore interface {
	ConsumeStopControl(context.Context, Identity, string, string, time.Time) (Record, bool, error)
}

// TrackGroup records a child whose PID leads its own process group and session.
func (r *Reaper) TrackGroup(ctx context.Context, pid int, class RecoveryClass) (Record, error) {
	return r.track(ctx, pid, true, class)
}

func (r *Reaper) track(ctx context.Context, pid int, processGroup bool, class RecoveryClass) (Record, error) {
	boot, err := r.prb().bootID()
	if err != nil {
		return Record{}, fmt.Errorf("snapshot boot identity: %w", err)
	}
	info, err := r.prb().probe(pid)
	if err != nil {
		return Record{}, fmt.Errorf("snapshot pid %d: %w", pid, err)
	}
	if processGroup && (info.groupID != pid || info.sessionID != pid) {
		return Record{}, fmt.Errorf("pid %d has process group %d and session %d, want a dedicated session leader", pid, info.groupID, info.sessionID)
	}
	rec := Record{
		RecoveryClass: class,
		PID:           pid,
		StartTime:     info.startTime,
		Boot:          boot,
		Comm:          info.comm,
		Generation:    r.Generation,
		ProcessGroup:  processGroup,
	}
	if processGroup {
		rec.SessionID = info.sessionID
	}
	if err := r.Store.Add(ctx, rec); err != nil {
		return Record{}, fmt.Errorf("record child %d: %w", pid, err)
	}
	return rec, nil
}

// Untrack removes a synchronously reaped child from the durable orphan store.
func (r *Reaper) Untrack(ctx context.Context, rec Record) error {
	if err := r.Store.Remove(ctx, []Record{rec}); err != nil {
		return fmt.Errorf("remove child %d: %w", rec.PID, err)
	}
	return nil
}

// Terminate delivers the bounded TERM/KILL ladder to one exact durably tracked
// process or process group and removes its record only after absence, reuse, or
// settlement is proven.
func (r *Reaper) Terminate(ctx context.Context, rec Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	records, err := r.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load tracked process: %w", err)
	}
	tracked := false
	for _, candidate := range records {
		if candidate == rec {
			tracked = true
			break
		}
	}
	if !tracked {
		return errors.New("proc: process is not durably tracked")
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return fmt.Errorf("load current boot identity: %w", err)
	}
	reaped, err := r.terminateOne(ctx, rec, boot)
	if err != nil {
		return err
	}
	if !reaped {
		return errors.New("proc: tracked process remained live")
	}
	if err := r.Store.Remove(ctx, []Record{rec}); err != nil {
		return fmt.Errorf("remove terminated process %d: %w", rec.PID, err)
	}
	return nil
}

// TerminateWithin applies Terminate with an exact SIGTERM grace without
// mutating the reaper's policy for any other process.
func (r *Reaper) TerminateWithin(ctx context.Context, rec Record, grace time.Duration) error {
	if grace <= 0 {
		return errors.New("proc: termination grace must be positive")
	}
	clone := *r
	clone.Grace = grace
	return clone.Terminate(ctx, rec)
}

// TerminateIdentityWithin applies the exact TERM/KILL ladder to a live process
// identity without requiring its one-shot authorization record to remain in
// the store. It is intended for the controller that directly spawned and must
// synchronously reap that process.
func (r *Reaper) TerminateIdentityWithin(ctx context.Context, identity Identity, grace time.Duration) error {
	if grace <= 0 {
		return errors.New("proc: termination grace must be positive")
	}
	record := Record{
		RecoveryClass: RecoveryTask,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Generation: r.Generation, Executable: identity.Executable,
		AuditToken: identity.AuditToken,
	}
	if identity.AuditToken.IsZero() {
		record.Executable = ""
	}
	if err := record.Validate(); err != nil {
		return err
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return fmt.Errorf("load current boot identity: %w", err)
	}
	clone := *r
	clone.Grace = grace
	settled, err := clone.terminateOne(ctx, record, boot)
	if err != nil {
		return err
	}
	if !settled {
		return errors.New("proc: process remained live")
	}
	return nil
}

func (r *Reaper) terminateOne(ctx context.Context, rec Record, boot string) (bool, error) {
	if rec.Boot != boot {
		return true, nil
	}
	if rec.PID <= 1 || rec.PID == os.Getpid() {
		return false, errors.New("refusing unsafe process identity")
	}
	if gone, err := r.auditRecordGone(rec); gone || err != nil {
		return gone, err
	}
	info, err := r.prb().probe(rec.PID)
	if rec.ProcessGroup {
		return r.reapGroup(ctx, rec, info, err, true)
	}
	switch {
	case errors.Is(err, errNoProc):
		return true, nil
	case err != nil:
		return false, err
	case info.startTime != rec.StartTime, info.zombie:
		return true, nil
	default:
		return r.reapOrphan(ctx, rec)
	}
}

// Owns reports whether rec still identifies the same live process instance.
func (r *Reaper) Owns(rec Record) (bool, error) {
	if err := rec.Validate(); err != nil {
		return false, err
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return false, fmt.Errorf("revalidate boot identity: %w", err)
	}
	if rec.Boot != boot {
		return false, nil
	}
	if gone, err := r.auditRecordGone(rec); gone || err != nil {
		return false, err
	}
	info, err := r.prb().probe(rec.PID)
	if errors.Is(err, errNoProc) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("revalidate pid %d: %w", rec.PID, err)
	}
	if info.startTime != rec.StartTime {
		return false, nil
	}
	if info.zombie {
		return false, nil
	}
	if rec.ProcessGroup && (info.groupID != rec.PID || info.sessionID != rec.SessionID) {
		return false, nil
	}
	return true, nil
}

// Reap settles every prior-generation record and atomically replaces each
// settled kill authority with an ordered durable receipt.
func (r *Reaper) Reap(ctx context.Context) error {
	recs, err := r.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load reaper records: %w", err)
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return fmt.Errorf("load current boot identity: %w", err)
	}
	var unresolved []error
	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rec.Generation == r.Generation {
			continue
		}
		if claimErr := r.Store.BeginReap(ctx, rec, r.Generation); claimErr != nil {
			unresolved = append(unresolved, fmt.Errorf("claim child %d for reap: %w", rec.PID, claimErr))
			continue
		}
		reaped, outcome, reapErr := r.reapOne(ctx, rec, boot)
		if reapErr != nil {
			unresolved = append(unresolved, fmt.Errorf("reap child %d: %w", rec.PID, reapErr))
		}
		if reaped {
			if _, commitErr := r.Store.CommitReap(ctx, rec, r.Generation, outcome); commitErr != nil {
				unresolved = append(unresolved, fmt.Errorf("commit receipt for child %d: %w", rec.PID, commitErr))
				continue
			}
		}
	}
	return errors.Join(unresolved...)
}

// ReapReceipts returns one stable class-filtered ledger page.
func (r *Reaper) ReapReceipts(
	ctx context.Context,
	class RecoveryClass,
	after ReapReceiptCursor,
	limit int,
) (ReapReceiptPage, error) {
	if r == nil || r.Store == nil || r.Generation == "" {
		return ReapReceiptPage{}, errors.New("proc: reap receipt page requires store and generation")
	}
	if err := class.Validate(); err != nil {
		return ReapReceiptPage{}, err
	}
	return r.Store.LoadReapReceipts(ctx, class, after, limit)
}

// RecoverReapReceipts settles and acknowledges every receipt in one recovery
// class, returning the durable committed floor even when no receipt remains.
func (r *Reaper) RecoverReapReceipts(
	ctx context.Context,
	class RecoveryClass,
	settle func(context.Context, ReapReceipt) error,
) (ReapReceiptFloor, error) {
	if settle == nil {
		return ReapReceiptFloor{}, errors.New("proc: reap receipt settlement callback is required")
	}
	var cursor ReapReceiptCursor
	for {
		page, err := r.ReapReceipts(ctx, class, cursor, ReapReceiptPageLimit)
		if err != nil {
			return ReapReceiptFloor{}, err
		}
		floor := page.Floor
		for _, receipt := range page.Receipts {
			if err := settle(ctx, receipt); err != nil {
				return floor, fmt.Errorf("proc: settle reap receipt %d: %w", receipt.Sequence, err)
			}
			floor, err = r.AcknowledgeReap(ctx, receipt)
			if err != nil {
				return ReapReceiptFloor{}, err
			}
			cursor = ReapReceiptCursor{LedgerID: receipt.LedgerID, Sequence: receipt.Sequence}
		}
		if !page.More {
			return r.Store.ReapReceiptFloor(ctx, class)
		}
	}
}

// ReapReceipt returns the durable receipt for one exact process record,
// independent of the bounded replay page.
func (r *Reaper) ReapReceipt(
	ctx context.Context,
	record Record,
) (ReapReceipt, bool, error) {
	if r == nil || r.Store == nil || r.Generation == "" {
		return ReapReceipt{}, false, errors.New("proc: reap receipt lookup requires store and generation")
	}
	if err := record.Validate(); err != nil {
		return ReapReceipt{}, false, err
	}
	return r.Store.FindReapReceipt(ctx, record)
}

// ReapRecord settles one exact prior-generation record independently of the
// bounded replay page and returns its durable receipt.
func (r *Reaper) ReapRecord(ctx context.Context, record Record) (ReapReceipt, error) {
	if r == nil || r.Store == nil || r.Generation == "" {
		return ReapReceipt{}, errors.New("proc: exact reap requires store and generation")
	}
	if err := record.Validate(); err != nil {
		return ReapReceipt{}, err
	}
	if receipt, found, err := r.Store.FindReapReceipt(ctx, record); err != nil {
		return ReapReceipt{}, err
	} else if found {
		return receipt, nil
	}
	if record.Generation == r.Generation {
		return ReapReceipt{}, errors.New("proc: cannot reap current process generation")
	}
	records, err := r.Store.Load(ctx)
	if err != nil {
		return ReapReceipt{}, fmt.Errorf("load reaper records: %w", err)
	}
	if !slices.Contains(records, record) {
		return ReapReceipt{}, errors.New("proc: exact process record has no durable reap authority")
	}
	if err := r.Store.BeginReap(ctx, record, r.Generation); err != nil {
		return ReapReceipt{}, fmt.Errorf("claim child %d for reap: %w", record.PID, err)
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return ReapReceipt{}, fmt.Errorf("load current boot identity: %w", err)
	}
	reaped, outcome, err := r.reapOne(ctx, record, boot)
	if err != nil {
		return ReapReceipt{}, fmt.Errorf("reap child %d: %w", record.PID, err)
	}
	if !reaped {
		return ReapReceipt{}, fmt.Errorf("proc: exact process %d did not settle", record.PID)
	}
	receipt, err := r.Store.CommitReap(ctx, record, r.Generation, outcome)
	if err != nil {
		return ReapReceipt{}, fmt.Errorf("commit receipt for child %d: %w", record.PID, err)
	}
	return receipt, nil
}

// Drop only on provably gone, reused, or reaped; our own live child or an Undetermined probe fails closed and keeps the record.
func (r *Reaper) reapOne(
	ctx context.Context,
	rec Record,
	boot string,
) (bool, ReapOutcome, error) {
	if err := rec.Validate(); err != nil {
		return false, 0, err
	}
	if rec.Boot != boot {
		return true, ReapCrossBoot, nil
	}
	if rec.PID <= 1 || rec.PID == os.Getpid() {
		return false, 0, errors.New("refusing unsafe process identity")
	}
	if gone, err := r.auditRecordGone(rec); gone || err != nil {
		return gone, ReapAbsent, err
	}
	info, err := r.prb().probe(rec.PID)
	if rec.ProcessGroup {
		return r.reapGroupOutcome(ctx, rec, info, err, false)
	}
	switch {
	case errors.Is(err, errNoProc):
		return true, ReapAbsent, nil
	case err != nil:
		return false, 0, err
	case info.startTime != rec.StartTime:
		return true, ReapIdentityReused, nil
	case info.zombie:
		return true, ReapAbsent, nil
	case rec.Generation == r.Generation:
		return false, 0, nil
	}
	return r.reapOrphanOutcome(ctx, rec)
}

// SIGTERM → grace → re-revalidate → SIGKILL; ESRCH is success, and a PID reused during grace is never SIGKILLed.
func (r *Reaper) reapOrphan(ctx context.Context, rec Record) (bool, error) {
	reaped, _, err := r.reapOrphanOutcome(ctx, rec)
	return reaped, err
}

func (r *Reaper) reapOrphanOutcome(
	ctx context.Context,
	rec Record,
) (bool, ReapOutcome, error) {
	gone, err := r.sendRecordSignal(rec, syscall.SIGTERM)
	if err != nil {
		return false, 0, err
	}
	if gone {
		return true, ReapAbsent, nil
	}
	select {
	case <-ctx.Done():
		return false, 0, ctx.Err()
	case <-clockOrReal(r.clock).After(r.graceDur()):
	}
	if gone, err := r.auditRecordGone(rec); gone || err != nil {
		return gone, ReapTerminated, err
	}
	info, perr := r.prb().probe(rec.PID)
	switch {
	case errors.Is(perr, errNoProc):
		return true, ReapTerminated, nil
	case perr != nil:
		return false, 0, perr
	case info.startTime != rec.StartTime:
		return true, ReapIdentityReused, nil
	case info.zombie:
		return true, ReapTerminated, nil
	}
	if _, err := r.sendRecordSignal(rec, syscall.SIGKILL); err != nil {
		return false, 0, err
	}
	reaped, err := r.awaitProcessSettlement(ctx, rec)
	return reaped, ReapTerminated, err
}

func (r *Reaper) reapGroup(ctx context.Context, rec Record, leader procInfo, leaderErr error, permitCurrent bool) (bool, error) {
	reaped, _, err := r.reapGroupOutcome(ctx, rec, leader, leaderErr, permitCurrent)
	return reaped, err
}

func (r *Reaper) reapGroupOutcome(
	ctx context.Context,
	rec Record,
	leader procInfo,
	leaderErr error,
	permitCurrent bool,
) (bool, ReapOutcome, error) {
	if rec.SessionID <= 1 || rec.SessionID != rec.PID {
		return false, 0, errors.New("process group has no durable dedicated-session identity")
	}
	if !permitCurrent && rec.Generation == r.Generation {
		return false, 0, nil
	}
	switch {
	case leaderErr == nil && leader.startTime != rec.StartTime:
		return true, ReapIdentityReused, nil
	case leaderErr == nil && (leader.groupID != rec.PID || leader.sessionID != rec.SessionID):
		return false, 0, errors.New("process-group leader left its recorded group or session")
	case leaderErr != nil && !errors.Is(leaderErr, errNoProc):
		return false, 0, leaderErr
	}
	members, err := r.verifiedGroupMembers(rec)
	if err != nil {
		return false, 0, err
	}
	if len(members) == 0 {
		return true, ReapAbsent, nil
	}
	if _, err := r.signalSessionGroups(members, syscall.SIGTERM); err != nil {
		return false, 0, err
	}
	select {
	case <-ctx.Done():
		return false, 0, ctx.Err()
	case <-clockOrReal(r.clock).After(r.graceDur()):
	}
	reaped, err := r.awaitGroupSettlement(ctx, rec)
	return reaped, ReapTerminated, err
}

func (r *Reaper) awaitProcessSettlement(ctx context.Context, rec Record) (bool, error) {
	clock := clockOrReal(r.clock)
	deadline := clock.Now().Add(r.settlementDur())
	for {
		if gone, err := r.auditRecordGone(rec); gone || err != nil {
			return gone, err
		}
		info, err := r.prb().probe(rec.PID)
		switch {
		case errors.Is(err, errNoProc):
			return true, nil
		case err != nil:
			return false, fmt.Errorf("prove killed process %d settled: %w", rec.PID, err)
		case info.startTime != rec.StartTime || info.zombie:
			return true, nil
		case !clock.Now().Before(deadline):
			return false, errors.New("killed process remained live through settlement deadline")
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-clock.After(settlementPollInterval):
		}
	}
}

func (r *Reaper) awaitGroupSettlement(ctx context.Context, rec Record) (bool, error) {
	clock := clockOrReal(r.clock)
	deadline := clock.Now().Add(r.settlementDur())
	for {
		members, err := r.verifiedGroupMembers(rec)
		if err != nil {
			return false, fmt.Errorf("prove killed process group settled: %w", err)
		}
		if len(members) == 0 {
			return true, nil
		}
		if !clock.Now().Before(deadline) {
			return false, errors.New("killed process group remained live through settlement deadline")
		}
		if _, err := r.signalSessionGroups(members, syscall.SIGKILL); err != nil {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-clock.After(settlementPollInterval):
		}
	}
}

func (r *Reaper) verifiedGroupMembers(rec Record) ([]groupMember, error) {
	for range 3 {
		members, err := r.prb().groupMembers(rec.PID, rec.SessionID)
		if err != nil {
			return nil, fmt.Errorf("enumerate dedicated session %d: %w", rec.SessionID, err)
		}
		stable := make([]groupMember, 0, len(members))
		changed := false
		for _, member := range members {
			info, err := r.prb().probe(member.pid)
			switch {
			case errors.Is(err, errNoProc):
				changed = true
			case err != nil:
				return nil, fmt.Errorf("revalidate process-group member %d: %w", member.pid, err)
			case info.startTime != member.info.startTime || info.sessionID != rec.SessionID:
				changed = true
			case info.zombie:
			default:
				stable = append(stable, groupMember{pid: member.pid, info: info})
			}
		}
		if !changed {
			return stable, nil
		}
	}
	return nil, errors.New("dedicated-session membership changed during identity verification")
}

func (r *Reaper) signalSessionGroups(members []groupMember, signal syscall.Signal) (bool, error) {
	groups := make([]int, 0, len(members))
	seen := make(map[int]struct{}, len(members))
	for _, member := range members {
		if member.info.groupID <= 1 {
			return false, errors.New("dedicated-session member has an invalid process group")
		}
		if _, ok := seen[member.info.groupID]; ok {
			continue
		}
		seen[member.info.groupID] = struct{}{}
		groups = append(groups, member.info.groupID)
	}
	slices.Sort(groups)
	allGone := true
	for _, group := range groups {
		gone, err := r.sendSignal(-group, signal)
		if err != nil {
			return false, err
		}
		allGone = allGone && gone
	}
	return allGone, nil
}

// sendSignal delivers sig to pid, mapping ESRCH (already gone) to gone=true.
func (r *Reaper) sendSignal(pid int, sig syscall.Signal) (gone bool, err error) {
	if err := r.sig().signal(pid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (r *Reaper) sendRecordSignal(rec Record, sig syscall.Signal) (bool, error) {
	if rec.AuditToken.Valid() {
		if r.auditSignal != nil {
			return r.auditSignal(rec.AuditToken, sig)
		}
		return signalAuditToken(rec.AuditToken, sig)
	}
	return r.sendSignal(rec.PID, sig)
}

func (r *Reaper) auditExecutable(token AuditToken) (string, error) {
	if r.auditPath != nil {
		return r.auditPath(token)
	}
	return ExecutablePathAuditToken(token)
}

func (r *Reaper) auditRecordGone(rec Record) (bool, error) {
	if !rec.AuditToken.Valid() {
		return false, nil
	}
	path, err := r.auditExecutable(rec.AuditToken)
	if errors.Is(err, ErrNoProcess) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("revalidate audit-token process: %w", err)
	}
	if path != rec.Executable {
		return false, fmt.Errorf("%w: audit-token executable got %q, want %q", ErrIdentityChanged, path, rec.Executable)
	}
	return false, nil
}

func (r *Reaper) prb() prober {
	if r.prober == nil {
		return sysProber{}
	}
	return r.prober
}

func (r *Reaper) sig() signaler {
	if r.signaler == nil {
		return sysSignaler{}
	}
	return r.signaler
}

func (r *Reaper) graceDur() time.Duration {
	if r.Grace > 0 {
		return r.Grace
	}
	return DefaultReapGrace
}

func (r *Reaper) settlementDur() time.Duration {
	if r.Settlement > 0 {
		return r.Settlement
	}
	return DefaultReapSettlement
}
