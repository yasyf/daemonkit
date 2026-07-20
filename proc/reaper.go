package proc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"time"
)

// DefaultReapGrace bounds the wait between an orphan's SIGTERM and its SIGKILL.
const DefaultReapGrace = 5 * time.Second

// DefaultReapSettlement bounds post-SIGKILL identity polling.
const DefaultReapSettlement = 2 * time.Second

const (
	settlementPollInterval = 10 * time.Millisecond
	recordSchemaVersion    = 4
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
	// PID is the spawned child's process id.
	PID int `json:"pid"`
	// StartTime is the prober's opaque, platform-native process start stamp.
	StartTime string `json:"start_time"`
	// Boot is the kernel boot session in which StartTime was captured.
	Boot string `json:"boot"`
	// Comm is the child's initial OS-reported (truncated) process name.
	Comm string `json:"comm"`
	// Executable is the exact kernel-resolved path bound to AuditToken.
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
}

// Validate rejects an incomplete durable process identity.
func (r Record) Validate() error {
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
	} else if r.Executable != "" {
		return fmt.Errorf("%w: executable requires audit-token authority", ErrInvalidRecord)
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
	// CommitReap atomically replaces one exact process record with its durable
	// retirement receipt.
	CommitReap(ctx context.Context, rec Record, receipt ReapReceipt) error
	// LoadReapReceipts returns a bounded stable page and whether more remain.
	LoadReapReceipts(ctx context.Context, limit int) ([]ReapReceipt, bool, error)
	// HasReapReceipt reports an exact durable receipt match.
	HasReapReceipt(ctx context.Context, receipt ReapReceipt) (bool, error)
	// FindReapReceipt returns the durable receipt for one exact process record,
	// independent of bounded page position.
	FindReapReceipt(ctx context.Context, record Record) (ReapReceipt, bool, error)
	// AcknowledgeReap forgets an exact receipt; absence is idempotent.
	AcknowledgeReap(ctx context.Context, receipt ReapReceipt) error
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
func (r *Reaper) Track(ctx context.Context, pid int) (Record, error) {
	return r.track(ctx, pid, false)
}

// TrackIdentity durably records an already authenticated exact process identity.
// It re-probes before writing so PID reuse can never turn the supplied identity
// into authority over a different process.
func (r *Reaper) TrackIdentity(ctx context.Context, identity Identity) (Record, error) {
	rec := Record{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Comm: identity.Comm, Generation: r.Generation, Executable: identity.Executable,
		AuditToken: identity.AuditToken,
	}
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

// TrackGroup records a child whose PID leads its own process group and session.
func (r *Reaper) TrackGroup(ctx context.Context, pid int) (Record, error) {
	return r.track(ctx, pid, true)
}

func (r *Reaper) track(ctx context.Context, pid int, processGroup bool) (Record, error) {
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
		PID:          pid,
		StartTime:    info.startTime,
		Boot:         boot,
		Comm:         info.comm,
		Generation:   r.Generation,
		ProcessGroup: processGroup,
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

// Reap returns one bounded stable page of durable retirement receipts. It
// revalidates stored records and atomically replaces each settled prior
// generation with its receipt before that kill authority is erased.
func (r *Reaper) Reap(ctx context.Context) (ReapResult, error) {
	receipts, moreReceipts, err := r.Store.LoadReapReceipts(ctx, ReapReceiptPageLimit)
	if err != nil {
		return ReapResult{}, fmt.Errorf("load reaper receipts: %w", err)
	}
	result := ReapResult{Receipts: receipts, More: moreReceipts}
	if moreReceipts || len(receipts) == ReapReceiptPageLimit {
		result.More = true
		return result, nil
	}
	recs, err := r.Store.Load(ctx)
	if err != nil {
		return ReapResult{}, fmt.Errorf("load reaper records: %w", err)
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return ReapResult{}, fmt.Errorf("load current boot identity: %w", err)
	}
	var unresolved []error
	for index, rec := range recs {
		if err := ctx.Err(); err != nil {
			return ReapResult{}, err
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
			receipt, receiptErr := newReapReceipt(rec, r.Generation, outcome)
			if receiptErr != nil {
				unresolved = append(unresolved, fmt.Errorf("receipt for child %d: %w", rec.PID, receiptErr))
				continue
			}
			if commitErr := r.Store.CommitReap(ctx, rec, receipt); commitErr != nil {
				unresolved = append(unresolved, fmt.Errorf("commit receipt for child %d: %w", rec.PID, commitErr))
				continue
			}
			result.Receipts = append(result.Receipts, receipt)
			if len(result.Receipts) == ReapReceiptPageLimit {
				result.More = index+1 < len(recs)
				break
			}
		}
	}
	return result, errors.Join(unresolved...)
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
	receipt, err := newReapReceipt(record, r.Generation, outcome)
	if err != nil {
		return ReapReceipt{}, fmt.Errorf("receipt for child %d: %w", record.PID, err)
	}
	if err := r.Store.CommitReap(ctx, record, receipt); err != nil {
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
	gone, err := r.sendSignal(-rec.PID, syscall.SIGTERM)
	if err != nil {
		return false, 0, err
	}
	if gone {
		reaped, err := r.groupGone(rec)
		return reaped, ReapTerminated, err
	}
	select {
	case <-ctx.Done():
		return false, 0, ctx.Err()
	case <-clockOrReal(r.clock).After(r.graceDur()):
	}
	members, err = r.verifiedGroupMembers(rec)
	if err != nil {
		return false, 0, err
	}
	if len(members) == 0 {
		return true, ReapTerminated, nil
	}
	gone, err = r.sendSignal(-rec.PID, syscall.SIGKILL)
	if err != nil {
		return false, 0, err
	}
	if gone {
		reaped, err := r.groupGone(rec)
		return reaped, ReapTerminated, err
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
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-clock.After(settlementPollInterval):
		}
	}
}

func (r *Reaper) groupGone(rec Record) (bool, error) {
	members, err := r.verifiedGroupMembers(rec)
	if err != nil {
		return false, err
	}
	if len(members) != 0 {
		return false, errors.New("process group remained enumerable after ESRCH")
	}
	return true, nil
}

func (r *Reaper) verifiedGroupMembers(rec Record) ([]groupMember, error) {
	for range 3 {
		members, err := r.prb().groupMembers(rec.PID, rec.SessionID)
		if err != nil {
			return nil, fmt.Errorf("enumerate process group %d session %d: %w", rec.PID, rec.SessionID, err)
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
			case info.startTime != member.info.startTime || info.groupID != rec.PID || info.sessionID != rec.SessionID:
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
	return nil, errors.New("process-group membership changed during identity verification")
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

// FileStore is the JSON-file Store: one file guarded by an exclusive file lock
// file so concurrent daemons serialize read-modify-writes; writes are atomic
// (temp file + rename) and a missing file reads as an empty set.
type FileStore struct {
	// Path is the JSON records file.
	Path string

	ops *recordFileOps
}

type recordFileOps struct {
	syncTemp func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

func (s *FileStore) recordOps() recordFileOps {
	ops := recordFileOps{
		syncTemp: func(file *os.File) error { return file.Sync() },
		rename:   os.Rename,
		syncDir:  fsyncDir,
	}
	if s.ops == nil {
		return ops
	}
	if s.ops.syncTemp != nil {
		ops.syncTemp = s.ops.syncTemp
	}
	if s.ops.rename != nil {
		ops.rename = s.ops.rename
	}
	if s.ops.syncDir != nil {
		ops.syncDir = s.ops.syncDir
	}
	return ops
}

// Add records rec, replacing any prior record for the same process instance.
func (s *FileStore) Add(ctx context.Context, rec Record) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	return s.mutateFile(ctx, func(file recordFile) (recordFile, error) {
		for _, claim := range file.Claims {
			if recordKey(claim.Record) == recordKey(rec) {
				return recordFile{}, errors.New("proc: process instance is claimed for reap")
			}
		}
		out := file.Records[:0:0]
		for _, existing := range file.Records {
			if recordKey(existing) != recordKey(rec) {
				out = append(out, existing)
			}
		}
		file.Records = append(out, rec)
		return file, nil
	})
}

// Remove deletes the given records, matched by PID + StartTime + Boot.
func (s *FileStore) Remove(ctx context.Context, victims []Record) error {
	if len(victims) == 0 {
		return nil
	}
	drop := make(map[Record]struct{}, len(victims))
	for _, v := range victims {
		if err := v.Validate(); err != nil {
			return err
		}
		drop[v] = struct{}{}
	}
	return s.mutateFile(ctx, func(file recordFile) (recordFile, error) {
		claimed := make(map[string]struct{}, len(file.Claims))
		for _, claim := range file.Claims {
			claimed[recordKey(claim.Record)] = struct{}{}
		}
		out := file.Records[:0:0]
		for _, existing := range file.Records {
			_, dropRequested := drop[existing]
			_, reapClaimed := claimed[recordKey(existing)]
			if !dropRequested || reapClaimed {
				out = append(out, existing)
			}
		}
		file.Records = out
		return file, nil
	})
}

// BeginReap durably fences graceful untracking of rec.
func (s *FileStore) BeginReap(
	ctx context.Context,
	rec Record,
	reaperGeneration string,
) error {
	claim := reapClaim{Record: rec, ReaperGeneration: reaperGeneration}
	if err := claim.validate(); err != nil {
		return err
	}
	return s.mutateFile(ctx, func(file recordFile) (recordFile, error) {
		present := false
		for _, existing := range file.Records {
			if existing == rec {
				present = true
			}
		}
		if !present {
			return recordFile{}, errors.New("proc: reap claim has no exact durable process record")
		}
		claims := file.Claims[:0:0]
		for _, existing := range file.Claims {
			if recordKey(existing.Record) != recordKey(rec) {
				claims = append(claims, existing)
			}
		}
		file.Claims = append(claims, claim)
		return file, nil
	})
}

// Load returns every stored record; a missing file is an empty set.
func (s *FileStore) Load(ctx context.Context) ([]Record, error) {
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	return readRecords(s.Path)
}

// CommitReap atomically replaces rec with its exact durable receipt.
func (s *FileStore) CommitReap(ctx context.Context, rec Record, receipt ReapReceipt) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Record != rec {
		return fmt.Errorf("%w: receipt record mismatch", ErrInvalidReapReceipt)
	}
	return s.mutateFile(ctx, func(file recordFile) (recordFile, error) {
		receiptPresent := false
		for _, existing := range file.Receipts {
			if existing.Digest != receipt.Digest {
				continue
			}
			if existing != receipt {
				return recordFile{}, fmt.Errorf("%w: receipt digest collision", ErrInvalidReapReceipt)
			}
			receiptPresent = true
		}
		claimPresent := false
		claims := file.Claims[:0:0]
		for _, claim := range file.Claims {
			if claim.Record == rec && claim.ReaperGeneration == receipt.ReaperGeneration {
				claimPresent = true
				continue
			}
			if recordKey(claim.Record) == recordKey(rec) {
				return recordFile{}, errors.New("proc: reap claim generation changed before receipt commit")
			}
			claims = append(claims, claim)
		}
		if !claimPresent && !receiptPresent {
			return recordFile{}, errors.New("proc: reap receipt has no exact durable claim")
		}
		recordPresent := false
		records := file.Records[:0:0]
		for _, existing := range file.Records {
			if existing == rec {
				recordPresent = true
				continue
			}
			if recordKey(existing) == recordKey(rec) {
				return recordFile{}, fmt.Errorf("%w: process record changed before receipt commit", ErrIdentityChanged)
			}
			records = append(records, existing)
		}
		if !recordPresent && !receiptPresent {
			return recordFile{}, errors.New("proc: reap receipt has no exact durable process record")
		}
		file.Records = records
		file.Claims = claims
		if !receiptPresent {
			file.Receipts = append(file.Receipts, receipt)
		}
		return file, nil
	})
}

// LoadReapReceipts returns one stable bounded page.
func (s *FileStore) LoadReapReceipts(
	ctx context.Context,
	limit int,
) ([]ReapReceipt, bool, error) {
	if limit <= 0 || limit > ReapReceiptPageLimit {
		return nil, false, fmt.Errorf("proc: reap receipt limit %d is out of bounds", limit)
	}
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	defer lock.Close()
	file, err := readRecordFile(s.Path)
	if err != nil {
		return nil, false, err
	}
	more := len(file.Receipts) > limit
	receipts := file.Receipts
	if more {
		receipts = receipts[:limit]
	}
	return append([]ReapReceipt(nil), receipts...), more, nil
}

// HasReapReceipt reports an exact durable receipt match.
func (s *FileStore) HasReapReceipt(ctx context.Context, receipt ReapReceipt) (bool, error) {
	if err := receipt.Validate(); err != nil {
		return false, err
	}
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	file, err := readRecordFile(s.Path)
	if err != nil {
		return false, err
	}
	for _, existing := range file.Receipts {
		if existing == receipt {
			return true, nil
		}
	}
	return false, nil
}

// FindReapReceipt returns the durable receipt for one exact process record.
func (s *FileStore) FindReapReceipt(
	ctx context.Context,
	record Record,
) (ReapReceipt, bool, error) {
	if err := record.Validate(); err != nil {
		return ReapReceipt{}, false, err
	}
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return ReapReceipt{}, false, err
	}
	defer lock.Close()
	file, err := readRecordFile(s.Path)
	if err != nil {
		return ReapReceipt{}, false, err
	}
	for _, receipt := range file.Receipts {
		if receipt.Record == record {
			return receipt, true, nil
		}
	}
	return ReapReceipt{}, false, nil
}

// AcknowledgeReap forgets an exact receipt; absence is a successful no-op.
func (s *FileStore) AcknowledgeReap(ctx context.Context, receipt ReapReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	return s.mutateFile(ctx, func(file recordFile) (recordFile, error) {
		receipts := file.Receipts[:0:0]
		for _, existing := range file.Receipts {
			if existing.Digest == receipt.Digest && existing != receipt {
				return recordFile{}, fmt.Errorf("%w: receipt digest collision", ErrInvalidReapReceipt)
			}
			if existing != receipt {
				receipts = append(receipts, existing)
			}
		}
		file.Receipts = receipts
		return file, nil
	})
}

func (s *FileStore) mutateFile(
	ctx context.Context,
	fn func(recordFile) (recordFile, error),
) error {
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	file, err := readRecordFile(s.Path)
	if err != nil {
		return err
	}
	file, err = fn(file)
	if err != nil {
		return err
	}
	return writeRecordFile(s.Path, file, s.recordOps())
}

// PID, boot, and start time: reuse within or across boots is a distinct instance.
func recordKey(r Record) string {
	return strconv.Itoa(r.PID) + "\x00" + r.Boot + "\x00" + r.StartTime
}

type recordFile struct {
	Schema   int           `json:"schema"`
	Records  []Record      `json:"records"`
	Claims   []reapClaim   `json:"claims"`
	Receipts []ReapReceipt `json:"receipts"`
}

func readRecords(path string) ([]Record, error) {
	file, err := readRecordFile(path)
	if err != nil {
		return nil, err
	}
	return file.Records, nil
}

func readRecordFile(path string) (recordFile, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return recordFile{Schema: recordSchemaVersion}, nil
	}
	if err != nil {
		return recordFile{}, fmt.Errorf("read records %s: %w", path, err)
	}
	var file recordFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return recordFile{}, fmt.Errorf("%w: parse records %s: %w", ErrRecordSchema, path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return recordFile{}, fmt.Errorf("%w: trailing records data", ErrRecordSchema)
	}
	if file.Schema != recordSchemaVersion {
		return recordFile{}, fmt.Errorf("%w: got %d, want %d", ErrRecordSchema, file.Schema, recordSchemaVersion)
	}
	for _, rec := range file.Records {
		if err := rec.Validate(); err != nil {
			return recordFile{}, err
		}
	}
	claims := make(map[string]struct{}, len(file.Claims))
	for _, claim := range file.Claims {
		if err := claim.validate(); err != nil {
			return recordFile{}, err
		}
		key := recordKey(claim.Record)
		if _, duplicate := claims[key]; duplicate {
			return recordFile{}, fmt.Errorf("%w: duplicate reap claim", ErrRecordSchema)
		}
		claims[key] = struct{}{}
		if !slices.Contains(file.Records, claim.Record) {
			return recordFile{}, fmt.Errorf("%w: reap claim has no exact process record", ErrRecordSchema)
		}
	}
	seen := make(map[[32]byte]struct{}, len(file.Receipts))
	for _, receipt := range file.Receipts {
		if err := receipt.Validate(); err != nil {
			return recordFile{}, err
		}
		if _, duplicate := seen[receipt.Digest]; duplicate {
			return recordFile{}, fmt.Errorf("%w: duplicate receipt", ErrRecordSchema)
		}
		seen[receipt.Digest] = struct{}{}
	}
	return file, nil
}

func writeRecordFile(path string, file recordFile, ops recordFileOps) error {
	if file.Records == nil {
		file.Records = []Record{}
	}
	if file.Receipts == nil {
		file.Receipts = []ReapReceipt{}
	}
	if file.Claims == nil {
		file.Claims = []reapClaim{}
	}
	for _, rec := range file.Records {
		if err := rec.Validate(); err != nil {
			return err
		}
	}
	claims := make(map[string]struct{}, len(file.Claims))
	for _, claim := range file.Claims {
		if err := claim.validate(); err != nil {
			return err
		}
		key := recordKey(claim.Record)
		if _, duplicate := claims[key]; duplicate {
			return fmt.Errorf("%w: duplicate reap claim", ErrRecordSchema)
		}
		claims[key] = struct{}{}
	}
	seen := make(map[[32]byte]struct{}, len(file.Receipts))
	for _, receipt := range file.Receipts {
		if err := receipt.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[receipt.Digest]; duplicate {
			return fmt.Errorf("%w: duplicate receipt", ErrRecordSchema)
		}
		seen[receipt.Digest] = struct{}{}
	}
	file.Schema = recordSchemaVersion
	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return fmt.Errorf("encode records: %w", err)
	}
	dir := filepath.Dir(path)
	if err := mkdirAllDurable(dir, 0o700, ops.syncDir); err != nil {
		return fmt.Errorf("create records dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".records-*")
	if err != nil {
		return fmt.Errorf("create temp records: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp records: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod temp records: %w", err)
	}
	if err := ops.syncTemp(tmp); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("fsync temp records: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp records: %w", err)
	}
	if err := ops.rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename records into place: %w", err)
	}
	if err := ops.syncDir(dir); err != nil {
		return fmt.Errorf("fsync records dir: %w", err)
	}
	return nil
}
