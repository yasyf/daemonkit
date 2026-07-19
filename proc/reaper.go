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
	recordSchemaVersion    = 2
)

// errNoProc is a definitive "gone", distinct from a probe failure (Undetermined, fails closed).
var errNoProc = errors.New("no such process")

var (
	// ErrInvalidRecord means a durable process record lacks required identity.
	ErrInvalidRecord = errors.New("proc: invalid durable process record")
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
	// Remove deletes the given records (matched by PID + StartTime + Boot).
	Remove(ctx context.Context, victims []Record) error
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

	prober   prober
	signaler signaler
	clock    clock
}

// Track snapshots a freshly spawned child's identity through the same prober
// Reap revalidates with and records it under this reaper's Generation.
func (r *Reaper) Track(ctx context.Context, pid int) (Record, error) {
	return r.track(ctx, pid, false)
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

// Reap revalidates every stored record and signals only provably-ours
// orphans of a prior generation, dropping gone-or-reused records and keeping
// unresolved ones. Run it once at cold start before accepting registrations.
func (r *Reaper) Reap(ctx context.Context) error {
	recs, err := r.Store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load reaper records: %w", err)
	}
	boot, err := r.prb().bootID()
	if err != nil {
		return fmt.Errorf("load current boot identity: %w", err)
	}
	drop := make([]Record, 0, len(recs))
	var unresolved []error
	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		reaped, reapErr := r.reapOne(ctx, rec, boot)
		if reapErr != nil {
			unresolved = append(unresolved, fmt.Errorf("reap child %d: %w", rec.PID, reapErr))
		}
		if reaped {
			drop = append(drop, rec)
		}
	}
	if len(drop) > 0 {
		if err := r.Store.Remove(ctx, drop); err != nil {
			unresolved = append(unresolved, fmt.Errorf("prune reaped records: %w", err))
		}
	}
	return errors.Join(unresolved...)
}

// Drop only on provably gone, reused, or reaped; our own live child or an Undetermined probe fails closed and keeps the record.
func (r *Reaper) reapOne(ctx context.Context, rec Record, boot string) (bool, error) {
	if err := rec.Validate(); err != nil {
		return false, err
	}
	if rec.Boot != boot {
		return true, nil
	}
	if rec.PID <= 1 || rec.PID == os.Getpid() {
		return false, errors.New("refusing unsafe process identity")
	}
	info, err := r.prb().probe(rec.PID)
	if rec.ProcessGroup {
		return r.reapGroup(ctx, rec, info, err)
	}
	switch {
	case errors.Is(err, errNoProc):
		return true, nil // recorded process gone → stale record
	case err != nil:
		return false, err // Undetermined → fail closed, keep
	case info.startTime != rec.StartTime:
		return true, nil // pid reused: a different process holds it now → drop stale record
	case info.zombie:
		return true, nil
	case rec.Generation == r.Generation:
		return false, nil // our own current-generation child → never signal, keep
	}
	return r.reapOrphan(ctx, rec)
}

// SIGTERM → grace → re-revalidate → SIGKILL; ESRCH is success, and a PID reused during grace is never SIGKILLed.
func (r *Reaper) reapOrphan(ctx context.Context, rec Record) (bool, error) {
	target := signalTarget(rec)
	gone, err := r.sendSignal(target, syscall.SIGTERM)
	if err != nil {
		return false, err // Undetermined signal error → fail closed
	}
	if gone {
		return true, nil // ESRCH: already gone
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err() // aborted mid-grace; retry next pass
	case <-clockOrReal(r.clock).After(r.graceDur()):
	}
	info, perr := r.prb().probe(rec.PID)
	switch {
	case errors.Is(perr, errNoProc):
		return true, nil // exited during grace
	case perr != nil:
		return false, perr // Undetermined → do not escalate
	case info.startTime != rec.StartTime:
		return true, nil // pid reused during grace: no longer provably ours; never SIGKILL it
	case info.zombie:
		return true, nil
	}
	if _, err := r.sendSignal(target, syscall.SIGKILL); err != nil {
		return false, err // Undetermined → fail closed
	}
	return r.awaitProcessSettlement(ctx, rec)
}

func (r *Reaper) reapGroup(ctx context.Context, rec Record, leader procInfo, leaderErr error) (bool, error) {
	if rec.SessionID <= 1 || rec.SessionID != rec.PID {
		return false, errors.New("process group has no durable dedicated-session identity")
	}
	if rec.Generation == r.Generation {
		return false, nil
	}
	switch {
	case leaderErr == nil && leader.startTime != rec.StartTime:
		return true, nil
	case leaderErr == nil && (leader.groupID != rec.PID || leader.sessionID != rec.SessionID):
		return false, errors.New("process-group leader left its recorded group or session")
	case leaderErr != nil && !errors.Is(leaderErr, errNoProc):
		return false, leaderErr
	}
	members, err := r.verifiedGroupMembers(rec)
	if err != nil {
		return false, err
	}
	if len(members) == 0 {
		return true, nil
	}
	gone, err := r.sendSignal(-rec.PID, syscall.SIGTERM)
	if err != nil {
		return false, err
	}
	if gone {
		return r.groupGone(rec)
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-clockOrReal(r.clock).After(r.graceDur()):
	}
	members, err = r.verifiedGroupMembers(rec)
	if err != nil {
		return false, err
	}
	if len(members) == 0 {
		return true, nil
	}
	gone, err = r.sendSignal(-rec.PID, syscall.SIGKILL)
	if err != nil {
		return false, err
	}
	if gone {
		return r.groupGone(rec)
	}
	return r.awaitGroupSettlement(ctx, rec)
}

func (r *Reaper) awaitProcessSettlement(ctx context.Context, rec Record) (bool, error) {
	clock := clockOrReal(r.clock)
	deadline := clock.Now().Add(r.settlementDur())
	for {
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

func signalTarget(rec Record) int {
	if rec.ProcessGroup {
		return -rec.PID
	}
	return rec.PID
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
}

// Add records rec, replacing any prior record for the same process instance.
func (s *FileStore) Add(ctx context.Context, rec Record) error {
	return s.mutate(ctx, func(recs []Record) []Record {
		out := recs[:0:0]
		for _, e := range recs {
			if recordKey(e) != recordKey(rec) {
				out = append(out, e)
			}
		}
		return append(out, rec)
	})
}

// Remove deletes the given records, matched by PID + StartTime + Boot.
func (s *FileStore) Remove(ctx context.Context, victims []Record) error {
	if len(victims) == 0 {
		return nil
	}
	drop := make(map[string]struct{}, len(victims))
	for _, v := range victims {
		if err := validateRecordIdentity(v); err != nil {
			return err
		}
		drop[recordKey(v)] = struct{}{}
	}
	return s.mutate(ctx, func(recs []Record) []Record {
		out := recs[:0:0]
		for _, e := range recs {
			if _, ok := drop[recordKey(e)]; !ok {
				out = append(out, e)
			}
		}
		return out
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

func (s *FileStore) mutate(ctx context.Context, fn func([]Record) []Record) error {
	lock, err := (FileLockSpec{
		Path:     s.Path + ".lock",
		Mode:     FileLockExclusive,
		Deadline: 5 * time.Second,
	}).Acquire(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	recs, err := readRecords(s.Path)
	if err != nil {
		return err
	}
	return writeRecords(s.Path, fn(recs))
}

// PID, boot, and start time: reuse within or across boots is a distinct instance.
func recordKey(r Record) string {
	return strconv.Itoa(r.PID) + "\x00" + r.Boot + "\x00" + r.StartTime
}

type recordFile struct {
	Schema  int      `json:"schema"`
	Records []Record `json:"records"`
}

func readRecords(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read records %s: %w", path, err)
	}
	var file recordFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return nil, fmt.Errorf("%w: parse records %s: %w", ErrRecordSchema, path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: trailing records data", ErrRecordSchema)
	}
	if file.Schema != recordSchemaVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrRecordSchema, file.Schema, recordSchemaVersion)
	}
	for _, rec := range file.Records {
		if err := rec.Validate(); err != nil {
			return nil, err
		}
	}
	return file.Records, nil
}

func writeRecords(path string, recs []Record) error {
	if recs == nil {
		recs = []Record{}
	}
	for _, rec := range recs {
		if err := rec.Validate(); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(recordFile{Schema: recordSchemaVersion, Records: recs}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode records: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create records dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".records-*")
	if err != nil {
		return fmt.Errorf("create temp records: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp records: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp records: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename records into place: %w", err)
	}
	return nil
}
