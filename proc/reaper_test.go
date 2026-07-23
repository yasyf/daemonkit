package proc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const testBoot = "test-boot"

func mustAdd(t *testing.T, s Store, rec Record) {
	t.Helper()
	if err := s.Add(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

type memStore struct {
	mu        sync.Mutex
	recs      []Record
	claims    map[string]reapClaim
	receipts  []ReapReceipt
	sequences map[RecoveryClass]uint64
	floors    map[RecoveryClass]uint64
}

func (m *memStore) Add(_ context.Context, rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.recs[:0:0]
	for _, e := range m.recs {
		if recordKey(e) != recordKey(rec) {
			out = append(out, e)
		}
	}
	m.recs = append(out, rec)
	return nil
}

func (m *memStore) Load(_ context.Context) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Record(nil), m.recs...), nil
}

func (m *memStore) Remove(_ context.Context, victims []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	drop := make(map[Record]struct{}, len(victims))
	for _, v := range victims {
		drop[v] = struct{}{}
	}
	out := m.recs[:0:0]
	for _, e := range m.recs {
		_, claimed := m.claims[recordKey(e)]
		if _, ok := drop[e]; !ok || claimed {
			out = append(out, e)
		}
	}
	m.recs = out
	return nil
}

func (m *memStore) BeginReap(_ context.Context, rec Record, generation string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claims == nil {
		m.claims = make(map[string]reapClaim)
	}
	present := slices.Contains(m.recs, rec)
	if !present {
		return errors.New("missing exact record")
	}
	m.claims[recordKey(rec)] = reapClaim{Record: rec, ReaperGeneration: generation}
	return nil
}

func (m *memStore) CommitReap(
	_ context.Context,
	rec Record,
	reaperGeneration string,
	outcome ReapOutcome,
) (ReapReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sequences == nil {
		m.sequences = make(map[RecoveryClass]uint64)
	}
	sequence := m.sequences[rec.RecoveryClass] + 1
	receipt, err := newReapReceipt(ReceiptLedgerID{1}, sequence, rec, reaperGeneration, outcome)
	if err != nil {
		return ReapReceipt{}, err
	}
	claim, claimed := m.claims[recordKey(rec)]
	if !claimed || claim.Record != rec || claim.ReaperGeneration != receipt.ReaperGeneration {
		return ReapReceipt{}, errors.New("missing exact reap claim")
	}
	present := false
	records := m.recs[:0:0]
	for _, existing := range m.recs {
		if existing == rec {
			present = true
			continue
		}
		records = append(records, existing)
	}
	for _, existing := range m.receipts {
		if existing == receipt {
			m.recs = records
			delete(m.claims, recordKey(rec))
			return existing, nil
		}
	}
	if !present {
		return ReapReceipt{}, errors.New("missing exact record")
	}
	m.recs = records
	delete(m.claims, recordKey(rec))
	m.receipts = append(m.receipts, receipt)
	m.sequences[rec.RecoveryClass] = sequence
	return receipt, nil
}

func (m *memStore) LoadReapReceipts(
	_ context.Context,
	class RecoveryClass,
	after ReapReceiptCursor,
	limit int,
) (ReapReceiptPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	page := ReapReceiptPage{
		Floor: ReapReceiptFloor{LedgerID: ReceiptLedgerID{1}, RecoveryClass: class, Sequence: m.floors[class]},
	}
	for _, receipt := range m.receipts {
		if receipt.Record.RecoveryClass != class || receipt.Sequence <= after.Sequence {
			continue
		}
		if len(page.Receipts) == limit {
			page.More = true
			break
		}
		page.Receipts = append(page.Receipts, receipt)
		page.Next = ReapReceiptCursor{LedgerID: receipt.LedgerID, Sequence: receipt.Sequence}
	}
	return page, nil
}

func (m *memStore) HasReapReceipt(_ context.Context, receipt ReapReceipt) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.receipts, receipt), nil
}

func (m *memStore) FindReapReceipt(
	_ context.Context,
	record Record,
) (ReapReceipt, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, receipt := range m.receipts {
		if receipt.Record == record {
			return receipt, true, nil
		}
	}
	return ReapReceipt{}, false, nil
}

func (m *memStore) AcknowledgeReap(_ context.Context, receipt ReapReceipt) (ReapReceiptFloor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.floors == nil {
		m.floors = make(map[RecoveryClass]uint64)
	}
	class := receipt.Record.RecoveryClass
	floor := m.floors[class]
	if receipt.Sequence < floor {
		return ReapReceiptFloor{}, ErrReapReceiptStale
	}
	if receipt.Sequence == floor {
		return ReapReceiptFloor{LedgerID: receipt.LedgerID, RecoveryClass: class, Sequence: floor}, nil
	}
	if receipt.Sequence != floor+1 {
		return ReapReceiptFloor{}, ErrReapReceiptOrder
	}
	if !slices.Contains(m.receipts, receipt) {
		return ReapReceiptFloor{}, ErrUnrecognizedReapReceipt
	}
	m.receipts = slices.DeleteFunc(m.receipts, func(existing ReapReceipt) bool {
		return existing == receipt
	})
	m.floors[class] = receipt.Sequence
	return ReapReceiptFloor{LedgerID: receipt.LedgerID, RecoveryClass: class, Sequence: receipt.Sequence}, nil
}

func (m *memStore) ReapReceiptFloor(_ context.Context, class RecoveryClass) (ReapReceiptFloor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ReapReceiptFloor{LedgerID: ReceiptLedgerID{1}, RecoveryClass: class, Sequence: m.floors[class]}, nil
}

func (m *memStore) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.recs)
}

type fakeProber struct {
	mu         sync.Mutex
	info       procInfo
	err        error
	probed     []int
	perProbe   []probeResult
	byPID      map[int]probeResult
	members    []groupMember
	memberSets [][]groupMember
	groupCalls int
	groupErr   error
	boot       string
	bootErr    error
}

func (f *fakeProber) bootID() (string, error) {
	if f.boot == "" {
		return testBoot, f.bootErr
	}
	return f.boot, f.bootErr
}

type probeResult struct {
	info procInfo
	err  error
}

func (f *fakeProber) probe(pid int) (procInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.probed)
	f.probed = append(f.probed, pid)
	if result, ok := f.byPID[pid]; ok {
		return result.info, result.err
	}
	if n < len(f.perProbe) {
		return f.perProbe[n].info, f.perProbe[n].err
	}
	return f.info, f.err
}

func (f *fakeProber) groupMembers(_, _ int) ([]groupMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := f.groupCalls
	f.groupCalls++
	if call < len(f.memberSets) {
		return append([]groupMember(nil), f.memberSets[call]...), f.groupErr
	}
	return append([]groupMember(nil), f.members...), f.groupErr
}

func (f *fakeProber) probedPIDs() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.probed...)
}

type recSignaler struct {
	mu       sync.Mutex
	sent     []signalCall
	delegate signaler
	err      error
}

type cancelSignaler struct {
	cancel context.CancelFunc
	mu     sync.Mutex
	sent   []signalCall
}

func (s *cancelSignaler) signal(pid int, sig syscall.Signal) error {
	s.mu.Lock()
	s.sent = append(s.sent, signalCall{pid: pid, sig: sig})
	s.mu.Unlock()
	s.cancel()
	return nil
}

func (s *cancelSignaler) calls() []signalCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signalCall(nil), s.sent...)
}

type signalCall struct {
	pid int
	sig syscall.Signal
}

func (r *recSignaler) signal(pid int, sig syscall.Signal) error {
	r.mu.Lock()
	r.sent = append(r.sent, signalCall{pid, sig})
	r.mu.Unlock()
	if r.delegate != nil {
		return r.delegate.signal(pid, sig)
	}
	return r.err
}

func (r *recSignaler) calls() []signalCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]signalCall(nil), r.sent...)
}

func liveInfo() procInfo { return procInfo{startTime: "111.222", comm: "worker"} }

func matchingRecord(pid int, gen string) Record {
	i := liveInfo()
	return Record{RecoveryClass: RecoveryTask, PID: pid, StartTime: i.startTime, Boot: testBoot, Comm: i.comm, Generation: gen}
}

func TestReapReceiptPersistsAndReplaysByteIdenticallyUntilAcknowledged(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path}
	record := matchingRecord(4041, "prior-generation")
	record.Executable = "/Applications/Fixed.app/Contents/MacOS/Fixed"
	record.AuditToken = auditTokenForPID(record.PID, 9)
	mustAdd(t, store, record)
	firstReaper := &Reaper{
		Store: store, Generation: "current-generation",
		prober: &fakeProber{err: errNoProc},
		auditPath: func(AuditToken) (string, error) {
			return "", ErrNoProcess
		},
	}
	if err := firstReaper.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := firstReaper.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Receipts) != 1 || first.More {
		t.Fatalf("first Reap = %+v, want one receipt", first)
	}
	receipt := first.Receipts[0]
	if receipt.Record != record || receipt.Outcome != ReapAbsent {
		t.Fatalf("receipt = %+v, want exact absent record", receipt)
	}
	firstBytes, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstReaper.VerifyReapReceipt(ctx, receipt); err != nil {
		t.Fatalf("VerifyReapReceipt: %v", err)
	}

	restarted := &Reaper{
		Store: &FileStore{Path: path}, Generation: "next-generation",
		prober: &fakeProber{},
	}
	if err := restarted.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Receipts) != 1 {
		t.Fatalf("replayed receipts = %+v", replayed)
	}
	replayedBytes, err := json.Marshal(replayed.Receipts[0])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(firstBytes, replayedBytes) {
		t.Fatalf("lost-response replay changed bytes:\nfirst=%s\nreplayed=%s", firstBytes, replayedBytes)
	}
	if _, err := restarted.AcknowledgeReap(ctx, receipt); err != nil {
		t.Fatalf("AcknowledgeReap: %v", err)
	}
	if _, err := restarted.AcknowledgeReap(ctx, receipt); err != nil {
		t.Fatalf("idempotent AcknowledgeReap: %v", err)
	}
	if err := restarted.VerifyReapReceipt(ctx, receipt); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("Verify acknowledged receipt = %v, want stale", err)
	}
	if err := restarted.Reap(ctx); err != nil {
		t.Fatalf("Reap after acknowledgement = %v", err)
	}
	empty, err := restarted.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil || len(empty.Receipts) != 0 || empty.More || empty.Floor.Sequence != receipt.Sequence {
		t.Fatalf("receipts after acknowledgement = %+v, %v", empty, err)
	}
}

func TestReapRecordRejectsVacuousAbsenceWithoutOwnedRecord(t *testing.T) {
	record := matchingRecord(4045, "prior-generation")
	reaper := &Reaper{
		Store: &memStore{}, Generation: "current-generation",
		prober: &fakeProber{err: errNoProc},
	}

	receipt, err := reaper.ReapRecord(t.Context(), record)
	if err == nil {
		t.Fatalf("ReapRecord without durable authority = %+v, want error", receipt)
	}
	if receipt != (ReapReceipt{}) {
		t.Fatalf("ReapRecord without durable authority returned receipt %+v", receipt)
	}
}

func TestReapReceiptRejectsForgeryWithoutForgettingDurableProof(t *testing.T) {
	ctx := t.Context()
	store := &memStore{}
	record := matchingRecord(4042, "prior-generation")
	mustAdd(t, store, record)
	reaper := &Reaper{
		Store: store, Generation: "current-generation",
		prober: &fakeProber{err: errNoProc},
	}
	if err := reaper.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := reaper.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	receipt := result.Receipts[0]
	forged := receipt
	forged.Record.Generation = "forged-generation"
	if err := reaper.VerifyReapReceipt(ctx, forged); !errors.Is(err, ErrInvalidReapReceipt) {
		t.Fatalf("Verify forged digest = %v, want invalid receipt", err)
	}
	other, err := newReapReceipt(
		ReceiptLedgerID{1}, receipt.Sequence,
		matchingRecord(4043, "prior-generation"), "current-generation", ReapAbsent,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.VerifyReapReceipt(ctx, other); !errors.Is(err, ErrUnrecognizedReapReceipt) {
		t.Fatalf("Verify uncommitted receipt = %v, want unrecognized", err)
	}
	if _, err := reaper.AcknowledgeReap(ctx, other); !errors.Is(err, ErrUnrecognizedReapReceipt) {
		t.Fatalf("acknowledge absent receipt = %v, want unrecognized", err)
	}
	if err := reaper.VerifyReapReceipt(ctx, receipt); err != nil {
		t.Fatalf("forged acknowledgement removed exact proof: %v", err)
	}
}

func TestReapReceiptRejectsLiveSignedProcessSubstitution(t *testing.T) {
	const pid = 4044
	record := matchingRecord(pid, "prior-generation")
	record.Executable = "/Applications/Fixed.app/Contents/MacOS/Fixed"
	record.AuditToken = auditTokenForPID(pid, 9)
	store := &memStore{}
	mustAdd(t, store, record)
	reaper := &Reaper{
		Store: store, Generation: "current-generation",
		prober: &fakeProber{info: liveInfo()},
		auditPath: func(AuditToken) (string, error) {
			return "/Applications/Substituted.app/Contents/MacOS/Substituted", nil
		},
	}
	err := reaper.Reap(t.Context())
	if !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("Reap substituted process = %v, want identity rejection", err)
	}
	page, pageErr := reaper.ReapReceipts(t.Context(), RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if pageErr != nil || len(page.Receipts) != 0 || store.len() != 1 {
		t.Fatalf("substituted process produced proof or lost authority: page=%+v err=%v records=%d",
			page, pageErr, store.len())
	}
}

func TestReapReceiptsArePageBoundedWithoutDroppingKillAuthority(t *testing.T) {
	ctx := t.Context()
	store := &memStore{}
	const total = ReapReceiptPageLimit + 1
	for index := range total {
		record := matchingRecord(5000+index, "prior-generation")
		record.Boot = "prior-boot"
		mustAdd(t, store, record)
	}
	reaper := &Reaper{
		Store: store, Generation: "current-generation",
		prober: &fakeProber{boot: testBoot},
	}
	if err := reaper.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := reaper.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Receipts) != ReapReceiptPageLimit || !first.More || store.len() != 0 {
		t.Fatalf("first bounded Reap = receipts:%d more:%t records:%d",
			len(first.Receipts), first.More, store.len())
	}
	hiddenRecord := matchingRecord(5000+ReapReceiptPageLimit, "prior-generation")
	hiddenRecord.Boot = "prior-boot"
	hiddenReceipt, found, err := reaper.ReapReceipt(ctx, hiddenRecord)
	if err != nil || hiddenReceipt.Record != hiddenRecord {
		t.Fatalf("exact hidden receipt = %+v, found %t, err %v", hiddenReceipt, found, err)
	}
	replayedHidden, found, err := reaper.ReapReceipt(ctx, hiddenRecord)
	if err != nil || !found || replayedHidden != hiddenReceipt {
		t.Fatalf("exact hidden receipt = %+v, found %t, err %v", replayedHidden, found, err)
	}
	for _, receipt := range first.Receipts {
		if _, err := reaper.AcknowledgeReap(ctx, receipt); err != nil {
			t.Fatal(err)
		}
	}
	second, err := reaper.ReapReceipts(ctx, RecoveryTask, first.Next, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Receipts) != 1 || second.More || store.len() != 0 {
		t.Fatalf("second bounded Reap = receipts:%d more:%t records:%d",
			len(second.Receipts), second.More, store.len())
	}
}

func auditTokenForPID(pid int, version uint32) AuditToken {
	var token AuditToken
	binary.NativeEndian.PutUint32(token[20:24], uint32(pid))
	binary.NativeEndian.PutUint32(token[28:32], version)
	return token
}

func TestRecordRejectsPresentIncompleteAuditToken(t *testing.T) {
	rec := matchingRecord(4242, "generation")
	rec.AuditToken = auditTokenForPID(rec.PID, 0)
	if err := rec.Validate(); !errors.Is(err, ErrNoAuditToken) {
		t.Fatalf("Validate error = %v, want ErrNoAuditToken", err)
	}
}

func TestReapRetainedAuthenticatedAppRecordUsesAuditTokenAuthority(t *testing.T) {
	const pid = 4242
	token := auditTokenForPID(pid, 17)
	rec := matchingRecord(pid, "stopped-daemon")
	rec.Executable = "/Applications/Fixed.app/Contents/MacOS/Fixed"
	rec.AuditToken = token
	store := &memStore{recs: []Record{rec}}
	pathCalls := 0
	var signals []syscall.Signal
	r := &Reaper{
		Store: store, Generation: "restarted-daemon", prober: &fakeProber{info: liveInfo()}, clock: newFakeClock(),
		auditPath: func(got AuditToken) (string, error) {
			if got != token {
				t.Fatalf("audit token = %v, want %v", got, token)
			}
			pathCalls++
			if pathCalls > 1 {
				return "", ErrNoProcess
			}
			return rec.Executable, nil
		},
		auditSignal: func(got AuditToken, sig syscall.Signal) (bool, error) {
			if got != token {
				t.Fatalf("audit token = %v, want %v", got, token)
			}
			signals = append(signals, sig)
			return false, nil
		},
	}
	if err := r.Reap(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(signals, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals = %v, want TERM through audit-token authority", signals)
	}
	if store.len() != 0 {
		t.Fatal("retained app record remained after exact process settlement")
	}
}

func TestTerminateIdentityRejectsNearMatchesWithoutSignal(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity Identity
		prober   *fakeProber
	}{
		{
			name:     "boot",
			identity: Identity{PID: 4242, StartTime: liveInfo().startTime, Boot: "prior-boot", Comm: liveInfo().comm},
			prober:   &fakeProber{info: liveInfo()},
		},
		{
			name:     "start",
			identity: Identity{PID: 4242, StartTime: "prior-start", Boot: testBoot, Comm: liveInfo().comm},
			prober:   &fakeProber{info: liveInfo()},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			signals := &recSignaler{}
			reaper := &Reaper{Generation: "controller", prober: test.prober, signaler: signals, clock: newFakeClock()}
			if err := reaper.TerminateIdentityWithin(t.Context(), test.identity, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if calls := signals.calls(); len(calls) != 0 {
				t.Fatalf("signals = %v, want none", calls)
			}
		})
	}

	const pid = 4242
	for _, test := range []struct {
		name     string
		identity Identity
		path     string
	}{
		{
			name: "audit pid",
			identity: Identity{
				PID: pid + 1, StartTime: liveInfo().startTime, Boot: testBoot, Comm: liveInfo().comm,
				Executable: "/Applications/Fixed.app/Contents/MacOS/Fixed", AuditToken: auditTokenForPID(pid, 17),
			},
			path: "/Applications/Fixed.app/Contents/MacOS/Fixed",
		},
		{
			name: "executable path",
			identity: Identity{
				PID: pid, StartTime: liveInfo().startTime, Boot: testBoot, Comm: liveInfo().comm,
				Executable: "/Applications/Fixed.app/Contents/MacOS/Fixed", AuditToken: auditTokenForPID(pid, 17),
			},
			path: "/Applications/Other.app/Contents/MacOS/Other",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var signals []syscall.Signal
			reaper := &Reaper{
				Generation: "controller", prober: &fakeProber{info: liveInfo()}, clock: newFakeClock(),
				auditPath: func(AuditToken) (string, error) { return test.path, nil },
				auditSignal: func(_ AuditToken, signal syscall.Signal) (bool, error) {
					signals = append(signals, signal)
					return false, nil
				},
			}
			if err := reaper.TerminateIdentityWithin(t.Context(), test.identity, time.Millisecond); err == nil {
				t.Fatal("TerminateIdentityWithin accepted a near-match identity")
			}
			if len(signals) != 0 {
				t.Fatalf("signals = %v, want none", signals)
			}
		})
	}
}

func matchingGroupRecord(pid int, gen string) Record {
	rec := matchingRecord(pid, gen)
	rec.ProcessGroup = true
	rec.SessionID = pid
	return rec
}

func groupInfo(pid int, startTime, comm string) procInfo {
	return procInfo{startTime: startTime, comm: comm, groupID: pid, sessionID: pid}
}

func TestTrackGroupAndUntrack(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("/bin/sh", "-c", "read _")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	store := &memStore{}
	r := &Reaper{Store: store, Generation: "current-gen"}
	rec, err := r.TrackGroup(ctx, cmd.Process.Pid, RecoveryTask)
	if err != nil {
		t.Fatalf("TrackGroup: %v", err)
	}
	if !rec.ProcessGroup {
		t.Fatal("ProcessGroup = false, want true")
	}
	if rec.SessionID != cmd.Process.Pid {
		t.Fatalf("SessionID = %d, want %d", rec.SessionID, cmd.Process.Pid)
	}
	if rec.PID != cmd.Process.Pid || rec.Generation != "current-gen" {
		t.Fatalf("record = %+v, want pid %d generation current-gen", rec, cmd.Process.Pid)
	}
	boot, err := BootID()
	if err != nil {
		t.Fatalf("BootID: %v", err)
	}
	if rec.Boot != boot {
		t.Fatalf("record boot = %q, want current kernel boot %q", rec.Boot, boot)
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want 1", store.len())
	}
	if err := r.Untrack(ctx, rec); err != nil {
		t.Fatalf("Untrack: %v", err)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want 0", store.len())
	}
}

func TestTrackGroupRejectsNonLeader(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "read _")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	r := &Reaper{
		Store:      &memStore{},
		Generation: "current-gen",
		prober:     &fakeProber{info: liveInfo()},
	}
	if _, err := r.TrackGroup(context.Background(), cmd.Process.Pid, RecoveryTask); err == nil {
		t.Fatal("TrackGroup succeeded for a process-group member, want error")
	}
}

func TestOwnsRevalidatesProcessInstance(t *testing.T) {
	rec := matchingRecord(4242, "old-gen")
	tests := []struct {
		name    string
		result  probeResult
		want    bool
		wantErr bool
	}{
		{name: "match", result: probeResult{info: liveInfo()}, want: true},
		{name: "vanished", result: probeResult{err: errNoProc}},
		{name: "pid reused", result: probeResult{info: procInfo{startTime: "new", comm: rec.Comm}}},
		{name: "exec preserves identity", result: probeResult{info: procInfo{startTime: rec.StartTime, comm: "other"}}, want: true},
		{name: "probe failure", result: probeResult{err: errors.New("probe failed")}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reaper{prober: &fakeProber{perProbe: []probeResult{tt.result}}}
			got, err := r.Owns(rec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Owns error = %v, wantErr %t", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("Owns = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestTrackIdentityRejectsMismatchAndAbsence(t *testing.T) {
	identity := Identity{PID: 4242, StartTime: liveInfo().startTime, Comm: liveInfo().comm, Boot: testBoot}
	tests := []struct {
		name  string
		probe probeResult
		want  error
	}{
		{name: "pid reused", probe: probeResult{info: procInfo{startTime: "reused"}}, want: ErrIdentityChanged},
		{name: "absent", probe: probeResult{err: errNoProc}, want: ErrNoProcess},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &memStore{}
			reaper := &Reaper{Store: store, Generation: "stop", prober: &fakeProber{perProbe: []probeResult{test.probe}}}
			if _, err := reaper.TrackIdentity(t.Context(), identity, RecoveryTrust); !errors.Is(err, test.want) {
				t.Fatalf("TrackIdentity error = %v, want %v", err, test.want)
			}
			if store.len() != 0 {
				t.Fatalf("store size = %d, want no authority over mismatched process", store.len())
			}
		})
	}
}

func TestTerminateTrackedIdentityEscalatesAndSettles(t *testing.T) {
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()}, // TrackIdentity.
		{info: liveInfo()}, // Initial termination revalidation.
		{info: liveInfo()}, // TERM-resistant grace expiry.
		{err: errNoProc},   // KILL settlement.
	}}
	signaler := &recSignaler{}
	reaper := &Reaper{
		Store: store, Generation: "stop", prober: prober, signaler: signaler, clock: newFakeClock(),
		Grace: time.Millisecond, Settlement: time.Millisecond,
	}
	identity := Identity{PID: 4242, StartTime: liveInfo().startTime, Comm: liveInfo().comm, Boot: testBoot}
	record, err := reaper.TrackIdentity(t.Context(), identity, RecoveryTrust)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.Terminate(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	want := []signalCall{{pid: identity.PID, sig: syscall.SIGTERM}, {pid: identity.PID, sig: syscall.SIGKILL}}
	if got := signaler.calls(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("signals = %v, want %v", got, want)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want settled record removed", store.len())
	}
}

func TestTerminateTrackedIdentityNeverKillsReusedPID(t *testing.T) {
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: liveInfo()},
		{info: procInfo{startTime: "reused", comm: "innocent"}},
	}}
	signaler := &recSignaler{}
	reaper := &Reaper{Store: store, Generation: "stop", prober: prober, signaler: signaler, clock: newFakeClock()}
	identity := Identity{PID: 4242, StartTime: liveInfo().startTime, Comm: liveInfo().comm, Boot: testBoot}
	record, err := reaper.TrackIdentity(t.Context(), identity, RecoveryTrust)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.Terminate(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if got := signaler.calls(); len(got) != 1 || got[0].sig != syscall.SIGTERM {
		t.Fatalf("signals = %v, want TERM only before PID reuse", got)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want stale identity removed", store.len())
	}
}

func TestTerminateTrackedIdentityCancellationRetainsDurableRecord(t *testing.T) {
	store := &memStore{}
	record := matchingRecord(4242, "stop")
	mustAdd(t, store, record)
	ctx, cancel := context.WithCancel(context.Background())
	signaler := &cancelSignaler{cancel: cancel}
	reaper := &Reaper{Store: store, Generation: "stop", prober: &fakeProber{info: liveInfo()}, signaler: signaler}
	if err := reaper.Terminate(ctx, record); !errors.Is(err, context.Canceled) {
		t.Fatalf("Terminate error = %v, want context canceled", err)
	}
	if calls := signaler.calls(); len(calls) != 1 || calls[0].sig != syscall.SIGTERM || store.len() != 1 {
		t.Fatalf("signals = %v store size = %d, want TERM and retained record", calls, store.len())
	}
}

func TestTerminateRefusesUntrackedIdentity(t *testing.T) {
	signaler := &recSignaler{}
	reaper := &Reaper{
		Store: &memStore{}, Generation: "stop", prober: &fakeProber{info: liveInfo()}, signaler: signaler,
	}
	if err := reaper.Terminate(t.Context(), matchingRecord(4242, "stop")); err == nil {
		t.Fatal("Terminate accepted an identity without durable ownership")
	}
	if calls := signaler.calls(); len(calls) != 0 {
		t.Fatalf("signals = %v, want none", calls)
	}
}

func TestOwnsRequiresCurrentBootBeforeProcessProbe(t *testing.T) {
	rec := matchingRecord(4242, "old-gen")
	prober := &fakeProber{
		boot: "current-boot",
		info: liveInfo(),
	}
	r := &Reaper{prober: prober}

	rec.Boot = "prior-boot"
	owned, err := r.Owns(rec)
	if err != nil {
		t.Fatalf("Owns cross-boot record: %v", err)
	}
	if owned {
		t.Fatal("Owns cross-boot record = true, want stale")
	}
	if got := prober.probedPIDs(); len(got) != 0 {
		t.Fatalf("probed pids = %v, want none for cross-boot record", got)
	}
}

func TestOwnsRejectsMissingBootBeforeProcessProbe(t *testing.T) {
	rec := matchingRecord(4242, "old-gen")
	rec.Boot = ""
	prober := &fakeProber{info: liveInfo()}
	r := &Reaper{prober: prober}

	owned, err := r.Owns(rec)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Owns error = %v, want ErrInvalidRecord", err)
	}
	if owned {
		t.Fatal("Owns incomplete record = true, want false")
	}
	if got := prober.probedPIDs(); len(got) != 0 {
		t.Fatalf("probed pids = %v, want none for incomplete record", got)
	}
}

func TestReapSignalsProcessGroup(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	info := groupInfo(4141, liveInfo().startTime, liveInfo().comm)
	members := []groupMember{{pid: 4141, info: info}}
	prober := &fakeProber{info: info, memberSets: [][]groupMember{members, members, nil}}
	sig := &recSignaler{}
	rec := matchingGroupRecord(4141, "old-gen")
	mustAdd(t, store, rec)

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Outcome != ReapTerminated ||
		result.Receipts[0].Record != rec {
		t.Fatalf("Reap receipt = %+v, want exact terminated group", result)
	}
	want := []signalCall{{pid: -rec.PID, sig: syscall.SIGTERM}, {pid: -rec.PID, sig: syscall.SIGKILL}}
	got := sig.calls()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("signals = %v, want %v", got, want)
	}
}

func TestReapLeaderlessGroupUsesDurableSessionMembers(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	leaderPID := 4151
	memberPID := 4152
	memberInfo := groupInfo(leaderPID, "222.333", "descendant")
	prober := &fakeProber{
		byPID: map[int]probeResult{
			leaderPID: {err: errNoProc},
			memberPID: {info: memberInfo},
		},
		memberSets: [][]groupMember{
			{{pid: memberPID, info: memberInfo}},
			{{pid: memberPID, info: memberInfo}},
			nil,
		},
	}
	sig := &recSignaler{}
	rec := matchingGroupRecord(leaderPID, "old-gen")
	mustAdd(t, store, rec)

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Record != rec ||
		result.Receipts[0].Outcome != ReapTerminated {
		t.Fatalf("leaderless group Reap receipt = %+v", result)
	}
	want := []signalCall{{pid: -leaderPID, sig: syscall.SIGTERM}, {pid: -leaderPID, sig: syscall.SIGKILL}}
	got := sig.calls()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("signals = %v, want %v", got, want)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want reaped leaderless record removed", store.len())
	}
}

func TestReapSignalsEveryGroupInDedicatedSession(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	leaderPID := 4171
	descendantGroup := 4172
	leader := groupInfo(leaderPID, liveInfo().startTime, liveInfo().comm)
	descendant := groupInfo(leaderPID, "333.444", "descendant")
	descendant.groupID = descendantGroup
	members := []groupMember{{pid: leaderPID, info: leader}, {pid: descendantGroup, info: descendant}}
	prober := &fakeProber{
		info: leader,
		byPID: map[int]probeResult{
			leaderPID:       {info: leader},
			descendantGroup: {info: descendant},
		},
		memberSets: [][]groupMember{members, members, nil},
	}
	signals := &recSignaler{}
	record := matchingGroupRecord(leaderPID, "old-gen")
	mustAdd(t, store, record)
	reaper := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: signals, clock: newFakeClock()}
	if err := reaper.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	want := []signalCall{
		{pid: -leaderPID, sig: syscall.SIGTERM},
		{pid: -descendantGroup, sig: syscall.SIGTERM},
		{pid: -leaderPID, sig: syscall.SIGKILL},
		{pid: -descendantGroup, sig: syscall.SIGKILL},
	}
	got := signals.calls()
	if len(got) != len(want) {
		t.Fatalf("signals = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("signals = %v, want %v", got, want)
		}
	}
}

func TestReapKillsGroupAppearingDuringDedicatedSessionSettlement(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	leaderPID := 4181
	descendantGroup := 4182
	leader := groupInfo(leaderPID, liveInfo().startTime, liveInfo().comm)
	descendant := groupInfo(leaderPID, "444.555", "late-descendant")
	descendant.groupID = descendantGroup
	leaderMember := groupMember{pid: leaderPID, info: leader}
	descendantMember := groupMember{pid: descendantGroup, info: descendant}
	prober := &fakeProber{
		info: leader,
		byPID: map[int]probeResult{
			leaderPID:       {info: leader},
			descendantGroup: {info: descendant},
		},
		memberSets: [][]groupMember{
			{leaderMember},
			{leaderMember},
			{descendantMember},
			nil,
		},
	}
	signals := &recSignaler{}
	record := matchingGroupRecord(leaderPID, "old-gen")
	mustAdd(t, store, record)
	reaper := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: signals, clock: newFakeClock()}
	if err := reaper.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	want := []signalCall{
		{pid: -leaderPID, sig: syscall.SIGTERM},
		{pid: -leaderPID, sig: syscall.SIGKILL},
		{pid: -descendantGroup, sig: syscall.SIGKILL},
	}
	got := signals.calls()
	if !slices.Equal(got, want) {
		t.Fatalf("signals = %v, want %v", got, want)
	}
}

func TestReapLeaderlessGroupEnumerationFailureFailsRecovery(t *testing.T) {
	store := &memStore{}
	rec := matchingGroupRecord(4161, "old-gen")
	mustAdd(t, store, rec)
	prober := &fakeProber{
		byPID:    map[int]probeResult{rec.PID: {err: errNoProc}},
		groupErr: errors.New("process table unavailable"),
	}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig}
	if err := r.Reap(context.Background()); err == nil || !strings.Contains(err.Error(), "process table unavailable") {
		t.Fatalf("Reap error = %v, want unresolved enumeration failure", err)
	}
	if len(sig.calls()) != 0 {
		t.Fatalf("signals = %v, want none without member proof", sig.calls())
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want forensic record retained", store.len())
	}
}

func TestReapRejectsLegacyGroupWithoutSessionIdentity(t *testing.T) {
	store := &memStore{}
	rec := matchingRecord(4171, "old-gen")
	rec.ProcessGroup = true
	mustAdd(t, store, rec)
	prober := &fakeProber{byPID: map[int]probeResult{rec.PID: {err: errNoProc}}}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig}
	if err := r.Reap(context.Background()); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Reap error = %v, want ErrInvalidRecord", err)
	}
	if got := prober.probedPIDs(); len(got) != 0 {
		t.Fatalf("probed pids = %v, want none for invalid record", got)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Fatalf("signals = %v, want none for invalid record", got)
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want incompatible record retained", store.len())
	}
}

func TestReapDropsCrossBootRecordWithoutProbeOrSignal(t *testing.T) {
	store := &memStore{}
	rec := matchingRecord(4181, "old-gen")
	rec.Boot = "prior-boot"
	mustAdd(t, store, rec)
	prober := &fakeProber{boot: "current-boot", info: liveInfo()}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig}

	if err := r.Reap(context.Background()); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if got := prober.probedPIDs(); len(got) != 0 {
		t.Fatalf("probed pids = %v, want none for stale cross-boot record", got)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Fatalf("signals = %v, want none for stale cross-boot record", got)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want stale cross-boot record removed", store.len())
	}
}

func TestReapPIDReuseResistance(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: errors.New("signal must not be sent")}

	rec := matchingRecord(4242, "old-gen")
	rec.StartTime = "999999.000000"
	mustAdd(t, store, rec)

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Record != rec ||
		result.Receipts[0].Outcome != ReapIdentityReused {
		t.Fatalf("PID-reuse Reap receipt = %+v", result)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none (innocent live process must not be killed)", got)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0 (stale record dropped)", store.len())
	}
}

func TestReapProbeErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{err: errors.New("kern.proc probe failed")}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(5252, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err == nil || !strings.Contains(err.Error(), "kern.proc probe failed") {
		t.Fatalf("Reap error = %v, want unresolved probe failure", err)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none on Undetermined probe", got)
	}
	if store.len() != 1 {
		t.Errorf("store size = %d, want 1 (record kept, fail closed)", store.len())
	}
}

func TestReapStaleRecordCleanup(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{err: errNoProc}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(6262, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Outcome != ReapAbsent {
		t.Fatalf("stale-record Reap receipt = %+v", result)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none for a vanished process", got)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0 (stale record cleaned up)", store.len())
	}
}

func TestReapESRCHOnSignalIsSuccess(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: syscall.ESRCH}
	mustAdd(t, store, matchingRecord(7272, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 1 || result.Receipts[0].Outcome != ReapAbsent {
		t.Fatalf("ESRCH Reap receipt = %+v", result)
	}
	calls := sig.calls()
	if len(calls) != 1 || calls[0].sig != syscall.SIGTERM {
		t.Errorf("signals = %v, want a single SIGTERM (ESRCH ends the ladder)", calls)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0 (ESRCH is success)", store.len())
	}
}

func TestReapRefusesSelfAndPID1(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(1, "old-gen"))
	mustAdd(t, store, matchingRecord(0, "old-gen"))
	mustAdd(t, store, matchingRecord(os.Getpid(), "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err == nil || !strings.Contains(err.Error(), "refusing unsafe process identity") {
		t.Fatalf("Reap error = %v, want unsafe identity refusal", err)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none (self/init refusal)", got)
	}
	if got := prober.probedPIDs(); len(got) != 0 {
		t.Errorf("probed pids = %v, want none (refused before probing)", got)
	}
	if store.len() != 3 {
		t.Errorf("store size = %d, want 3 (refused records kept untouched)", store.len())
	}
}

func TestReapSkipsOwnGeneration(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(8282, "current-gen"))

	r := &Reaper{Store: store, Generation: "current-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	err := r.Reap(ctx)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	result, err := r.ReapReceipts(ctx, RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatalf("ReapReceipts: %v", err)
	}
	if len(result.Receipts) != 0 {
		t.Fatalf("own-generation Reap produced receipts: %+v", result)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none for our own generation", got)
	}
	if store.len() != 1 {
		t.Errorf("store size = %d, want 1 (own-generation record kept)", store.len())
	}
}

func TestReapPIDReuseDuringGrace(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: procInfo{startTime: "555.000000", comm: "someoneelse"}},
	}}
	sig := &recSignaler{}
	mustAdd(t, store, matchingRecord(9292, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	calls := sig.calls()
	if len(calls) != 1 || calls[0].sig != syscall.SIGTERM {
		t.Errorf("signals = %v, want a single SIGTERM (no SIGKILL after pid reuse)", calls)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0 (our orphan is gone)", store.len())
	}
}

func TestReapExecWithStableStartIdentityStillEscalates(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: procInfo{startTime: liveInfo().startTime, comm: "execd-away"}},
	}}
	sig := &recSignaler{}
	mustAdd(t, store, matchingRecord(9393, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	calls := sig.calls()
	if len(calls) != 2 || calls[0].sig != syscall.SIGTERM || calls[1].sig != syscall.SIGKILL {
		t.Errorf("signals = %v, want SIGTERM then SIGKILL for the same process instance", calls)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0", store.len())
	}
}

func TestReapReprobeErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{err: errors.New("re-probe failed")},
	}}
	sig := &recSignaler{}
	mustAdd(t, store, matchingRecord(9494, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err == nil || !strings.Contains(err.Error(), "re-probe failed") {
		t.Fatalf("Reap error = %v, want unresolved re-probe failure", err)
	}
	calls := sig.calls()
	if len(calls) != 1 || calls[0].sig != syscall.SIGTERM {
		t.Errorf("signals = %v, want SIGTERM only (Undetermined re-probe blocks SIGKILL)", calls)
	}
	if store.len() != 1 {
		t.Errorf("store size = %d, want 1 (record kept, fail closed)", store.len())
	}
}

func TestReapRemovesRecordOnlyAfterPostKillAbsence(t *testing.T) {
	store := &memStore{}
	rec := matchingRecord(9501, "old-gen")
	mustAdd(t, store, rec)
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: liveInfo()},
		{info: liveInfo()},
		{info: liveInfo()},
		{err: errNoProc},
	}}
	sig := &recSignaler{}
	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock(), Settlement: 50 * time.Millisecond}
	if err := r.Reap(context.Background()); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if store.len() != 0 {
		t.Fatalf("store size = %d, want removal after absence proof", store.len())
	}
	if probes := prober.probedPIDs(); len(probes) != 5 {
		t.Fatalf("probe count = %d, want initial, grace, and three settlement probes", len(probes))
	}
}

func TestReapRetainsRecordWhenKilledProcessNeverSettles(t *testing.T) {
	store := &memStore{}
	rec := matchingRecord(9502, "old-gen")
	mustAdd(t, store, rec)
	prober := &fakeProber{info: liveInfo()}
	r := &Reaper{
		Store: store, Generation: "new-gen", prober: prober,
		signaler: &recSignaler{}, clock: newFakeClock(), Settlement: 25 * time.Millisecond,
	}
	err := r.Reap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "remained live through settlement deadline") {
		t.Fatalf("Reap error = %v, want settlement deadline failure", err)
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want unresolved record retained", store.len())
	}
}

func TestReapRetainsRecordWhenKilledGroupNeverSettles(t *testing.T) {
	store := &memStore{}
	rec := matchingGroupRecord(9503, "old-gen")
	mustAdd(t, store, rec)
	info := groupInfo(rec.PID, rec.StartTime, rec.Comm)
	member := groupMember{pid: rec.PID, info: info}
	prober := &fakeProber{info: info, members: []groupMember{member}}
	r := &Reaper{
		Store: store, Generation: "new-gen", prober: prober,
		signaler: &recSignaler{}, clock: newFakeClock(), Settlement: 25 * time.Millisecond,
	}
	err := r.Reap(context.Background())
	if err == nil || !strings.Contains(err.Error(), "group remained live through settlement deadline") {
		t.Fatalf("Reap error = %v, want group settlement deadline failure", err)
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want unresolved group record retained", store.len())
	}
}

func TestReapLadderRealChild(t *testing.T) {
	ctx := context.Background()
	pid, wait := startTermIgnorer(t)

	dir, err := os.MkdirTemp("/tmp", "reaper")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store := &FileStore{Path: filepath.Join(dir, "recovery.db")}

	old := &Reaper{Store: store, Generation: "old-gen"}
	if _, err := old.Track(ctx, pid, RecoveryTask); err != nil {
		t.Fatalf("Track: %v", err)
	}

	sig := &recSignaler{delegate: sysSignaler{}}
	r := &Reaper{Store: store, Generation: "new-gen", signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	calls := sig.calls()
	if len(calls) != 2 || calls[0].sig != syscall.SIGTERM || calls[1].sig != syscall.SIGKILL {
		t.Fatalf("signal ladder = %v, want [SIGTERM SIGKILL] (child ignores SIGTERM)", calls)
	}

	wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("child pid %d still signalable = %v, want ESRCH (SIGKILL should have reaped it)", pid, err)
	}
	left, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("records after reap = %v, want empty", left)
	}
}

func startTermIgnorer(t *testing.T) (int, func()) {
	t.Helper()
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	rout, wout, err := os.Pipe()
	if err != nil {
		pr.Close()
		pw.Close()
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", `trap "" TERM; echo r; read _`)
	cmd.Stdin = pr
	cmd.Stdout = wout
	if err := cmd.Start(); err != nil {
		pr.Close()
		pw.Close()
		rout.Close()
		wout.Close()
		t.Fatalf("start term-ignorer: %v", err)
	}
	pr.Close()
	wout.Close()
	if _, err := rout.Read(make([]byte, 1)); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		pw.Close()
		rout.Close()
		t.Fatalf("await term-ignorer ready: %v", err)
	}
	rout.Close()
	var once sync.Once
	wait := func() { once.Do(func() { _ = cmd.Wait() }) }
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		wait()
		pw.Close()
	})
	return cmd.Process.Pid, wait
}

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/tmp", "reaper")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store := &FileStore{Path: filepath.Join(dir, "recovery.db")}

	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Load missing = %v, want empty", got)
	}

	a := Record{RecoveryClass: RecoveryTask, PID: 100, StartTime: "1.1", Boot: testBoot, Comm: "a", Generation: "g1"}
	b := Record{RecoveryClass: RecoveryTask, PID: 200, StartTime: "2.2", Boot: testBoot, Comm: "b", Generation: "g1"}
	mustAdd(t, store, a)
	mustAdd(t, store, b)
	mustAdd(t, store, a)

	got, err = store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("Load after adds = %d records, want 2", len(got))
	}

	if err := store.Remove(ctx, []Record{a}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PID != 200 {
		t.Errorf("Load after remove = %v, want only pid 200", got)
	}
}

func TestFileStoreRemoveByInstance(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("/tmp", "reaper")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store := &FileStore{Path: filepath.Join(dir, "recovery.db")}

	current := Record{RecoveryClass: RecoveryTask, PID: 300, StartTime: "9.9", Boot: "current-boot", Comm: "new", Generation: "g2"}
	prior := Record{RecoveryClass: RecoveryTask, PID: 300, StartTime: "9.9", Boot: "prior-boot", Comm: "old", Generation: "g1"}
	mustAdd(t, store, prior)
	mustAdd(t, store, current)

	if err := store.Remove(ctx, []Record{prior}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != current {
		t.Errorf("Load = %v, want only current-boot instance %v", got, current)
	}
}
