package proc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func mustAdd(t *testing.T, s Store, rec Record) {
	t.Helper()
	if err := s.Add(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

type memStore struct {
	mu   sync.Mutex
	recs []Record
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
	drop := make(map[string]struct{}, len(victims))
	for _, v := range victims {
		drop[recordKey(v)] = struct{}{}
	}
	out := m.recs[:0:0]
	for _, e := range m.recs {
		if _, ok := drop[recordKey(e)]; !ok {
			out = append(out, e)
		}
	}
	m.recs = out
	return nil
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
	return Record{PID: pid, StartTime: i.startTime, Comm: i.comm, Generation: gen}
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
	rec, err := r.TrackGroup(ctx, cmd.Process.Pid)
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
	if _, err := r.TrackGroup(context.Background(), cmd.Process.Pid); err == nil {
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	r := &Reaper{Store: store, Generation: "new-gen", prober: prober}
	if err := r.Reap(context.Background()); err == nil || !strings.Contains(err.Error(), "no durable dedicated-session identity") {
		t.Fatalf("Reap error = %v, want missing session identity failure", err)
	}
	if store.len() != 1 {
		t.Fatalf("store size = %d, want incompatible record retained", store.len())
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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
	store := &FileStore{Path: filepath.Join(dir, "records.json")}

	old := &Reaper{Store: store, Generation: "old-gen"}
	if _, err := old.Track(ctx, pid); err != nil {
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
	store := &FileStore{Path: filepath.Join(dir, "records.json")}

	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Load missing = %v, want empty", got)
	}

	a := Record{PID: 100, StartTime: "1.1", Comm: "a", Generation: "g1"}
	b := Record{PID: 200, StartTime: "2.2", Comm: "b", Generation: "g1"}
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
	store := &FileStore{Path: filepath.Join(dir, "records.json")}

	reused := Record{PID: 300, StartTime: "9.9", Comm: "new", Generation: "g2"}
	mustAdd(t, store, Record{PID: 300, StartTime: "3.3", Comm: "old", Generation: "g1"})
	mustAdd(t, store, reused)

	if err := store.Remove(ctx, []Record{{PID: 300, StartTime: "3.3"}}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].StartTime != "9.9" {
		t.Errorf("Load = %v, want only the reused-pid instance (start 9.9)", got)
	}
}
