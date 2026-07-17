package proc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// mustAdd stores rec or fails the test.
func mustAdd(t *testing.T, s Store, rec Record) {
	t.Helper()
	if err := s.Add(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}

// memStore is an in-memory Store double for the reaper-logic tests.
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

// fakeProber returns a fixed identity or error for every pid, unless perProbe
// overrides the result on a given (0-indexed) probe.
type fakeProber struct {
	mu       sync.Mutex
	info     procInfo
	err      error
	probed   []int
	perProbe []probeResult
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
	if n < len(f.perProbe) {
		return f.perProbe[n].info, f.perProbe[n].err
	}
	return f.info, f.err
}

func (f *fakeProber) probedPIDs() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.probed...)
}

// recSignaler records every signal and optionally delegates to the OS.
type recSignaler struct {
	mu       sync.Mutex
	sent     []signalCall
	delegate signaler
	err      error // returned for every call when delegate is nil
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

// TestReapPIDReuseResistance: a live, innocent process whose start time differs
// from the record is never signaled, and the stale record is dropped.
func TestReapPIDReuseResistance(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()} // the live process
	sig := &recSignaler{err: errors.New("signal must not be sent")}

	rec := matchingRecord(4242, "old-gen")
	rec.StartTime = "999999.000000" // record predates a reused pid
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

// TestReapProbeErrorFailsClosed: an Undetermined probe signals nothing and keeps
// the record for a later pass.
func TestReapProbeErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{err: errors.New("kern.proc probe failed")}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(5252, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if got := sig.calls(); len(got) != 0 {
		t.Errorf("signals sent = %v, want none on Undetermined probe", got)
	}
	if store.len() != 1 {
		t.Errorf("store size = %d, want 1 (record kept, fail closed)", store.len())
	}
}

// TestReapStaleRecordCleanup: a record whose pid no longer exists is dropped
// without any signal (ESRCH-as-success on the initial probe).
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

// TestReapESRCHOnSignalIsSuccess: an orphan that exits between probe and SIGTERM
// (kill → ESRCH) counts as reaped; the record is dropped.
func TestReapESRCHOnSignalIsSuccess(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: syscall.ESRCH} // process gone by the time we signal
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

// TestReapRefusesSelfAndPID1: records for pid<=1 or the caller's own pid are
// never probed or signaled, and are kept.
func TestReapRefusesSelfAndPID1(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{info: liveInfo()}
	sig := &recSignaler{err: errors.New("signal must not be sent")}
	mustAdd(t, store, matchingRecord(1, "old-gen"))
	mustAdd(t, store, matchingRecord(0, "old-gen"))
	mustAdd(t, store, matchingRecord(os.Getpid(), "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
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

// TestReapSkipsOwnGeneration: a live child bearing the reaper's own generation
// is never signaled and its record is kept.
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

// TestReapPIDReuseDuringGrace: an orphan that dies during the grace window and
// whose pid is reused (new start time) is never SIGKILLed; the record drops.
func TestReapPIDReuseDuringGrace(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	// probe 0: initial revalidation matches; probe 1: post-grace, pid reused.
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: procInfo{startTime: "555.000000", comm: "someoneelse"}},
	}}
	sig := &recSignaler{} // SIGTERM succeeds; SIGKILL must not follow
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

// TestReapCommMismatchDuringGrace: a post-grace probe matching pid+startTime but
// not comm (the process exec'd away) is never SIGKILLed; the record drops.
func TestReapCommMismatchDuringGrace(t *testing.T) {
	ctx := context.Background()
	store := &memStore{}
	prober := &fakeProber{perProbe: []probeResult{
		{info: liveInfo()},
		{info: procInfo{startTime: liveInfo().startTime, comm: "execd-away"}},
	}}
	sig := &recSignaler{} // SIGTERM succeeds; SIGKILL must not follow
	mustAdd(t, store, matchingRecord(9393, "old-gen"))

	r := &Reaper{Store: store, Generation: "new-gen", prober: prober, signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	calls := sig.calls()
	if len(calls) != 1 || calls[0].sig != syscall.SIGTERM {
		t.Errorf("signals = %v, want a single SIGTERM (no SIGKILL on comm mismatch)", calls)
	}
	if store.len() != 0 {
		t.Errorf("store size = %d, want 0 (no longer provably ours)", store.len())
	}
}

// TestReapReprobeErrorFailsClosed: an Undetermined re-probe after SIGTERM stops
// short of SIGKILL and keeps the record.
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
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}
	calls := sig.calls()
	if len(calls) != 1 || calls[0].sig != syscall.SIGTERM {
		t.Errorf("signals = %v, want SIGTERM only (Undetermined re-probe blocks SIGKILL)", calls)
	}
	if store.len() != 1 {
		t.Errorf("store size = %d, want 1 (record kept, fail closed)", store.len())
	}
}

// TestReapLadderRealChild spawns a real SIGTERM-ignoring child through a
// non-test binary, tracks it with the real prober and a FileStore, then proves
// the full SIGTERM → grace → SIGKILL ladder ends the process. It exercises the
// real prober and signaler end to end.
func TestReapLadderRealChild(t *testing.T) {
	ctx := context.Background()
	pid, wait := startTermIgnorer(t)

	dir, err := os.MkdirTemp("/tmp", "reaper")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	store := &FileStore{Path: filepath.Join(dir, "records.json")}

	// Record the child under a prior generation via the real prober.
	old := &Reaper{Store: store, Generation: "old-gen"}
	if _, err := old.Track(ctx, pid); err != nil {
		t.Fatalf("Track: %v", err)
	}

	sig := &recSignaler{delegate: sysSignaler{}} // real kills, recorded
	r := &Reaper{Store: store, Generation: "new-gen", signaler: sig, clock: newFakeClock()}
	if err := r.Reap(ctx); err != nil {
		t.Fatalf("Reap: %v", err)
	}

	calls := sig.calls()
	if len(calls) != 2 || calls[0].sig != syscall.SIGTERM || calls[1].sig != syscall.SIGKILL {
		t.Fatalf("signal ladder = %v, want [SIGTERM SIGKILL] (child ignores SIGTERM)", calls)
	}

	// The child was mine; reap the zombie so kill(pid,0) can report ESRCH.
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

// startTermIgnorer starts a single /bin/sh that installs an empty SIGTERM trap
// and blocks reading a held-open pipe (no child to orphan), so only SIGKILL can
// end it. It returns the pid and a wait func that reaps the zombie.
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
	// echo after trap: the ready byte proves the child has exec'd /bin/sh (comm
	// is stable, no fork-window mismatch) and installed the TERM trap before we
	// Track it.
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
	pr.Close()   // the child holds its own dup; keep pw open so read blocks
	wout.Close() // the child holds its own dup; parent reads readiness on rout
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

	// Missing file loads as empty.
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
	mustAdd(t, store, a) // re-adding the same instance replaces, not duplicates

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

	// Same pid, different start time == different instance (pid reuse).
	reused := Record{PID: 300, StartTime: "9.9", Comm: "new", Generation: "g2"}
	mustAdd(t, store, Record{PID: 300, StartTime: "3.3", Comm: "old", Generation: "g1"})
	mustAdd(t, store, reused)

	// Removing the old instance leaves the reused-pid record intact.
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
