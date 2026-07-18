package drain

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

type autoClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAutoClock() *autoClock {
	return &autoClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *autoClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Millisecond)
	return c.now
}

func (c *autoClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

type proberResult struct {
	id  proc.Identity
	err error
}

type fakeProber struct {
	results map[int]proberResult
	bootID  string
	bootErr error
}

func (p *fakeProber) probe(pid int) (proc.Identity, error) {
	r, ok := p.results[pid]
	if !ok {
		return proc.Identity{}, proc.ErrNoProcess
	}
	return r.id, r.err
}

func (p *fakeProber) boot() (string, error) {
	return p.bootID, p.bootErr
}

const seedInc = "seed-inc"

func newGen(t *testing.T, dotdir, name string) Generation {
	t.Helper()
	g, err := NewGeneration(dotdir, name)
	if err != nil {
		t.Fatalf("NewGeneration %q: %v", name, err)
	}
	return g
}

func seedOwner(t *testing.T, g Generation, id proc.Identity) Generation {
	t.Helper()
	if err := g.writeOwnerUnlocked(id, seedInc); err != nil {
		t.Fatalf("writeOwnerUnlocked: %v", err)
	}
	return Generation{dir: g.dir, inc: seedInc}
}

type fakeFence struct {
	mu       sync.Mutex
	held     bool
	released bool
}

func (f *fakeFence) Held() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held
}

func (f *fakeFence) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = false
	f.released = true
}

type fakeResources struct {
	mu      sync.Mutex
	keys    []Key
	keysErr error
	seize   func(Key) (Fence, error)
	attest  func(Key) (IdleVerdict, error)
	yield   func(Key, Fence) error
	restore func(context.Context, Key, Fence) error
	log     []string
}

func (r *fakeResources) record(op string, key Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, fmt.Sprintf("%s %s", op, key))
}

func (r *fakeResources) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.log...)
}

func (r *fakeResources) Keys(context.Context) ([]Key, error) {
	r.record("keys", "*")
	return r.keys, r.keysErr
}

func (r *fakeResources) Seize(_ context.Context, key Key) (Fence, error) {
	r.record("seize", key)
	if r.seize != nil {
		return r.seize(key)
	}
	return &fakeFence{held: true}, nil
}

func (r *fakeResources) AttestIdle(_ context.Context, key Key) (IdleVerdict, error) {
	r.record("attest", key)
	if r.attest != nil {
		return r.attest(key)
	}
	return IdleConfirmed, nil
}

func (r *fakeResources) Yield(_ context.Context, key Key, fence Fence) error {
	r.record("yield", key)
	if r.yield != nil {
		return r.yield(key, fence)
	}
	return nil
}

func (r *fakeResources) Restore(ctx context.Context, key Key, fence Fence) error {
	r.record("restore", key)
	if r.restore != nil {
		return r.restore(ctx, key, fence)
	}
	return nil
}

func mustApply(t *testing.T, j Journal, rows ...Row) {
	t.Helper()
	n, err := j.apply(context.Background(), rows...)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if n != len(rows) {
		t.Fatalf("apply applied %d rows, want %d", n, len(rows))
	}
}

func mustRows(t *testing.T, j Journal) map[Key]Row {
	t.Helper()
	rows, err := j.Rows(context.Background())
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	return rows
}

func indexOf(t *testing.T, log []string, entry string, from int) int {
	t.Helper()
	for i := from; i < len(log); i++ {
		if log[i] == entry {
			return i
		}
	}
	t.Fatalf("entry %q not found in %v from index %d", entry, log, from)
	return -1
}
