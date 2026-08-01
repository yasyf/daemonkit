package proc

import (
	"context"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func openTestStore(t testing.TB, path string) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := OpenStore(ctx, path)
	if err != nil {
		t.Fatalf("OpenStore() = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestStore(t testing.TB) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "records.dkstate")
	return openTestStore(t, path), path
}

func unheld(*Child) {}

type funcProber struct {
	probeFn   func(pid int) (procInfo, error)
	membersFn func(sessionID int) ([]groupMember, error)
	bootFn    func() (uint64, error)

	mu     sync.Mutex
	probed []int
}

func (p *funcProber) probe(pid int) (procInfo, error) {
	p.mu.Lock()
	p.probed = append(p.probed, pid)
	p.mu.Unlock()
	return p.probeFn(pid)
}

func (p *funcProber) groupMembers(sessionID int) ([]groupMember, error) {
	if p.membersFn == nil {
		return nil, nil
	}
	return p.membersFn(sessionID)
}

func (p *funcProber) boot() (uint64, error) {
	if p.bootFn == nil {
		return testBoot, nil
	}
	return p.bootFn()
}

type sentSignal struct {
	pid int
	sig syscall.Signal
}

type funcSignaler struct {
	fn func(pid int, sig syscall.Signal) error

	mu   sync.Mutex
	sent []sentSignal
}

func (s *funcSignaler) signal(pid int, sig syscall.Signal) error {
	s.mu.Lock()
	s.sent = append(s.sent, sentSignal{pid: pid, sig: sig})
	s.mu.Unlock()
	if s.fn == nil {
		return nil
	}
	return s.fn(pid, sig)
}

func (s *funcSignaler) signals() []sentSignal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sentSignal(nil), s.sent...)
}

const testBoot = uint64(7_777_777_000_001)
