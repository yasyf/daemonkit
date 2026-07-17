package daemon

import (
	"context"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// autoClock advances Now by each After's duration and fires immediately, so a
// bounded poll terminates without real sleeps (mirrors proc's test clock).
type autoClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAutoClock() *autoClock {
	return &autoClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *autoClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *autoClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	fire := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- fire
	return ch
}

// healthResult is one scripted Health return.
type healthResult struct {
	h   Health
	err error
}

// fakePeer scripts Health returns (consumed in order, the last repeating) and
// counts Shutdown/Handoff calls.
type fakePeer struct {
	mu          sync.Mutex
	health      []healthResult
	hi          int
	shutdowns   int
	handoffs    int
	shutdownErr error
	handoffErr  error
}

func (p *fakePeer) Health(context.Context) (Health, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.health[clamp(p.hi, len(p.health))]
	p.hi++
	return r.h, r.err
}

func (p *fakePeer) Shutdown(context.Context) error {
	p.mu.Lock()
	p.shutdowns++
	p.mu.Unlock()
	return p.shutdownErr
}

func (p *fakePeer) Handoff(context.Context) error {
	p.mu.Lock()
	p.handoffs++
	p.mu.Unlock()
	return p.handoffErr
}

func (p *fakePeer) counts() (shutdowns, handoffs int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shutdowns, p.handoffs
}

func (p *fakePeer) healthCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hi
}

// proberResult is one scripted probe return.
type proberResult struct {
	id  proc.Identity
	err error
}

// fakeProber scripts probe returns (consumed in order, the last repeating).
type fakeProber struct {
	mu      sync.Mutex
	results []proberResult
	i       int
}

func (p *fakeProber) probe(pid int) (proc.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.results[clamp(p.i, len(p.results))]
	p.i++
	if r.err == nil && r.id.PID == 0 {
		r.id.PID = pid
	}
	return r.id, r.err
}

// constProber returns the same result for every pid; a nil err with a matching
// pid is the default "alive".
type constProber struct {
	id  proc.Identity
	err error
}

func (p constProber) probe(pid int) (proc.Identity, error) {
	if p.err == nil && p.id.PID == 0 {
		return proc.Identity{PID: pid, StartTime: p.id.StartTime, Comm: p.id.Comm}, nil
	}
	return p.id, p.err
}

// signalRec is one recorded signal delivery.
type signalRec struct {
	pid int
	sig syscall.Signal
}

// fakeSignaler records every signal and returns err for each.
type fakeSignaler struct {
	mu   sync.Mutex
	sent []signalRec
	err  error
}

func (s *fakeSignaler) signal(pid int, sig syscall.Signal) error {
	s.mu.Lock()
	s.sent = append(s.sent, signalRec{pid, sig})
	s.mu.Unlock()
	return s.err
}

func (s *fakeSignaler) calls() []signalRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]signalRec(nil), s.sent...)
}

func clamp(i, n int) int {
	if i >= n {
		return n - 1
	}
	return i
}
