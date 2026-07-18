package drain

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

type simpleRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *simpleRecorder) record(what string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, what)
}

func (r *simpleRecorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...)
}

func (r *simpleRecorder) config() SimpleConfig {
	return SimpleConfig{
		Deactivate:      func(context.Context) error { r.record("deactivate"); return nil },
		MarkClosing:     func() { r.record("mark-closing") },
		CancelExecutors: func() { r.record("cancel-executors") },
	}
}

func TestSimpleDrainOrder(t *testing.T) {
	var s Simple
	rec := &simpleRecorder{}
	if err := s.Drain(context.Background(), rec.config()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	want := []string{"deactivate", "mark-closing", "cancel-executors"}
	got := rec.calls()
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSimpleAdmissionCountsQueuedWork(t *testing.T) {
	var s Simple
	rec := &simpleRecorder{}
	var closing atomic.Bool
	deactivated := make(chan struct{})
	cfg := rec.config()
	cfg.Deactivate = func(context.Context) error { rec.record("deactivate"); close(deactivated); return nil }
	cfg.MarkClosing = func() { closing.Store(true); rec.record("mark-closing") }

	runningDone, err := s.Admit()
	if err != nil {
		t.Fatal(err)
	}
	queuedDone, err := s.Admit()
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.Drain(context.Background(), cfg) }()
	<-deactivated

	runningDone()
	if closing.Load() {
		t.Fatal("pools marked closing while admitted-queued work is in flight")
	}
	if _, err := s.Admit(); !errors.Is(err, ErrDraining) {
		t.Errorf("Admit during drain err = %v, want ErrDraining", err)
	}
	queuedDone()
	if err := <-errCh; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if !closing.Load() {
		t.Error("pools never marked closing after settle")
	}
	got := rec.calls()
	want := []string{"deactivate", "mark-closing", "cancel-executors"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStampsDuplicateContentHashExecutesOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "stamps")
	old := Stamps{Dir: dir}
	newer := Stamps{Dir: dir}

	executed := 0
	for _, s := range []Stamps{old, newer} {
		if s.Claim("sha256:abcd") {
			executed++
		}
	}
	if executed != 1 {
		t.Errorf("duplicate content-hash executed %d times across instances, want 1", executed)
	}
}

func TestStampsDistinctKeysBothExecute(t *testing.T) {
	s := Stamps{Dir: t.TempDir()}
	if !s.Claim("sha256:aaaa") || !s.Claim("sha256:bbbb") {
		t.Error("distinct content hashes were deduped")
	}
}

func TestStampsFailOpenOnFSError(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := Stamps{Dir: filepath.Join(blocker, "stamps")}
	for i := range 2 {
		if !s.Claim("sha256:abcd") {
			t.Errorf("claim %d blocked a request on FS error; must fail open", i)
		}
	}
}
