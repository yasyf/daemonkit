package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

var errStreamClosed = errors.New("wire: stream closed")
var errStreamSealed = errors.New("wire: stream sealed")

type streamSequence struct {
	next      uint32
	exhausted bool
}

func (s *streamSequence) take() (uint32, error) {
	if s.exhausted {
		return 0, fmt.Errorf("%w: sequence exhausted", ErrStreamOrder)
	}
	value := s.next
	if s.next == math.MaxUint32 {
		s.exhausted = true
	} else {
		s.next++
	}
	return value, nil
}

type boundedStream[T any] struct {
	values chan T
	done   chan struct{}

	mu        sync.Mutex
	closed    bool
	senders   sync.WaitGroup
	closeOnce sync.Once
}

type latestStream[T any] struct {
	notify chan struct{}
	done   chan struct{}

	mu        sync.Mutex
	value     T
	ready     bool
	sealed    bool
	terminal  T
	hasTerm   bool
	closed    bool
	closeOnce sync.Once
}

func newLatestStream[T any]() *latestStream[T] {
	return &latestStream[T]{notify: make(chan struct{}, 1), done: make(chan struct{})}
}

func (s *latestStream[T]) offer(value T) error {
	return s.offerValue(value)
}

func (s *latestStream[T]) offerTerminalExact(value T, equal func(T, T) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStreamClosed
	}
	if s.sealed {
		if s.hasTerm && equal != nil && equal(s.terminal, value) {
			return nil
		}
		return errStreamSealed
	}
	s.value = value
	s.ready = true
	s.sealed = true
	s.terminal = value
	s.hasTerm = true
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

func (s *latestStream[T]) offerValue(value T) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStreamClosed
	}
	if s.sealed {
		return errStreamSealed
	}
	s.value = value
	s.ready = true
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

func (s *latestStream[T]) next(ctx context.Context) (T, error) {
	for {
		if value, err, ok := s.try(); ok {
			return value, err
		}
		if err := s.wait(ctx); err != nil {
			var zero T
			return zero, err
		}
	}
}

func (s *latestStream[T]) wait(ctx context.Context) error {
	select {
	case <-s.notify:
		return nil
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *latestStream[T]) try() (T, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready {
		value := s.value
		var zero T
		s.value = zero
		s.ready = false
		if s.closed {
			return value, errStreamClosed, true
		}
		return value, nil, true
	}
	if s.closed {
		var zero T
		return zero, errStreamClosed, true
	}
	var zero T
	return zero, nil, false
}

func (s *latestStream[T]) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
}

type latestWriteLane struct {
	notify chan struct{}

	mu      sync.Mutex
	changed chan struct{}
	desired uint64
	taken   uint64
	written uint64
	payload []byte
	sealed  bool
	err     error
}

func newLatestWriteLane() *latestWriteLane {
	return &latestWriteLane{notify: make(chan struct{}, 1), changed: make(chan struct{})}
}

func (l *latestWriteLane) offer(payload []byte, terminal bool) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, l.err
	}
	if l.sealed {
		if terminal && bytes.Equal(l.payload, payload) {
			return l.desired, nil
		}
		return 0, errStreamSealed
	}
	if l.desired == math.MaxUint64 {
		return 0, ErrFlowControl
	}
	l.desired++
	l.payload = append(l.payload[:0], payload...)
	l.sealed = terminal
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return l.desired, nil
}

func (l *latestWriteLane) next(ctx context.Context) (uint64, []byte, error) {
	for {
		generation, payload, ok, err := l.tryTake()
		if err != nil {
			return 0, nil, err
		}
		if ok {
			return generation, payload, nil
		}
		select {
		case <-l.notify:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
}

func (l *latestWriteLane) tryTake() (uint64, []byte, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return 0, nil, false, l.err
	}
	if l.taken >= l.desired {
		return 0, nil, false, nil
	}
	l.taken = l.desired
	select {
	case <-l.notify:
	default:
	}
	return l.taken, append([]byte(nil), l.payload...), true, nil
}

func (l *latestWriteLane) complete(generation uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	if err != nil {
		l.err = err
		l.payload = nil
	} else if generation > l.written {
		l.written = generation
	}
	close(l.changed)
	l.changed = make(chan struct{})
	if err != nil {
		select {
		case l.notify <- struct{}{}:
		default:
		}
	}
}

func (l *latestWriteLane) wait(ctx context.Context, generation uint64) error {
	for {
		l.mu.Lock()
		if l.written >= generation {
			l.mu.Unlock()
			return nil
		}
		if l.err != nil {
			err := l.err
			l.mu.Unlock()
			return err
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *latestWriteLane) fail(err error) {
	if err == nil {
		err = errStreamClosed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	l.err = err
	l.payload = nil
	close(l.changed)
	l.changed = make(chan struct{})
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func newBoundedStream[T any](capacity int) *boundedStream[T] {
	return &boundedStream[T]{
		values: make(chan T, capacity),
		done:   make(chan struct{}),
	}
}

func (s *boundedStream[T]) channel() <-chan T { return s.values }

func (s *boundedStream[T]) offer(value T) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errStreamClosed
	}
	s.senders.Add(1)
	s.mu.Unlock()
	defer s.senders.Done()

	select {
	case s.values <- value:
		return nil
	case <-s.done:
		return errStreamClosed
	default:
		return ErrFlowControl
	}
}

func (s *boundedStream[T]) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.done)
		s.mu.Unlock()
		s.senders.Wait()
		close(s.values)
	})
}

type creditWindow struct {
	done   chan struct{}
	notify chan struct{}

	mu        sync.Mutex
	credits   uint64
	closed    bool
	closeOnce sync.Once
}

func newCreditWindow() *creditWindow {
	return &creditWindow{done: make(chan struct{}), notify: make(chan struct{}, 1)}
}

func (w *creditWindow) acquire(ctx context.Context) error {
	for {
		w.mu.Lock()
		if w.credits > 0 {
			w.credits--
			if w.credits > 0 {
				select {
				case w.notify <- struct{}{}:
				default:
				}
			}
			w.mu.Unlock()
			return nil
		}
		closed := w.closed
		w.mu.Unlock()
		if closed {
			return errStreamClosed
		}
		select {
		case <-w.notify:
		case <-w.done:
			return errStreamClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *creditWindow) grant(count uint32) error {
	if count == 0 {
		return ErrFlowControl
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errStreamClosed
	}
	if w.credits > math.MaxUint64-uint64(count) {
		return ErrFlowControl
	}
	w.credits += uint64(count)
	select {
	case w.notify <- struct{}{}:
	default:
	}
	return nil
}

func (w *creditWindow) close() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		close(w.done)
	})
}
