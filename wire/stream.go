package wire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

var errStreamClosed = errors.New("wire: stream closed")

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
