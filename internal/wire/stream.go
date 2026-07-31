package wire

import (
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

// offer blocks past the buffer bound, propagating consumer pressure to the
// caller — the socket, when the caller is a read loop.
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
