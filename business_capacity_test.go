package daemonkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

func capacityLane(t *testing.T, attach func(context.Context) (*wire.Client, int, error)) *Business {
	t.Helper()
	return &Business{contract: Contract{Schema: "test.v1"}, attach: attach}
}

func TestAcquireRetriesSessionCapacity(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, int, error) {
		attempts++
		if attempts < attachAttempts {
			return nil, 0, wire.ErrSessionCapacity
		}
		return nil, 0, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := lane.acquire(ctx); err != nil {
		t.Fatalf("acquire() through a momentarily full slot table = %v, want success", err)
	}
	if attempts != attachAttempts {
		t.Fatalf("attach attempts = %d, want %d", attempts, attachAttempts)
	}
}

func TestAcquireGivesUpOnSustainedCapacity(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, int, error) {
		attempts++
		return nil, 0, wire.ErrSessionCapacity
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := lane.acquire(ctx)
	if !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("acquire() against a permanently full slot table = %v, want ErrSessionCapacity", err)
	}
	if attempts != attachAttempts {
		t.Fatalf("attach attempts = %d, want exactly %d; capacity must stay bounded", attempts, attachAttempts)
	}
}

func TestAcquireDoesNotRetryAProtocolMismatch(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, int, error) {
		attempts++
		return nil, 0, wire.ErrProtocolVersion
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, _, err := lane.acquire(ctx); !errors.Is(err, ErrProtocolMismatch) {
		t.Fatalf("acquire() = %v, want ErrProtocolMismatch", err)
	}
	if attempts != 1 {
		t.Fatalf("attach attempts = %d, want 1; a foreign protocol is an answer, never retried", attempts)
	}
}

func TestAcquireStopsRetryingWhenTheCallerGivesUp(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, int, error) {
		attempts++
		return nil, 0, wire.ErrSessionCapacity
	})
	ctx, cancel := context.WithTimeout(context.Background(), capacityBackoff/2)
	defer cancel()

	if _, _, err := lane.acquire(ctx); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("acquire() = %v, want ErrSessionCapacity", err)
	}
	if attempts != 1 {
		t.Fatalf("attach attempts = %d, want 1; an expired caller deadline ends the backoff", attempts)
	}
}

func TestClassifyWireKeepsSessionCapacityIdentity(t *testing.T) {
	if got := classifyWire(wire.ErrSessionCapacity); !errors.Is(got, ErrSessionCapacity) {
		t.Fatalf("classifyWire(ErrSessionCapacity) = %v, want the public ErrSessionCapacity identity", got)
	}
}
