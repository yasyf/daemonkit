package daemonkit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/internal/wire"
)

func capacityLane(t *testing.T, attach func(context.Context) (*wire.Client, error)) *Business {
	t.Helper()
	return &Business{contract: Contract{Schema: "test.v1"}, attach: attach}
}

func TestAcquireRetriesSessionCapacity(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, error) {
		attempts++
		if attempts < attachAttempts {
			return nil, wire.ErrSessionCapacity
		}
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := lane.acquire(ctx); err != nil {
		t.Fatalf("acquire() through a momentarily full slot table = %v, want success", err)
	}
	if attempts != attachAttempts {
		t.Fatalf("attach attempts = %d, want %d", attempts, attachAttempts)
	}
}

func TestAcquireGivesUpOnSustainedCapacity(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, error) {
		attempts++
		return nil, wire.ErrSessionCapacity
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := lane.acquire(ctx)
	if !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("acquire() against a permanently full slot table = %v, want ErrSessionCapacity", err)
	}
	if attempts != attachAttempts {
		t.Fatalf("attach attempts = %d, want exactly %d; capacity must stay bounded", attempts, attachAttempts)
	}
}

func TestAcquireDoesNotRetryATrustDenial(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, error) {
		attempts++
		return nil, ErrUntrusted
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := lane.acquire(ctx); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("acquire() = %v, want ErrUntrusted", err)
	}
	if attempts != 1 {
		t.Fatalf("attach attempts = %d, want 1; a trust denial is never retried", attempts)
	}
}

func TestAcquireStopsRetryingWhenTheCallerGivesUp(t *testing.T) {
	attempts := 0
	lane := capacityLane(t, func(context.Context) (*wire.Client, error) {
		attempts++
		return nil, wire.ErrSessionCapacity
	})
	ctx, cancel := context.WithTimeout(context.Background(), capacityBackoff/2)
	defer cancel()

	if _, err := lane.acquire(ctx); !errors.Is(err, ErrSessionCapacity) {
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
