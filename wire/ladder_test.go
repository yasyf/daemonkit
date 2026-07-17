package wire_test

import (
	"errors"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
)

func TestNewLadder(t *testing.T) {
	ms := time.Millisecond
	tests := []struct {
		name    string
		server  map[wire.Op]time.Duration
		client  map[wire.Op]time.Duration
		wantErr error
	}{
		{
			name:   "client outlives server",
			server: map[wire.Op]time.Duration{"mount": 5 * ms, "health": 1 * ms},
			client: map[wire.Op]time.Duration{"mount": 10 * ms, "health": 2 * ms},
		},
		{
			name:    "server equals client",
			server:  map[wire.Op]time.Duration{"mount": 5 * ms},
			client:  map[wire.Op]time.Duration{"mount": 5 * ms},
			wantErr: wire.ErrLadderInverted,
		},
		{
			name:    "server exceeds client",
			server:  map[wire.Op]time.Duration{"mount": 9 * ms},
			client:  map[wire.Op]time.Duration{"mount": 5 * ms},
			wantErr: wire.ErrLadderInverted,
		},
		{
			name:    "server-only op",
			server:  map[wire.Op]time.Duration{"mount": 1 * ms, "extra": 1 * ms},
			client:  map[wire.Op]time.Duration{"mount": 2 * ms},
			wantErr: wire.ErrLadderMissingPair,
		},
		{
			name:    "client-only op",
			server:  map[wire.Op]time.Duration{"mount": 1 * ms},
			client:  map[wire.Op]time.Duration{"mount": 2 * ms, "extra": 2 * ms},
			wantErr: wire.ErrLadderMissingPair,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := wire.NewLadder(tt.server, tt.client)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewLadder err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			for op, wantS := range tt.server {
				s, c, ok := l.Deadlines(op)
				if !ok {
					t.Errorf("Deadlines(%q) missing", op)
					continue
				}
				if s != wantS || c != tt.client[op] {
					t.Errorf("Deadlines(%q) = (%s, %s), want (%s, %s)", op, s, c, wantS, tt.client[op])
				}
			}
			if _, _, ok := l.Deadlines("absent"); ok {
				t.Error("Deadlines(absent) ok = true, want false")
			}
		})
	}
}

func TestNewLadderCopiesInput(t *testing.T) {
	server := map[wire.Op]time.Duration{"mount": time.Second}
	client := map[wire.Op]time.Duration{"mount": 2 * time.Second}
	l, err := wire.NewLadder(server, client)
	if err != nil {
		t.Fatalf("NewLadder: %v", err)
	}
	server["mount"] = 99 * time.Second
	client["mount"] = 3 * time.Second
	s, c, _ := l.Deadlines("mount")
	if s != time.Second || c != 2*time.Second {
		t.Errorf("ladder aliased its input maps: got (%s, %s), want (1s, 2s)", s, c)
	}
}
