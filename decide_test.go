package daemonkit

import (
	"testing"

	"github.com/yasyf/daemonkit/internal/wire"
)

func healthy(mutate func(*Health)) Health {
	health := Health{
		Phase:      PhaseReady,
		Protocol:   wire.ProtocolVersion,
		Generation: 0x5eed,
		PID:        4242,
		Build:      "wanted",
	}
	if mutate != nil {
		mutate(&health)
	}
	return health
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name     string
		observed Health
		want     string
		action   Action
		refused  bool
	}{
		{
			name:     "unnamed phase is an incomplete identity",
			observed: healthy(func(h *Health) { h.Phase = phaseInvalid }),
			want:     "wanted",
			action:   actionInvalid,
			refused:  true,
		},
		{
			name:     "a foreign protocol is an incomplete identity",
			observed: healthy(func(h *Health) { h.Protocol = wire.ProtocolVersion + 1 }),
			want:     "wanted",
			action:   actionInvalid,
			refused:  true,
		},
		{
			name:     "an unstamped build is an incomplete identity",
			observed: healthy(func(h *Health) { h.Build = "" }),
			want:     "",
			action:   actionInvalid,
			refused:  true,
		},
		{
			name:     "a zero generation is an incomplete identity",
			observed: healthy(func(h *Health) { h.Generation = 0 }),
			want:     "wanted",
			action:   actionInvalid,
			refused:  true,
		},
		{
			name:     "a different build is upgraded",
			observed: healthy(func(h *Health) { h.Build = "obsolete" }),
			want:     "wanted",
			action:   ActionUpgraded,
		},
		{
			name:     "a different build starting is upgraded, never waited on",
			observed: healthy(func(h *Health) { h.Build, h.Phase = "obsolete", PhaseStarting }),
			want:     "wanted",
			action:   ActionUpgraded,
		},
		{
			name:     "a different build draining is upgraded, never waited on",
			observed: healthy(func(h *Health) { h.Build, h.Phase = "obsolete", PhaseDraining }),
			want:     "wanted",
			action:   ActionUpgraded,
		},
		{
			name:     "a failed runtime is restarted",
			observed: healthy(func(h *Health) { h.Phase = PhaseFailed }),
			want:     "wanted",
			action:   ActionRestarted,
		},
		{
			name:     "a starting runtime is transitional",
			observed: healthy(func(h *Health) { h.Phase = PhaseStarting }),
			want:     "wanted",
			action:   actionObserve,
		},
		{
			name:     "a draining runtime is transitional",
			observed: healthy(func(h *Health) { h.Phase = PhaseDraining }),
			want:     "wanted",
			action:   actionObserve,
		},
		{
			name:     "the wanted build ready is left alone",
			observed: healthy(nil),
			want:     "wanted",
			action:   ActionNothing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, err := decide(tt.observed, tt.want)
			if tt.refused != (err != nil) {
				t.Fatalf("decide() error = %v, want refusal = %v", err, tt.refused)
			}
			if action != tt.action {
				t.Fatalf("decide() = %v, want %v", action, tt.action)
			}
		})
	}
}

func TestDecideIsPure(t *testing.T) {
	observed := healthy(func(h *Health) { h.Detail = []byte("product bytes") })
	first, err := decide(observed, "wanted")
	if err != nil {
		t.Fatalf("decide() error = %v", err)
	}
	second, err := decide(observed, "wanted")
	if err != nil {
		t.Fatalf("decide() error = %v", err)
	}
	if first != second {
		t.Fatalf("decide() = %v then %v for the same input", first, second)
	}
	if string(observed.Detail) != "product bytes" {
		t.Fatalf("decide() mutated its input detail to %q", observed.Detail)
	}
}

func TestActionString(t *testing.T) {
	tests := []struct {
		action Action
		want   string
	}{
		{ActionNothing, "nothing"},
		{ActionStarted, "started"},
		{ActionUpgraded, "upgraded"},
		{ActionRestarted, "restarted"},
		{actionObserve, "observe"},
		{actionInvalid, "Action(0)"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.action.String(); got != tt.want {
				t.Fatalf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
