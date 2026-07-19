package drain

import (
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/proc"
)

func TestAllowForce(t *testing.T) {
	tests := []struct {
		name   string
		policy ForcePolicy
		live   Liveness
		want   bool
	}{
		{"defer never forces on dead", ForcePolicyDefer, Dead, false},
		{"defer never forces on undetermined", ForcePolicyDefer, Undetermined, false},
		{"confirmed-dead forces only on dead", ForcePolicyConfirmedDead, Dead, true},
		{"confirmed-dead refuses alive", ForcePolicyConfirmedDead, Alive, false},
		{"confirmed-dead refuses undetermined", ForcePolicyConfirmedDead, Undetermined, false},
		{"zero policy is defer", 0, Dead, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AllowForce(tt.policy, tt.live); got != tt.want {
				t.Errorf("AllowForce(%v, %v) = %v, want %v", tt.policy, tt.live, got, tt.want)
			}
		})
	}
}

func TestAssess(t *testing.T) {
	id := proc.Identity{PID: 42, StartTime: "111.222", Comm: "daemon"}
	t.Run("boot session", func(t *testing.T) {
		booted := id
		booted.Boot = "boot-a"
		alive := proberResult{id: proc.Identity{PID: 42, StartTime: "111.222", Comm: "daemon"}}
		tests := []struct {
			name    string
			bootID  string
			bootErr error
			want    Liveness
		}{
			{"mismatched boot is dead despite a matching probe", "boot-b", nil, Dead},
			{"matching boot falls through to the probe", "boot-a", nil, Alive},
			{"boot read failure is undetermined", "", errors.New("sysctl failed"), Undetermined},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				p := &fakeProber{
					results: map[int]proberResult{42: alive},
					bootID:  tt.bootID,
					bootErr: tt.bootErr,
				}
				if got := assess(p, booted); got != tt.want {
					t.Errorf("assess = %v, want %v", got, tt.want)
				}
			})
		}
	})
	tests := []struct {
		name string
		res  proberResult
		want Liveness
	}{
		{"gone is dead", proberResult{err: proc.ErrNoProcess}, Dead},
		{"stat timeout is undetermined", proberResult{err: errors.New("stat timed out")}, Undetermined},
		{"reused pid is dead", proberResult{id: proc.Identity{PID: 42, StartTime: "999.0", Comm: "daemon"}}, Dead},
		{"matching instance is alive", proberResult{id: id}, Alive},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProber{results: map[int]proberResult{42: tt.res}}
			if got := assess(p, id); got != tt.want {
				t.Errorf("assess() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLivenessString(t *testing.T) {
	tests := []struct {
		live Liveness
		want string
	}{
		{Undetermined, "undetermined"},
		{Alive, "alive"},
		{Dead, "dead"},
		{Liveness(9), "Liveness(9)"},
	}
	for _, tt := range tests {
		if got := tt.live.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}
