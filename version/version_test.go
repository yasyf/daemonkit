package version

import (
	"testing"
	"time"
)

func TestNewer(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	devBase := DevString(base)
	devNext := DevString(base.Add(time.Nanosecond))

	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"release newer major", "2.0.0", "1.9.9", true},
		{"release older major", "1.0.0", "2.0.0", false},
		{"release newer minor", "v1.2.0", "1.1.9", true},
		{"release suffix ties", "v0.8.0-1-gHASH", "v0.8.0", false},
		{"release exact tie not newer", "1.2.3", "1.2.3", false},
		{"legacy dev beats release", "dev", "1.2.3", true},
		{"release never beats legacy dev", "1.2.3", "dev", false},
		{"mtime dev beats release", devBase, "9.9.9", true},
		{"release never beats mtime dev", "9.9.9", devBase, false},
		{"mtime dev beats legacy dev", devBase, "dev", true},
		{"legacy dev loses to mtime dev", "dev", devBase, false},
		{"newer rebuild wins", devNext, devBase, true},
		{"older rebuild loses", devBase, devNext, false},
		{"same rebuild exact tie not newer", devBase, devBase, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Newer(tt.a, tt.b); got != tt.want {
				t.Errorf("Newer(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Version
	}{
		{"release triple", "1.2.3", Release{Major: 1, Minor: 2, Patch: 3}},
		{"release v prefix and suffix", "v0.8.0-1-gHASH", Release{Minor: 8}},
		{"dev sentinel", "9999.1700000000000000000.0-dev", Dev{BuildUnixNano: 1_700_000_000_000_000_000}},
		{"plain dev is zero nanos", "dev", Dev{}},
		{"non-triple is dev", "garbage", Dev{}},
		{"malformed sentinel is dev", "9999.abc.0-dev", Dev{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Parse(tt.in); got != tt.want {
				t.Errorf("Parse(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestDevString(t *testing.T) {
	got := DevString(time.Unix(1_700_000_000, 1))
	if want := "9999.1700000000000000001.0-dev"; got != want {
		t.Fatalf("DevString = %q, want %q", got, want)
	}
}

func TestResolve(t *testing.T) {
	mtime := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name    string
		stamped string
		mtime   time.Time
		ok      bool
		want    string
	}{
		{"stamped release passthrough", "1.2.3", time.Time{}, false, "1.2.3"},
		{"unstamped with mtime", "dev", mtime, true, "9999.1700000000000000000.0-dev"},
		{"unstamped fallback", "dev", time.Time{}, false, "dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Resolve(tt.stamped, tt.mtime, tt.ok); got != tt.want {
				t.Errorf("Resolve(%q, %v, %v) = %q, want %q", tt.stamped, tt.mtime, tt.ok, got, tt.want)
			}
		})
	}
}
