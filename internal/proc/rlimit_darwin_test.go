package proc

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestRestoredNprocStaysUnderTheAppliedHardLimit(t *testing.T) {
	cases := []struct {
		name    string
		orig    unix.Rlimit
		applied unix.Rlimit
		want    unix.Rlimit
	}{
		{"launchd hard limit above maxprocperuid", unix.Rlimit{Cur: 10666, Max: 16000}, unix.Rlimit{Cur: 2608, Max: 10666}, unix.Rlimit{Cur: 10666, Max: 10666}},
		{"hard limit already clamped", unix.Rlimit{Cur: 2500, Max: 10666}, unix.Rlimit{Cur: 2400, Max: 10666}, unix.Rlimit{Cur: 2500, Max: 10666}},
		{"soft limit above the applied hard limit", unix.Rlimit{Cur: 16000, Max: 16000}, unix.Rlimit{Cur: 2608, Max: 10666}, unix.Rlimit{Cur: 10666, Max: 10666}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := restoredNproc(tc.orig, tc.applied); got != tc.want {
				t.Fatalf("restoredNproc(%+v, %+v) = %+v, want %+v", tc.orig, tc.applied, got, tc.want)
			}
		})
	}
}

func TestChildNprocCapRestoresTheSoftLimitAcrossSpawns(t *testing.T) {
	var start unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &start); err != nil {
		t.Fatalf("Getrlimit() = %v", err)
	}
	raised := unix.Rlimit{Cur: start.Max, Max: start.Max}
	if err := unix.Setrlimit(unix.RLIMIT_NPROC, &raised); err != nil {
		t.Fatalf("raise the soft limit to the hard limit: %v", err)
	}
	var before unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &before); err != nil {
		t.Fatalf("Getrlimit() = %v", err)
	}
	t.Cleanup(func() {
		reset := restoredNproc(start, before)
		if err := unix.Setrlimit(unix.RLIMIT_NPROC, &reset); err != nil {
			t.Errorf("reset RLIMIT_NPROC to %+v: %v", reset, err)
		}
	})

	for range 2 {
		var during unix.Rlimit
		err := withChildNprocCap(func() error { return unix.Getrlimit(unix.RLIMIT_NPROC, &during) })
		if err != nil {
			t.Fatalf("withChildNprocCap() = %v", err)
		}
		if during.Cur >= before.Cur {
			t.Fatalf("soft limit during the spawn = %d, want it lowered below %d", during.Cur, before.Cur)
		}
		var after unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_NPROC, &after); err != nil {
			t.Fatalf("Getrlimit() = %v", err)
		}
		if after.Cur != before.Cur {
			t.Fatalf("soft limit after the spawn = %d, want it restored to %d", after.Cur, before.Cur)
		}
	}
}
