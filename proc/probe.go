package proc

// Identity is a live process's revalidation identity: its PID plus the prober's
// opaque, platform-native start stamp and OS-truncated comm. A PID paired with a
// matching StartTime is the only safe kill authority — a reused PID gets a fresh
// StartTime, so callers compare {PID, StartTime} before they signal. Within one
// boot session start stamps are monotonic across process creations, which keeps
// a recorded identity collision-resistant against PID reuse.
type Identity struct {
	// PID is the probed process id.
	PID int
	// StartTime is the prober's opaque, platform-native process start stamp.
	StartTime string
	// Comm is the OS-reported (truncated) process name.
	Comm string
	// Boot is the host boot session the identity was probed in (BootID). Start
	// stamps are only unique within one boot on linux (ticks since boot), so a
	// recorded identity whose Boot differs from the current session belongs to
	// a process that cannot have survived — liveness checks read it as dead.
	Boot string
}

// ErrNoProcess means a probed PID has no live process — a definitive "gone",
// distinct from a probe failure (an error the caller treats as Undetermined and
// must fail closed on). It is the reaper's internal gone sentinel, exported so a
// SIGKILL-authority caller can branch on it with errors.Is.
var ErrNoProcess = errNoProc

// Probe reads pid's identity from the live process table via the same platform
// prober the reaper revalidates with, so a Probe StartTime and a later reaper
// probe of the same process instance match exactly. It returns ErrNoProcess when
// the process is gone; any other error is a genuine probe failure.
func Probe(pid int) (Identity, error) {
	info, err := probeProc(pid)
	if err != nil {
		return Identity{}, err
	}
	boot, err := BootID()
	if err != nil {
		return Identity{}, err
	}
	return Identity{PID: pid, StartTime: info.startTime, Comm: info.comm, Boot: boot}, nil
}
