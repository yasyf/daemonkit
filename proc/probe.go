package proc

// Identity is a live process's revalidation identity. A PID paired with a
// matching StartTime is the only safe kill authority: a reused PID gets a
// fresh StartTime.
type Identity struct {
	PID       int
	StartTime string
	Comm      string
	// Boot is the host boot session the identity was probed in; linux start
	// stamps are only unique within one boot, so a foreign Boot reads as dead.
	Boot string
}

// ErrNoProcess means a probed PID has no live process — a definitive "gone",
// distinct from a probe failure; a SIGKILL-authority caller branches on it with errors.Is.
var ErrNoProcess = errNoProc

// Probe reads pid's identity from the live process table, returning ErrNoProcess
// when the process is gone; any other error is a genuine probe failure.
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
