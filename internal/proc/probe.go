package proc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	darwinStopState   = 4
	darwinZombieState = 5
)

// errNoProc is the package's one definitive "gone" sentinel, distinct from a
// probe failure, which is undetermined and fails closed.
var errNoProc = errors.New("proc: no such process")

type procInfo struct {
	start   uint64
	comm    string
	group   int
	session int
	zombie  bool
	stopped bool
}

type groupMember struct {
	pid  int
	info procInfo
}

type prober interface {
	probe(pid int) (procInfo, error)
	groupMembers(sessionID int) ([]groupMember, error)
	boot() (uint64, error)
}

type sysProber struct{}

func (sysProber) probe(pid int) (procInfo, error) { return probeProc(pid) }

func (sysProber) groupMembers(sessionID int) ([]groupMember, error) {
	return probeGroupMembers(sessionID)
}

func (sysProber) boot() (uint64, error) { return bootSession() }

type signaler interface {
	signal(pid int, sig syscall.Signal) error
}

type sysSignaler struct{}

func (sysSignaler) signal(pid int, sig syscall.Signal) error { return syscall.Kill(pid, sig) }

func probeProc(pid int) (procInfo, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return procInfo{}, fmt.Errorf("sysctl kern.proc.pid %d: %w", pid, err)
	}
	if len(procs) == 0 {
		return procInfo{}, errNoProc
	}
	kp := procs[0]
	sid, err := unix.Getsid(pid)
	if errors.Is(err, unix.ESRCH) {
		return procInfo{}, errNoProc
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("getsid %d: %w", pid, err)
	}
	return procInfoFromKinfo(kp, sid), nil
}

func probeGroupMembers(sessionID int) ([]groupMember, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("sysctl kern.proc.all: %w", err)
	}
	members := make([]groupMember, 0)
	for _, kp := range procs {
		pid := int(kp.Proc.P_pid)
		if pid <= 1 {
			continue
		}
		sid, err := unix.Getsid(pid)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("getsid %d while enumerating session %d: %w", pid, sessionID, err)
		}
		if sid != sessionID {
			continue
		}
		members = append(members, groupMember{pid: pid, info: procInfoFromKinfo(kp, sid)})
	}
	return members, nil
}

func procInfoFromKinfo(kp unix.KinfoProc, sessionID int) procInfo {
	st := kp.Proc.P_starttime
	comm := string(kp.Proc.P_comm[:])
	if i := strings.IndexByte(comm, 0); i >= 0 {
		comm = comm[:i]
	}
	return procInfo{
		start:   microStamp(st.Sec, int64(st.Usec)),
		comm:    comm,
		group:   int(kp.Eproc.Pgid),
		session: sessionID,
		zombie:  kp.Proc.P_stat == darwinZombieState,
		stopped: kp.Proc.P_stat == darwinStopState,
	}
}

func bootSession() (uint64, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return 0, fmt.Errorf("sysctl kern.boottime: %w", err)
	}
	return microStamp(tv.Sec, int64(tv.Usec)), nil
}

func microStamp(sec, usec int64) uint64 {
	return uint64(sec)*1_000_000 + uint64(usec) //nolint:gosec // kernel stamps are non-negative
}

// parseLegacyStart maps v1's darwin "%d.%06d" start stamp onto the frozen
// seconds×1e6+microseconds numeric encoding.
func parseLegacyStart(stamp string) (uint64, error) { return parseMicroStamp(stamp) }

// parseLegacyBoot maps v1's darwin kern.boottime stamp identically.
func parseLegacyBoot(stamp string) (uint64, error) { return parseMicroStamp(stamp) }

func parseMicroStamp(stamp string) (uint64, error) {
	secText, usecText, ok := strings.Cut(stamp, ".")
	if !ok || len(usecText) != 6 {
		return 0, fmt.Errorf("proc: malformed legacy stamp %q", stamp)
	}
	sec, err := strconv.ParseUint(secText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("proc: malformed legacy stamp %q: %w", stamp, err)
	}
	usec, err := strconv.ParseUint(usecText, 10, 64)
	if err != nil || usec >= 1_000_000 {
		return 0, fmt.Errorf("proc: malformed legacy stamp %q", stamp)
	}
	return sec*1_000_000 + usec, nil
}
