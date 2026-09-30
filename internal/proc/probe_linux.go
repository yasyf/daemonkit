package proc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	bootIDPath = "/proc/sys/kernel/random/boot_id"

	taskExiting      = 0x00000004 // PF_EXITING
	taskKernelThread = 0x00200000 // PF_KTHREAD

	// Indexes into /proc/<pid>/stat counted from the field after the comm,
	// which is the third: proc(5) numbers state 3, pgrp 5, session 6, flags 9,
	// num_threads 20 and starttime 22.
	statState     = 0
	statGroup     = 2
	statSession   = 3
	statFlags     = 6
	statThreads   = 17
	statStart     = 19
	statMinFields = statStart + 1
)

// taskStat is the one read of /proc/<pid>/stat every probe decides from.
type taskStat struct {
	comm    string
	state   byte
	group   int
	session int
	flags   uint64
	threads int
	start   uint64
}

// zombie reports a process that has wholly exited and only awaits its reap. A
// thread-group leader that exited ahead of its threads reads Z too, and is a
// live process: the kernel counts its surviving threads beside it.
func (s taskStat) zombie() bool {
	return (s.state == 'Z' || s.state == 'X') && s.threads <= 1
}

func (s taskStat) info() procInfo {
	return procInfo{
		start:   s.start,
		comm:    s.comm,
		group:   s.group,
		session: s.session,
		zombie:  s.zombie(),
		stopped: s.state == 'T',
		exiting: s.flags&taskExiting != 0,
	}
}

func probeProc(pid int) (procInfo, error) {
	stat, err := readStat(pid)
	if err != nil {
		return procInfo{}, err
	}
	return stat.info(), nil
}

func probeGroupMembers(sessionID int) ([]groupMember, error) {
	pids, err := tablePIDs()
	if err != nil {
		return nil, err
	}
	members := make([]groupMember, 0)
	for _, pid := range pids {
		if pid <= 1 {
			continue
		}
		stat, err := readStat(pid)
		if errors.Is(err, errNoProc) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat %d while enumerating session %d: %w", pid, sessionID, err)
		}
		if stat.session != sessionID || stat.zombie() {
			continue
		}
		members = append(members, groupMember{pid: pid, info: stat.info()})
	}
	return members, nil
}

// bootSession folds the kernel's per-boot random id into the boot stamp. The
// boot time /proc/stat reports is wall clock minus uptime and moves when the
// clock is stepped, which would read a live process as one from another boot.
func bootSession() (uint64, error) {
	raw, err := os.ReadFile(bootIDPath)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", bootIDPath, err)
	}
	id, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(string(raw)), "-", ""))
	if err != nil || len(id) != 16 {
		return 0, fmt.Errorf("parse %s: %q is not a uuid", bootIDPath, raw)
	}
	boot := binary.BigEndian.Uint64(id[:8])
	if boot == 0 {
		return 0, fmt.Errorf("parse %s: %q folds to a zero boot stamp", bootIDPath, raw)
	}
	return boot, nil
}

// tablePIDs lists every process id the process table holds right now.
func tablePIDs() ([]int, error) {
	table, err := os.Open("/proc")
	if err != nil {
		return nil, fmt.Errorf("enumerate process table: %w", err)
	}
	names, readErr := table.Readdirnames(-1)
	closeErr := table.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("enumerate process table: %w", err)
	}
	pids := make([]int, 0, len(names))
	for _, name := range names {
		pid, err := strconv.Atoi(name)
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

func readStat(pid int) (taskStat, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if vanished(err) {
		return taskStat{}, errNoProc
	}
	if err != nil {
		return taskStat{}, fmt.Errorf("read stat of pid %d: %w", pid, err)
	}
	stat, err := parseStat(raw)
	if err != nil {
		return taskStat{}, fmt.Errorf("parse stat of pid %d: %w", pid, err)
	}
	return stat, nil
}

// vanished reports a /proc read that failed because the process left: the
// entry is gone at open, or the task was reaped between open and read.
func vanished(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, unix.ESRCH)
}

// parseStat reads one /proc/<pid>/stat line. The comm is whatever the process
// set, parentheses and spaces included, so the fixed fields are located from
// the last closing parenthesis and never by splitting the whole line.
func parseStat(raw []byte) (taskStat, error) {
	opening := bytes.IndexByte(raw, '(')
	closing := bytes.LastIndexByte(raw, ')')
	if opening < 0 || closing < opening {
		return taskStat{}, errors.New("no parenthesized comm")
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < statMinFields {
		return taskStat{}, fmt.Errorf("%d fields after the comm, want at least %d", len(fields), statMinFields)
	}
	if len(fields[statState]) != 1 {
		return taskStat{}, fmt.Errorf("state %q is not one character", fields[statState])
	}
	group, err := strconv.Atoi(fields[statGroup])
	if err != nil {
		return taskStat{}, fmt.Errorf("process group: %w", err)
	}
	session, err := strconv.Atoi(fields[statSession])
	if err != nil {
		return taskStat{}, fmt.Errorf("session: %w", err)
	}
	flags, err := strconv.ParseUint(fields[statFlags], 10, 64)
	if err != nil {
		return taskStat{}, fmt.Errorf("flags: %w", err)
	}
	threads, err := strconv.Atoi(fields[statThreads])
	if err != nil {
		return taskStat{}, fmt.Errorf("thread count: %w", err)
	}
	start, err := strconv.ParseUint(fields[statStart], 10, 64)
	if err != nil {
		return taskStat{}, fmt.Errorf("start time: %w", err)
	}
	return taskStat{
		comm:    string(raw[opening+1 : closing]),
		state:   fields[statState][0],
		group:   group,
		session: session,
		flags:   flags,
		threads: threads,
		start:   start,
	}, nil
}

// effectiveUID reads the effective uid /proc/<pid>/status reports. The owner
// of the /proc/<pid> inode is not it: the kernel hands a non-dumpable
// process's entry to root.
func effectiveUID(pid int) (int, error) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if vanished(err) {
		return 0, errNoProc
	}
	if err != nil {
		return 0, fmt.Errorf("read status of pid %d: %w", pid, err)
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		value, found := strings.CutPrefix(line, "Uid:")
		if !found {
			continue
		}
		ids := strings.Fields(value)
		if len(ids) != 4 {
			return 0, fmt.Errorf("parse status of pid %d: uid line %q", pid, line)
		}
		effective, err := strconv.Atoi(ids[1])
		if err != nil {
			return 0, fmt.Errorf("parse status of pid %d: effective uid: %w", pid, err)
		}
		return effective, nil
	}
	return 0, fmt.Errorf("parse status of pid %d: no uid line", pid)
}
