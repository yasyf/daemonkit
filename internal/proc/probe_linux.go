package proc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func probeProc(pid int) (procInfo, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return procInfo{}, errNoProc
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("read /proc/%d/stat: %w", pid, err)
	}
	// comm is parenthesized and free to contain spaces or ')': split on the LAST ')'.
	s := string(data)
	open := strings.IndexByte(s, '(')
	shut := strings.LastIndexByte(s, ')')
	if open < 0 || shut < 0 || shut < open {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat comm: %q", pid, s)
	}
	comm := s[open+1 : shut]
	// starttime is stat field 22 → index 19 of the post-comm fields.
	fields := strings.Fields(s[shut+1:])
	if len(fields) < 20 {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat: %d fields after comm, want >=20", pid, len(fields))
	}
	group, err := strconv.Atoi(fields[2])
	if err != nil {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat process group: %w", pid, err)
	}
	session, err := strconv.Atoi(fields[3])
	if err != nil {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat session: %w", pid, err)
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat start time: %w", pid, err)
	}
	return procInfo{
		start:   start,
		comm:    comm,
		group:   group,
		session: session,
		zombie:  fields[0] == "Z",
		stopped: fields[0] == "T" || fields[0] == "t",
	}, nil
}

func probeGroupMembers(sessionID int) ([]groupMember, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("enumerate /proc: %w", err)
	}
	members := make([]groupMember, 0)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		info, err := probeProc(pid)
		if errors.Is(err, errNoProc) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.session == sessionID {
			members = append(members, groupMember{pid: pid, info: info})
		}
	}
	return members, nil
}

func bootSession() (uint64, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return 0, fmt.Errorf("read boot_id: %w", err)
	}
	return bootFromID(strings.TrimSpace(string(data)))
}

// parseLegacyStart maps v1's linux clock-tick start stamp onto the frozen
// uint64 encoding it already was.
func parseLegacyStart(stamp string) (uint64, error) {
	start, err := strconv.ParseUint(stamp, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("proc: malformed legacy start stamp %q: %w", stamp, err)
	}
	return start, nil
}

// parseLegacyBoot maps v1's boot_id UUID onto the frozen encoding: the UUID's
// first 16 hex digits parsed base-16 — 64 of its random bits.
func parseLegacyBoot(stamp string) (uint64, error) { return bootFromID(stamp) }

func bootFromID(id string) (uint64, error) {
	digits := strings.ReplaceAll(id, "-", "")
	if len(digits) < 16 {
		return 0, fmt.Errorf("proc: malformed boot id %q", id)
	}
	boot, err := strconv.ParseUint(digits[:16], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("proc: malformed boot id %q: %w", id, err)
	}
	return boot, nil
}
