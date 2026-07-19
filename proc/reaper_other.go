//go:build !darwin

package proc

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func probeProc(pid int) (procInfo, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if errors.Is(err, os.ErrNotExist) {
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
	groupID, err := strconv.Atoi(fields[2])
	if err != nil {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat process group: %w", pid, err)
	}
	sessionID, err := strconv.Atoi(fields[3])
	if err != nil {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat session: %w", pid, err)
	}
	return procInfo{startTime: fields[19], comm: comm, groupID: groupID, sessionID: sessionID, zombie: fields[0] == "Z"}, nil
}

func probeGroupMembers(groupID, sessionID int) ([]groupMember, error) {
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
		if info.groupID == groupID && info.sessionID == sessionID {
			members = append(members, groupMember{pid: pid, info: info})
		}
	}
	return members, nil
}
