//go:build !darwin

package proc

import (
	"errors"
	"fmt"
	"os"
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
	return procInfo{startTime: fields[19], comm: comm}, nil
}
