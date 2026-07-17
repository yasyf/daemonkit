//go:build !darwin

package proc

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// probeProc reads pid's start time and comm from /proc/<pid>/stat. A missing
// entry means the process is gone (errNoProc); any other read or parse failure
// is a genuine probe error the reaper treats as Undetermined.
func probeProc(pid int) (procInfo, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if errors.Is(err, os.ErrNotExist) {
		return procInfo{}, errNoProc
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("read /proc/%d/stat: %w", pid, err)
	}
	// comm is field 2, parenthesized and free to contain spaces or ')', so split
	// on the LAST ')': everything before its '(' is comm, the rest are fields 3+.
	s := string(data)
	open := strings.IndexByte(s, '(')
	shut := strings.LastIndexByte(s, ')')
	if open < 0 || shut < 0 || shut < open {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat comm: %q", pid, s)
	}
	comm := s[open+1 : shut]
	// Fields after comm begin at field 3 (state); starttime is field 22, i.e.
	// index 22-3 == 19 of the post-comm fields.
	fields := strings.Fields(s[shut+1:])
	if len(fields) < 20 {
		return procInfo{}, fmt.Errorf("parse /proc/%d/stat: %d fields after comm, want >=20", pid, len(fields))
	}
	return procInfo{startTime: fields[19], comm: comm}, nil
}
