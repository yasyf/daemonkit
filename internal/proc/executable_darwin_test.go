package proc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// TestClassifyPidPath is the gate's fail-closed contract at its source. The
// kernel answers ENOENT for a live process whose binary an in-place upgrade
// unlinked, so reading that errno as "gone" reports an empty inventory for a
// daemon that is still running and clears the irreversible step it guards.
func TestClassifyPidPath(t *testing.T) {
	tests := []struct {
		name  string
		errno unix.Errno
		want  pidPathVerdict
	}{
		{name: "no such process", errno: unix.ESRCH, want: pidPathGone},
		{name: "live process whose binary was unlinked", errno: unix.ENOENT, want: pidPathUnnamed},
		{name: "invalid argument", errno: unix.EINVAL, want: pidPathUndetermined},
		{name: "operation not permitted", errno: unix.EPERM, want: pidPathUndetermined},
		{name: "buffer refused", errno: unix.ENOMEM, want: pidPathUndetermined},
		{name: "interrupted", errno: unix.EINTR, want: pidPathUndetermined},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyPidPath(test.errno); got != test.want {
				t.Fatalf("classifyPidPath(%v) = %d, want %d", test.errno, got, test.want)
			}
		})
	}
}

func TestParseExecPath(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "argc header then the exec path", raw: []byte("\x02\x00\x00\x00/usr/bin/true\x00-x\x00"), want: "/usr/bin/true"},
		{name: "padded with the alignment nuls", raw: []byte("\x01\x00\x00\x00/bin/sh\x00\x00\x00"), want: "/bin/sh"},
		{name: "header only", raw: []byte("\x00\x00\x00\x00"), want: ""},
		{name: "truncated header", raw: []byte("\x00\x00"), want: ""},
		{name: "unterminated", raw: []byte("\x01\x00\x00\x00/bin/sh"), want: ""},
		{name: "empty exec path", raw: []byte("\x01\x00\x00\x00\x00/bin/sh\x00"), want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := parseExecPath(test.raw); got != test.want {
				t.Fatalf("parseExecPath(%q) = %q, want %q", test.raw, got, test.want)
			}
		})
	}
}

// TestExecPathFromArgs runs the ENOENT fallback against the real kernel: the
// recorded execve path recovers an identity proc_pidpath could not name, a
// pid the kernel does not hold is still gone, and another user's process —
// which KERN_PROCARGS2 refuses with the same EINVAL as a dead pid — is neither
// an error that aborts the scan nor a survivor, because daemonkit's trust
// floor is the same effective uid.
func TestExecPathFromArgs(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(self)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		pid  func(*testing.T) int
		want string
		err  error
	}{
		{
			name: "this process",
			pid:  func(*testing.T) int { return os.Getpid() },
			want: resolved,
		},
		{
			name: "a pid the kernel cannot hold",
			pid:  func(*testing.T) int { return 1 << 30 },
			err:  ErrNoProcess,
		},
		{
			name: "another user's process",
			pid:  foreignPID,
			err:  errForeignProcess,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pid := test.pid(t)
			got, err := execPathFromArgs(pid)
			if !errors.Is(err, test.err) {
				t.Fatalf("execPathFromArgs(%d) error = %v, want %v", pid, err, test.err)
			}
			if got != test.want {
				t.Fatalf("execPathFromArgs(%d) = %q, want %q", pid, got, test.want)
			}
		})
	}
}

func foreignPID(t *testing.T) int {
	t.Helper()
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		t.Fatal(err)
	}
	for _, kp := range procs {
		if pid := int(kp.Proc.P_pid); pid > 1 && int(kp.Eproc.Ucred.Uid) != os.Geteuid() {
			return pid
		}
	}
	t.Fatal("no process owned by another user; a live macOS always runs some, and without one this read proves nothing")
	return 0
}
