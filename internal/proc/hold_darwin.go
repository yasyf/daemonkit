package proc

import "syscall"

type pidHold struct{ pid int }

func (sysSignaler) hold(pid int) (held, error) { return pidHold{pid: pid}, nil }

func (h pidHold) signal(sig syscall.Signal) error { return syscall.Kill(h.pid, sig) }

func (pidHold) release() {}
