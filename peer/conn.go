package peer

import (
	"fmt"
	"net"
)

// FromConn captures the exact kernel identity of a connected Unix peer.
func FromConn(conn *net.UnixConn) (Identity, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Identity{}, fmt.Errorf("peer: syscall conn: %w", err)
	}
	var identity Identity
	var opErr error
	if err := raw.Control(func(fd uintptr) { identity, opErr = fromFD(int(fd)) }); err != nil {
		return Identity{}, fmt.Errorf("peer: control fd: %w", err)
	}
	if opErr != nil {
		return Identity{}, opErr
	}
	process, err := bindProcess(identity)
	if err != nil {
		return Identity{}, fmt.Errorf("peer: bind pid %d to process identity: %w", identity.PID, err)
	}
	identity.StartTime = process.StartTime
	identity.Comm = process.Comm
	identity.Boot = process.Boot
	identity.Executable = process.Executable
	return identity, nil
}
