package paths

import "fmt"

const sunPathBytes = 104

// Socket returns the daemon socket path for name's state directory
// (~/.daemonkit/agents/<name>/daemon.sock). A path that cannot fit darwin's
// sun_path with its terminating NUL returns a *SocketPathError instead of
// surviving to a truncated bind.
func Socket(name string) (string, error) {
	path := Agent(name).SocketPath()
	if len(path) >= sunPathBytes {
		return "", &SocketPathError{Path: path}
	}
	return path, nil
}

// SocketPathError reports a socket path too long for darwin's 104-byte
// sun_path.
type SocketPathError struct {
	Path string
}

func (e *SocketPathError) Error() string {
	return fmt.Sprintf("socket path is %d bytes; sun_path fits %d: %q", len(e.Path), sunPathBytes-1, e.Path)
}
