package wire

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
)

func provesNoListener(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}

// UnixDialer returns a context-aware unix socket dialer for ClientConfig.
func UnixDialer(path string) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", path)
	}
}
