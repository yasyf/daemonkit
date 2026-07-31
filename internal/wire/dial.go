package wire

import (
	"context"
	"net"
)

// UnixDialer returns a context-aware unix socket dialer for ClientConfig.
func UnixDialer(path string) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", path)
	}
}
