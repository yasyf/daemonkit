// Command trustfixture is the peer binary the trust E2E signs and spawns:
// given a unix socket path it dials and blocks until the server closes the
// connection, so tests can inspect the live peer's code identity; with no
// arguments it sleeps until killed.
package main

import (
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) > 1 {
		conn, err := net.Dial("unix", os.Args[1])
		if err != nil {
			os.Exit(1)
		}
		defer conn.Close()
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		return
	}
	for {
		time.Sleep(time.Hour)
	}
}
