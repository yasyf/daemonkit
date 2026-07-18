// Command trustfixture is the signed peer binary the trust E2E spawns: it dials
// the given unix socket and blocks so tests can inspect its live code identity.
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
