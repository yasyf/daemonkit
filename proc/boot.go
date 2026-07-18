package proc

import "sync"

var bootOnce = sync.OnceValues(bootSession)

// BootID returns an identifier for the host's current boot session, stable for
// the boot's life and different across boots.
func BootID() (string, error) {
	return bootOnce()
}
