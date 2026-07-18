package proc

import "sync"

var bootOnce = sync.OnceValues(bootSession)

// BootID returns an identifier for the host's current boot session, stable for
// the life of the boot and different across boots. Identity records carry it so
// a start stamp recorded before a power loss can never collide with a
// same-PID process on the next boot (linux start stamps are ticks since boot).
func BootID() (string, error) {
	return bootOnce()
}
