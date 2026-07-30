//go:build !darwin

package proc

// Uncapped off darwin: Linux RLIMIT_NPROC counts threads, so capping around a fork would starve the Go runtime.
func withChildNprocCap(spawn func() error) error { return spawn() }
