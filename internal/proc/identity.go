// Package proc is the consumer-agnostic process runtime for one detached
// daemon: identity-exact suspended spawn, driver-owned settlement, a
// single-writer durable record store, and the reap ladder. Process identity
// is the unexported comparable identity struct, compared only by matches.
package proc

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// identity is one process instance: a PID is authority only beside the boot
// session and kernel start stamp that minted it.
type identity struct {
	pid   int
	start uint64
	boot  uint64
}

// matches is the module's only identity comparison.
func (i identity) matches(o identity) bool { return i == o }

func (i identity) crossBoot(boot uint64) bool { return i.boot != boot }

func (i identity) unsafe() bool { return i.pid <= 1 || i.pid == os.Getpid() }

func mintGeneration() (uint64, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, fmt.Errorf("proc: mint generation: %w", err)
	}
	generation := binary.BigEndian.Uint64(raw[:])
	if generation == 0 {
		return 0, errors.New("proc: mint generation: random source returned zero")
	}
	return generation, nil
}
