package proc

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

var processGeneration = sync.OnceValues(func() (string, error) {
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return "", fmt.Errorf("proc: create process generation: %w", err)
	}
	return hex.EncodeToString(identity[:]), nil
})

// ProcessGeneration returns one opaque generation stable for this process and
// different from every other process invocation.
func ProcessGeneration() (string, error) {
	return processGeneration()
}
