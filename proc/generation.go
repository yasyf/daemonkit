package proc

import (
	"crypto/rand"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

const ownerGenerationTextLength = 32

var (
	_ encoding.TextMarshaler   = OwnerGeneration{}
	_ encoding.TextUnmarshaler = (*OwnerGeneration)(nil)
)

// OwnerGeneration is one exact process-owner generation.
type OwnerGeneration [16]byte

// ParseOwnerGeneration decodes exactly 32 lowercase hexadecimal bytes.
func ParseOwnerGeneration(text string) (OwnerGeneration, error) {
	if len(text) != ownerGenerationTextLength {
		return OwnerGeneration{}, errors.New("proc: owner generation must be 32 lowercase hexadecimal bytes")
	}
	var generation OwnerGeneration
	if _, err := hex.Decode(generation[:], []byte(text)); err != nil || hex.EncodeToString(generation[:]) != text {
		return OwnerGeneration{}, errors.New("proc: owner generation must be 32 lowercase hexadecimal bytes")
	}
	if generation == (OwnerGeneration{}) {
		return OwnerGeneration{}, errors.New("proc: owner generation is zero")
	}
	return generation, nil
}

// String returns the exact lowercase hexadecimal wire representation.
func (g OwnerGeneration) String() string { return hex.EncodeToString(g[:]) }

// MarshalText encodes the exact nonzero v1 scalar representation.
func (g OwnerGeneration) MarshalText() ([]byte, error) {
	if g == (OwnerGeneration{}) {
		return nil, errors.New("proc: owner generation is zero")
	}
	return []byte(g.String()), nil
}

// UnmarshalText replaces g only after strict validation.
func (g *OwnerGeneration) UnmarshalText(text []byte) error {
	if g == nil {
		return errors.New("proc: owner generation target is nil")
	}
	parsed, err := ParseOwnerGeneration(string(text))
	if err != nil {
		return err
	}
	*g = parsed
	return nil
}

var processGeneration = sync.OnceValues(func() (OwnerGeneration, error) {
	var identity OwnerGeneration
	if _, err := rand.Read(identity[:]); err != nil {
		return OwnerGeneration{}, fmt.Errorf("proc: create process generation: %w", err)
	}
	if identity == (OwnerGeneration{}) {
		return OwnerGeneration{}, errors.New("proc: create process generation: random source returned zero")
	}
	return identity, nil
})

// ProcessGeneration returns one opaque generation stable for this process and
// different from every other process invocation.
func ProcessGeneration() (OwnerGeneration, error) {
	return processGeneration()
}
