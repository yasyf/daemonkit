// Package runtimeauth connects daemonkit's private runtime constructor to its
// public wire composer without exposing lifecycle construction outside the
// module.
package runtimeauth

import (
	"errors"
	"sync"
)

type builder func(config any) (any, error)

var (
	mu      sync.Mutex
	compose builder
)

// Register installs the daemon package's private constructor once.
func Register(candidate func(config any) (any, error)) {
	mu.Lock()
	defer mu.Unlock()
	if candidate == nil || compose != nil {
		panic("runtimeauth: invalid constructor registration")
	}
	compose = candidate
}

// Build invokes the module-private constructor installed by daemon.
func Build(config any) (any, error) {
	mu.Lock()
	candidate := compose
	mu.Unlock()
	if candidate == nil {
		return nil, errors.New("runtimeauth: constructor is not registered")
	}
	return candidate(config)
}
