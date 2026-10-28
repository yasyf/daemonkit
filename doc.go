// Package daemonkit provides the fleet's daemon lifecycle primitives:
// detached spawn, exclusive listener ownership, launcher-owned settlement, and
// bounded worker pools. Every process that can open a daemon's socket is
// trusted: no peer is judged.
package daemonkit
