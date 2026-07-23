// Package fenceauth carries the module-private child-dispatch capability.
package fenceauth

type token struct{}

// Authority is constructible only inside daemonkit's internal import boundary.
type Authority struct{ token *token }

// New constructs one valid module-private authority.
func New() Authority { return Authority{token: &token{}} }

// Valid reports whether the authority was constructed by New.
func (a Authority) Valid() bool { return a.token != nil }
