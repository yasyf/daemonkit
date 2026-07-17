package wire

import (
	"errors"
	"fmt"
	"time"
)

// ErrLadderInverted means an op's server deadline is not strictly shorter than
// its client deadline, so a live server response could race a client timeout.
var ErrLadderInverted = errors.New("wire: ladder op server deadline >= client deadline")

// ErrLadderMissingPair means an op appears on only one side of the ladder.
var ErrLadderMissingPair = errors.New("wire: ladder op missing its server/client pair")

// Op names a control-plane operation. Consumers define their own op values.
type Op string

// Ladder holds each op's server and client deadlines. The invariant — the
// client always outlives the server's work — is enforced at construction, so a
// constructed Ladder is always well-formed.
type Ladder struct {
	server map[Op]time.Duration
	client map[Op]time.Duration
}

// NewLadder pairs per-op server and client deadlines. Both maps must carry the
// same ops, and each op's client deadline must strictly exceed its server
// deadline; otherwise a spurious client timeout could mask a server result.
func NewLadder(server, client map[Op]time.Duration) (Ladder, error) {
	for op, s := range server {
		c, ok := client[op]
		if !ok {
			return Ladder{}, fmt.Errorf("%w: %q server-only", ErrLadderMissingPair, op)
		}
		if s >= c {
			return Ladder{}, fmt.Errorf("%w: %q server=%s client=%s", ErrLadderInverted, op, s, c)
		}
	}
	for op := range client {
		if _, ok := server[op]; !ok {
			return Ladder{}, fmt.Errorf("%w: %q client-only", ErrLadderMissingPair, op)
		}
	}
	l := Ladder{
		server: make(map[Op]time.Duration, len(server)),
		client: make(map[Op]time.Duration, len(client)),
	}
	for op, s := range server {
		l.server[op] = s
		l.client[op] = client[op]
	}
	return l, nil
}

// Deadlines returns op's server and client deadlines; ok is false when the
// ladder has no entry for op.
func (l Ladder) Deadlines(op Op) (server, client time.Duration, ok bool) {
	s, ok := l.server[op]
	if !ok {
		return 0, 0, false
	}
	return s, l.client[op], true
}
