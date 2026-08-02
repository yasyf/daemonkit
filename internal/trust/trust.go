// Package trust decides whether a connected unix-socket peer is the signed
// application a daemon expects, using only kernel-held code-signing state: a
// same-EUID floor, plus six csops_audittoken reads against the peer's audit
// token on signed builds. It opens no file, contacts
// no daemon, builds no CoreFoundation object, and cannot block. A configured
// Requirement with no verifier fails closed, never downgrading to UID-only; a
// nil Requirement is explicit UID-only trust. The daemonkit_unsigned build tag
// drops the verifier for local-test builds, which release CI must reject.
package trust

import (
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/yasyf/daemonkit/internal/proc"
)

// ErrUntrustedPeer is returned when a peer fails the trust check.
var ErrUntrustedPeer = errors.New("trust: untrusted peer")

// ErrNoVerifier is the fail-closed denial when a Requirement is configured but
// the verifier is absent or the kernel answered a shape it cannot interpret —
// never a downgrade to UID-only.
var ErrNoVerifier = errors.New("trust: no code-identity verifier for a configured requirement")

// ErrPeerGone is returned when the peer's execution generation ended before
// verification completed: a per-connection race, not a missing verifier.
var ErrPeerGone = errors.New("trust: peer exited before code-identity verification completed")

// Peer is the kernel-authenticated identity of a connected unix-socket peer.
// Token is the zero value where the platform has no audit token.
type Peer struct {
	UID   int
	Token proc.AuditToken
}

// PeerCredentials reads conn's kernel-authenticated peer identity. Call it
// once per connection, immediately after accept.
func PeerCredentials(conn *net.UnixConn) (Peer, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return Peer{}, fmt.Errorf("trust: syscall conn: %w", err)
	}
	var (
		peer  Peer
		opErr error
	)
	if err := raw.Control(func(fd uintptr) { peer, opErr = peerFromFD(int(fd)) }); err != nil {
		return Peer{}, fmt.Errorf("trust: control fd: %w", err)
	}
	if opErr != nil {
		return Peer{}, opErr
	}
	return peer, nil
}

// Floor enforces the unconditional same-effective-UID requirement that runs
// for every peer, before any Requirement is consulted.
func Floor(uid int) error {
	if uid != os.Geteuid() {
		return fmt.Errorf("%w: uid %d != %d", ErrUntrustedPeer, uid, os.Geteuid())
	}
	return nil
}

// Verify enforces Floor, then req against the peer's audit token. A nil req is
// explicit UID-only trust. Every failure denies: ErrUntrustedPeer for a policy
// mismatch, ErrPeerGone for an execution generation that ended, ErrNoVerifier
// for an absent verifier or a kernel answer this build cannot interpret. An
// invalid req is a configuration error and matches none of the three.
//
// The verdict is a query-time property of one pidversion generation. A peer
// that execs after connecting is re-judged only at the next admission; a peer
// that passes its descriptor to another process is never re-judged.
func Verify(peer Peer, req *Requirement) error {
	if err := Floor(peer.UID); err != nil {
		return err
	}
	if req == nil {
		return nil
	}
	return judge(peer.Token, *req)
}

// VerifyAny enforces Floor, then admits the peer if any req matches. An empty
// set is explicit UID-only trust, exactly as a nil req is for Verify. Every
// disjunct is judged against the same audit token, so a denial that is not a
// policy mismatch — ErrPeerGone, ErrNoVerifier, an invalid requirement — denies
// the whole set: no later disjunct could be judged more soundly than the one
// that already failed to read the kernel. When every disjunct is a policy
// mismatch the denial joins them all and is ErrUntrustedPeer.
func VerifyAny(peer Peer, reqs []Requirement) error {
	if err := Floor(peer.UID); err != nil {
		return err
	}
	if len(reqs) == 0 {
		return nil
	}
	return anyOf(reqs, func(req Requirement) error { return judge(peer.Token, req) })
}

// anyOf folds the per-requirement verdicts into the set's one verdict. It is
// the disjunction itself, separated from the kernel seam it is folded over.
func anyOf(reqs []Requirement, judged func(Requirement) error) error {
	denials := make([]error, 0, len(reqs))
	for _, req := range reqs {
		err := judged(req)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrUntrustedPeer) {
			return err
		}
		denials = append(denials, err)
	}
	return errors.Join(denials...)
}

func judge(token proc.AuditToken, req Requirement) error {
	if err := req.Validate(); err != nil {
		return err
	}
	return verifyRequirement(token, req)
}
