package daemonkit

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/trust"
)

// Serving is the code identity a process must prove before daemonkit will
// speak to it or let it run. Its two constructors are its two policies, a
// Serving carries exactly one of them, and no field is settable — so the
// unstated posture is not representable and every site states which one it
// took. The same value answers for both boundaries it guards: the process
// accepting on a daemon's socket, and the executable behind a Cmd.Path before
// its child runs an instruction.
type Serving struct{ policy servingPolicy }

// servingPolicy is one Serving's whole policy. Its two implementations are
// Serving's two constructors, and nothing outside this file can write a third.
type servingPolicy interface {
	// requirement is the code-signing requirement this posture pins, nil for
	// the posture that pins none.
	requirement() *Requirement
}

// ServingSigned pins the process to a designated code-signing requirement,
// verified against the kernel-held code identity the exec established. As
// Cmd.Exec it reads the suspended child in place — never a staged copy — and a
// failed verify aborts the spawn before release, so the child never executes
// an instruction.
func ServingSigned(r Requirement) Serving { return Serving{policy: signedServing{pinned: r}} }

// ServingSameUser is the named waiver: whatever a same-UID writer left behind
// the path is admitted, which is the only posture a Python interpreter, a
// platform binary, or a homebrew tool can take. It proves nothing beyond the
// same-EUID floor that runs unconditionally, and it exists so that residue is
// greppable in the consumer's source instead of implied by an absent field.
func ServingSameUser() Serving { return Serving{policy: sameUserServing{}} }

type signedServing struct{ pinned Requirement }

func (s signedServing) requirement() *Requirement { return &s.pinned }

type sameUserServing struct{}

func (sameUserServing) requirement() *Requirement { return nil }

func (s Serving) stated() bool { return s.policy != nil }

// validate is the config-boundary check on a stated posture: the posture is
// one of the two constructors, and a pinned requirement is one a peer could
// satisfy. It runs where a refusal names the field, so a policy admitting
// nobody fails there rather than at the first attach.
func (s Serving) validate(field string) error {
	if !s.stated() {
		return fmt.Errorf("daemonkit: %s is unstated (ServingSigned or ServingSameUser)", field)
	}
	req := s.policy.requirement()
	if req == nil {
		return nil
	}
	if err := wireRequirement(req).Validate(); err != nil {
		return fmt.Errorf("daemonkit: %s: %w", field, err)
	}
	return nil
}

// verifyProcess runs the posture against a live process, which may be a child
// suspended at its entry point. The same-user waiver has nothing to read: a
// posix_spawn child carries this process's own UID by construction, so the
// floor it names is already structural.
func (s Serving) verifyProcess(pid int) error {
	req := s.policy.requirement()
	if req == nil {
		return nil
	}
	if err := trust.VerifyProcess(pid, *wireRequirement(req)); err != nil {
		return fmt.Errorf("%w: pid %d: %w", ErrUntrusted, pid, err)
	}
	return nil
}
