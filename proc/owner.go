package proc

import (
	"errors"
	"fmt"
	"os"
)

// VerifyCurrentOwner proves record names this exact live dedicated-session
// leader and carries the expected recovery policy.
func VerifyCurrentOwner(record Record, expected RecoveryClass) error {
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("proc: verify current owner recovery class: %w", err)
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.RecoveryClass != expected {
		return fmt.Errorf("%w: recovery class is %v, want %v", ErrIdentityChanged, record.RecoveryClass, expected)
	}
	if !record.ProcessGroup || record.SessionID != record.PID {
		return fmt.Errorf("%w: owner is not a dedicated-session leader", ErrIdentityChanged)
	}
	if record.PID != os.Getpid() {
		return fmt.Errorf("%w: owner pid is %d, current pid is %d", ErrIdentityChanged, record.PID, os.Getpid())
	}
	owned, err := (&Reaper{}).Owns(record)
	if err != nil {
		return fmt.Errorf("proc: verify current owner: %w", err)
	}
	if !owned {
		return errors.Join(ErrIdentityChanged, errors.New("current process identity does not match owner record"))
	}
	return nil
}
