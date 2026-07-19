//go:build !darwin

package proc

import "syscall"

// ExecutablePathAuditToken is unavailable off Darwin.
func ExecutablePathAuditToken(AuditToken) (string, error) { return "", ErrNoAuditToken }

func signalAuditToken(AuditToken, syscall.Signal) (bool, error) { return false, ErrNoAuditToken }
