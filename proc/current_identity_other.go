//go:build !darwin

package proc

// CurrentIdentity fails closed where no kernel audit-token self identity is
// available.
func CurrentIdentity() (Identity, error) {
	return Identity{}, ErrNoAuditToken
}
