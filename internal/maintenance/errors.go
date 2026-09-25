// Package maintenance shares preservation refusals across ownership and wire layers.
package maintenance

import "errors"

var (
	// ErrDrainBusy rejects an irreversible drain while protected work remains.
	ErrDrainBusy = errors.New("daemonkit: preservation drain is busy")
	// ErrDrainPreparationTimeout rejects a drain whose reversible wait expired.
	ErrDrainPreparationTimeout = errors.New("daemonkit: preservation drain preparation timed out")
)
