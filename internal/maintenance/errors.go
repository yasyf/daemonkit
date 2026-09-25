package maintenance

import "errors"

var (
	ErrDrainBusy               = errors.New("daemonkit: preservation drain is busy")
	ErrDrainPreparationTimeout = errors.New("daemonkit: preservation drain preparation timed out")
)
