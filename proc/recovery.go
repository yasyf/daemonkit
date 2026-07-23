package proc

import "fmt"

// RecoveryClass names the exact recovery barrier that must settle a retired
// process receipt before it can be acknowledged.
type RecoveryClass uint8

const (
	_ RecoveryClass = iota
	// RecoverySourceOwner gates source-owner catalog fence recovery.
	RecoverySourceOwner
	// RecoverySourceDriver gates source mutation-journal recovery.
	RecoverySourceDriver
	// RecoveryBroker gates broker state recovery.
	RecoveryBroker
	// RecoveryNativeMount gates native presentation replacement.
	RecoveryNativeMount
	// RecoveryCatalogWorker gates catalog-worker replacement.
	RecoveryCatalogWorker
	// RecoveryObserver gates source-observer journal recovery.
	RecoveryObserver
	// RecoveryTask gates disposable-task recovery.
	RecoveryTask
	// RecoveryService gates launch-service desired-state reconciliation.
	RecoveryService
	// RecoveryTrust gates trust-verifier process settlement.
	RecoveryTrust
	// RecoveryHolder gates the aggregate holder recovery barrier.
	RecoveryHolder
	// RecoveryStopControl authenticates one pre-dispatch cross-process stop caller.
	RecoveryStopControl
)

// Validate rejects an absent or unknown recovery class.
func (c RecoveryClass) Validate() error {
	switch c {
	case RecoverySourceOwner, RecoverySourceDriver, RecoveryBroker, RecoveryNativeMount,
		RecoveryCatalogWorker, RecoveryObserver, RecoveryTask, RecoveryService,
		RecoveryTrust, RecoveryHolder, RecoveryStopControl:
		return nil
	default:
		return fmt.Errorf("proc: invalid recovery class %d", c)
	}
}
