package wire

import "context"

// ProtectedSessionClassifier authenticates protected process candidates for
// reserved session capacity.
type ProtectedSessionClassifier interface {
	Validate() error
	Classify(context.Context, Peer) (bool, error)
}
