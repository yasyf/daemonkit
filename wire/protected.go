package wire

import "context"

// ProtectedSessionClassifier authenticates protected process candidates and
// authorizes the client/server release relationship before lifecycle mutation.
type ProtectedSessionClassifier interface {
	Validate() error
	Classify(context.Context, Peer) (bool, error)
	AuthorizeLifecycleBuild(serverBuild, peerBuild string) bool
}
