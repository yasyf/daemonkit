package wire

import "context"

// ProtectedSessionClassifier authenticates protected process candidates and
// authorizes the client/server build relationship before lifecycle dispatch.
type ProtectedSessionClassifier interface {
	Validate() error
	Classify(context.Context, Peer) (bool, error)
	AuthorizeBuild(serverBuild, peerBuild string) bool
}
