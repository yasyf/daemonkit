package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
)

func main() {
	identity := codeidentity.CodeIdentity{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit.broker",
	}
	var digest codeidentity.PolicyDigest
	digest[0] = 1
	stop := service.AppStopSpec{
		ExecutableName: "DaemonKitBroker",
		CodeIdentity:   identity,
		PolicyDigest:   digest,
	}
	peer := wire.Peer{UID: os.Geteuid(), Audit: []byte(os.Getenv("DAEMONKIT_AUDIT_TOKEN"))}
	keepAlive := service.AppKeepAlive{
		Label:    identity.SigningIdentifier,
		AppPath:  "/Applications/DaemonKitBroker.app",
		BundleID: identity.SigningIdentifier,
	}
	expected := service.AuthenticatedAppPeer{
		PID:          peer.PID,
		UID:          peer.UID,
		Executable:   stop.ExecutableName,
		CodeIdentity: identity,
		PolicyDigest: digest,
	}
	fmt.Println(keepAlive.Stop(context.Background(), stop, expected), codeidentity.CodePolicy{Identity: identity}.Check(peer))
}
