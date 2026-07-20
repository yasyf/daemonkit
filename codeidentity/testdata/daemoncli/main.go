package main

import (
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
	fmt.Println(stop, codeidentity.CodePolicy{Identity: identity}.Check(peer))
}
