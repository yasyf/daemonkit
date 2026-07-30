// Command daemoncli is the daemon-facing half of the binary-absence fixture:
// it configures only the code-signing halves of a Requirement, so no
// consumer-supplied entitlement value can reach a daemon-facing binary.
package main

import (
	"fmt"

	"github.com/yasyf/daemonkit/internal/trust"
)

func main() {
	requirement := trust.Requirement{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit.broker",
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		panic(err)
	}
	designated, err := requirement.DRString()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%x %s\n", digest, designated)
}
