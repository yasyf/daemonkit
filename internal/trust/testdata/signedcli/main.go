// Command signedcli is the signed-side control for the binary-absence
// fixture: it names every consumer-supplied policy value the daemon-facing
// binary must not carry.
package main

import (
	"fmt"
	"strings"

	"github.com/yasyf/daemonkit/internal/trust"
)

var markers = []string{
	"group.com.example.daemonkit.signed-only-marker",
	"com.example.daemonkit.signed-only-marker",
	"daemonkit-signed-only-marker-value",
}

func main() {
	requirement := trust.Requirement{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit.signed",
		RequiredAppGroup:  markers[0],
		RequiredEntitlements: map[string]trust.EntitlementRequirement{
			markers[1]: {Match: trust.EntitlementString, String: markers[2]},
		},
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%x %s\n", digest, strings.Join(markers, "\x00"))
}
