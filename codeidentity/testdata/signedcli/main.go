package main

import (
	"fmt"
	"strings"

	"github.com/yasyf/daemonkit/trust"
)

var markers = []string{
	"com.apple.security.application-groups",
	"com.apple.security.cs.disable-library-validation",
	"com.apple.security.cs.allow-dyld-environment-variables",
	"com.apple.security.cs.allow-unsigned-executable-memory",
	"com.apple.security.cs.allow-jit",
	"com.apple.security.cs.disable-executable-page-protection",
	"com.apple.security.get-task-allow",
	"group.com.example.daemonkit.signed-only-marker",
	"com.example.daemonkit.signed-only-marker",
	"daemonkit-signed-only-marker-value",
}

func main() {
	requirement := trust.Requirement{
		TeamID:            "ABCDE12345",
		SigningIdentifier: "com.example.daemonkit.signed",
		RequiredAppGroup:  markers[7],
		RequiredEntitlements: map[string]trust.EntitlementRequirement{
			markers[8]: {Match: trust.EntitlementString, String: markers[9]},
		},
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%x %s\n", digest, strings.Join(markers, "\x00"))
}
