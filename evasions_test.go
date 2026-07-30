//go:build evasions

package daemonkit_test

import (
	"time"

	"github.com/yasyf/daemonkit"
)

// The Device-1 budget evasions, kept as a compile-time proof: Budget's fields
// are unexported, so no package can restate a duration as a deadline. Prove it
// with
//
//	go vet -tags evasions .
//
// which must FAIL on every line below.
func budgetEvasions() {
	_ = daemonkit.Budget{deadline: time.Now().Add(30 * time.Second)} // unexported field
	var b daemonkit.Budget
	b.deadline = b.deadline.Add(5 * time.Second) // unexported field
}
