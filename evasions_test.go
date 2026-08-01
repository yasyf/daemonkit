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

// The supervision evasions: Child's identity model is sealed to PID, Serving
// has no third policy and no settable field, and the zero Ctx cannot be given
// a scope from outside.
func supervisionEvasions(c *daemonkit.Child, x daemonkit.Ctx) {
	_ = c.Start                                                // Child exposes no start stamp
	_ = c.Boot                                                 // Child exposes no boot session
	_ = daemonkit.Serving{policy: nil}                         // unexported field
	_ = daemonkit.Cmd{Exec: daemonkit.Serving{}}.Exec.stated() // unexported method
	x.owner = nil                                              // unexported field
}
