//go:build evasions

package proc

// The adversarially-proven settlement evasions of the v1 design, kept as a
// compile-time proof: with reap authority a closure local of the driver
// goroutine, none of these constructions typechecks. Prove it with
//
//	go vet -tags evasions ./internal/proc/
//
// which must FAIL on every line below.
func settlementEvasions(c *Child) {
	_ = c.record                  // Child has no record field
	_ = c.store                   // Child has no store field
	_ = c.manager                 // Child has no manager field
	c.store.retire(c.record.id()) // settlement is unreachable from outside the driver
}
