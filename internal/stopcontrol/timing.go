// Package stopcontrol owns the fixed v1 timing contract shared by the
// controller and hidden stop role.
package stopcontrol

import "time"

// The v1 timing contract keeps authority consumption shorter than child
// settlement and gives the parent a strictly longer observation ceiling.
const (
	IdentityBound        time.Duration = 5 * time.Second
	AuthorityBound       time.Duration = 5 * time.Second
	ChildSettlementBound time.Duration = 30 * time.Second
	ParentOperationBound time.Duration = 35 * time.Second
	PollInterval         time.Duration = 25 * time.Millisecond
)
