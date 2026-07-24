// Package stopcontrol owns the fixed v1 timing contract shared by the
// controller and hidden stop role.
package stopcontrol

import "time"

// The v1 timing contract keeps the complete post-commit authority window
// shorter than child settlement and gives the parent a strictly longer
// observation ceiling. Durable arming privately reserves one additional
// AuthorityBound and never releases a child unless this full window remains.
const (
	IdentityBound          time.Duration = 5 * time.Second
	TrackBound             time.Duration = 5 * time.Second
	AuthorityBound         time.Duration = 5 * time.Second
	ChildSettlementBound   time.Duration = 30 * time.Second
	ParentSettlementMargin time.Duration = 5 * time.Second
	ParentOperationBound                 = ChildSettlementBound + ParentSettlementMargin
	DeferredUntrackBound   time.Duration = 5 * time.Second
	TotalBound                           = IdentityBound + TrackBound + ParentOperationBound + DeferredUntrackBound
	PollInterval           time.Duration = 25 * time.Millisecond
)
